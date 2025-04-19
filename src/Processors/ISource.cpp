#include <Processors/ISource.h>
#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::~ISource() = default;

ISource::ISource(Block header, bool enable_auto_progress)
    : IProcessor({}, {std::move(header)})
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

ISource::Status ISource::prepare()
{
    // 已经标记为结束了，主动通知 output port，它也不能再用了，然后返回 Finished，此节点将不会再调度。
    if (finished)
    {
        output.finish();
        return Status::Finished;
    }
    /**
     * 更具体地说：
        ClickHouse 的数据流图是 push-based（源头推数据）
        如果下游某个节点（比如说 limit 已满足、query 被取消了）不再需要数据了，就会调用 input.close()，它的 upstream OutputPort 会被标记为 finished
        那么上游的 processor 调 output.isFinished() 就能知道：“你别再生成数据了，下游不会接收了”
    */
    /// Check can output.
    if (output.isFinished())
        return Status::Finished;
    // 如果下游 buffer 已满（output port 满了），不能再推数据，返回 PortFull，调度器会等下游消费后再调这个节点。
    if (!output.canPush())
        return Status::PortFull;
    // 如果我们准备好一个新的 Chunk 了（有数据），告诉调度器：“我已经准备好了！调我执行 work() 来生成数据”。
    if (!has_input)
        return Status::Ready;
    // 真正把数据推入 output port，然后标记我们当前数据已经用完，下次要重新生成。
    output.pushData(std::move(current_chunk)); // 将ISource::work()生成的current_chunk推送到output
    has_input = false; // 数据已经用完，下次重新生成

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull; // 正常流程里，数据已经推入，下游暂时还没消费，返回 PortFull，进入 idle 状态，等下游处理完再调度我。
}

void ISource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    std::lock_guard lock(read_progress_mutex);
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void ISource::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void ISource::addTotalBytes(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_bytes += value;
}

/**
 * 调用者是 static void executeJob
 */
void ISource::work()
{
    try
    {
        read_progress_was_set = false;
        // 这里调用的是 ISource::tryGenerate() 方法，进一步调用generate方法
        if (auto chunk = tryGenerate())
        {
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        got_exception = true;
        throw;
    }
}

// 这里实际上使用的是具体实现类的tryGenerate()方法，比如 KafkaSource::tryGenerate() 方法
Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

/**
 * 在ISource::work()中被调用
 */
std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate(); // generate方法来自于实现类的具体实现，比如KafkaSource::generate()
    if (!chunk)
        return std::nullopt;

    return chunk;
}

}

