#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/PullingOutputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * 初始化 PullingPipelineExecutor 对象，并设置管道的输出格式为 PullingOutputFormat。如果管道不支持拉取模式，则抛出异常。
 * @param pipeline_
 */
PullingPipelineExecutor::PullingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    // 构造的时候，这个Pipeline不应该已经处于pulling的状态
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for PullingPipelineExecutor must be pulling");

    pulling_format = std::make_shared<PullingOutputFormat>(pipeline.output->getHeader(), has_data_flag);
    pipeline.complete(pulling_format);
}

PullingPipelineExecutor::~PullingPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingPipelineExecutor");
    }
}

const Block & PullingPipelineExecutor::getHeader() const
{
    return pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader();
}

/**
 * 从管道中拉取数据块放入到chunk中。如果执行成功，则返回 true，否则返回 false。
 * @param chunk
 * @return
 */
bool PullingPipelineExecutor::pull(Chunk & chunk)
{
    if (!executor) // 如果还没有executor，则通过传入的QueryPipeline，构造对应的 PipelineExecutor
    {
        executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
        executor->setReadProgressCallback(pipeline.getReadProgressCallback());
    }

    if (!executor->checkTimeLimitSoft())
        return false;
    // PipelineExecutor::executeStep
    // 这是单线程执行的模式
    if (!executor->executeStep(&has_data_flag))
        return false;

    chunk = pulling_format->getChunk(); // 获取对应的chunk作为参数返回
    return true;
}

/**
 * 从管道中拉取一个数据块，并将其转换为 Block 类型。
 * 在 MergeTask::ExecuteAndFinalizeHorizontalPart::executeImpl()
 * 和 MergeTask::VerticalMergeStage::executeVerticalMergeForOneColumn()
 * 中调用
 * 以及在
 * @param block
 * @return
 */
bool PullingPipelineExecutor::pull(Block & block)
{
    Chunk chunk;

    if (!pull(chunk))
        return false;

    if (!chunk)
    {
        /// In case if timeout exceeded.
        block.clear();
        return true;
    }

    block = pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
    if (auto chunk_info = chunk.getChunkInfo())
    {
        if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
        }
    }

    return true;
}

void PullingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor)
        executor->cancel();
}

Chunk PullingPipelineExecutor::getTotals()
{
    return pulling_format->getTotals();
}

Chunk PullingPipelineExecutor::getExtremes()
{
    return pulling_format->getExtremes();
}

Block PullingPipelineExecutor::getTotalsBlock()
{
    auto totals = getTotals();

    if (totals.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Totals).getHeader();
    return header.cloneWithColumns(totals.detachColumns());
}

Block PullingPipelineExecutor::getExtremesBlock()
{
    auto extremes = getExtremes();

    if (extremes.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Extremes).getHeader();
    return header.cloneWithColumns(extremes.detachColumns());
}

ProfileInfo & PullingPipelineExecutor::getProfileInfo()
{
    return pulling_format->getProfileInfo();
}

}
