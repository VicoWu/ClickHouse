#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Processors/Executors/ExecutionThreadContext.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

void ExecutionThreadContext::wait(std::atomic_bool & finished)
{
    std::unique_lock lock(mutex);

    condvar.wait(lock, [&]
    {
        return finished || wake_flag;
    });

    wake_flag = false;
}

void ExecutionThreadContext::wakeUp()
{
    std::lock_guard guard(mutex);
    wake_flag = true;
    condvar.notify_one();
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXCEEDED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT;
}

/**
 * 调用者是   ExecutionThreadContext::executeTask()
 * 这是一个静态方法，不属于任何一个类
 * @param node
 * @param read_progress_callback
 */
static void executeJob(ExecutingGraph::Node * node, ReadProgressCallback * read_progress_callback)
{
    try
    {
        // 当前的Processor是否支持spill
        if (node->processor->isSpillable() && CurrentThread::getGroup())
            CurrentThread::getGroup()->memory_spill_scheduler.checkAndSpill(node->processor);

        node->processor->work(); // 这里的IProcessor::work，比如，对于Kafka，就是调用 KafkaSource::work,实际上是 ISource::work

        /// Update read progress only for source nodes.
        bool is_source = node->back_edges.empty();

        if (is_source && read_progress_callback)
        {
            if (auto read_progress = node->processor->getReadProgress())
            {
                if (read_progress->counters.total_rows_approx)
                    read_progress_callback->addTotalRowsApprox(read_progress->counters.total_rows_approx);

                if (read_progress->counters.total_bytes)
                    read_progress_callback->addTotalBytes(read_progress->counters.total_bytes);

                if (!read_progress_callback->onProgress(read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits))
                    node->processor->cancel();
            }
        }
    }
    catch (Exception exception) /// NOLINT
    {
        /// Copy exception before modifying it because multiple threads can rethrow the same exception
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + node->processor->getName());
        throw exception;
    }
}

/**
 * 调用者是 PipelineExecutor::executeStepImpl
 * @return
 */
bool ExecutionThreadContext::executeTask()
{
    std::unique_ptr<OpenTelemetry::SpanHolder> span;

    if (trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(node->processor->getUniqID());
        span->addAttribute("thread_number", thread_number);
    }
    std::optional<Stopwatch> execution_time_watch;

#ifndef NDEBUG
    execution_time_watch.emplace();
#else
    if (profile_processors)
        execution_time_watch.emplace();
#endif

    try
    {
        /**
         *
         * 这是实质上的调用。它会调用 node->processor->prepare() 并根据返回的状态调用：
         *  work()：处理数据
         *  generate()：对 source 来说（如 KafkaSource）
         *  consume()：对 sink 来说（如 materialized view 的 insert sink）
         */

        executeJob(node, read_progress_callback);
        ++node->num_executed_jobs;
    }
    catch (...)
    {
        node->exception = std::current_exception();
    }

    if (profile_processors)
    {
        UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
        node->processor->elapsed_ns += elapsed_ns;
        if (trace_processors)
            span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
    }
#ifndef NDEBUG
    execution_time_ns += execution_time_watch->elapsed();
    if (trace_processors)
        span->addAttribute("execution_time_ns", execution_time_watch->elapsed());
#endif
    return node->exception == nullptr;
}

void ExecutionThreadContext::rethrowExceptionIfHas()
{
    if (exception)
        std::rethrow_exception(exception);
}

ExecutingGraph::Node * ExecutionThreadContext::tryPopAsyncTask()
{
    ExecutingGraph::Node * task = nullptr;

    if (!async_tasks.empty())
    {
        task = async_tasks.front();
        async_tasks.pop();

        if (async_tasks.empty())
            has_async_tasks = false;
    }

    return task;
}

void ExecutionThreadContext::pushAsyncTask(ExecutingGraph::Node * async_task)
{
    async_tasks.push(async_task);
    has_async_tasks = true;
}

}
