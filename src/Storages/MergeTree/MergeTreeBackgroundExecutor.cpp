#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>

#include <algorithm>

#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
    extern const Metric MergeTreeBackgroundExecutorThreads;
    extern const Metric MergeTreeBackgroundExecutorThreadsActive;
    extern const Metric MergeTreeBackgroundExecutorThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int INVALID_CONFIG_PARAMETER;
}


template <class Queue>
MergeTreeBackgroundExecutor<Queue>::MergeTreeBackgroundExecutor(
    String name_,
    size_t threads_count_, /// 32
    size_t max_tasks_count_, /// 64
    CurrentMetrics::Metric metric_,
    CurrentMetrics::Metric max_tasks_metric_,
    std::string_view policy)
    : name(name_)
    , threads_count(threads_count_)  //  background_pool_size
    , max_tasks_count(max_tasks_count_) // // background_pool_size * background_merges_mutations_concurrency_ratio
    , metric(metric_) //  CurrentMetrics::BackgroundMergesAndMutationsPoolTask
    , max_tasks_metric(max_tasks_metric_, 2 * max_tasks_count) // BackgroundMergesAndMutationsPoolSize, active + pending  max_tasks_metric负责将 max_tasks_metric_的值写入成为2 * max_tasks_count
    , pool(std::make_unique<ThreadPool>(
          CurrentMetrics::MergeTreeBackgroundExecutorThreads, CurrentMetrics::MergeTreeBackgroundExecutorThreadsActive, CurrentMetrics::MergeTreeBackgroundExecutorThreadsScheduled))
{
    if (max_tasks_count == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Task count for MergeTreeBackgroundExecutor must not be zero");

    pending.setCapacity(max_tasks_count);
    active.set_capacity(max_tasks_count);

    pool->setMaxThreads(std::max(1UL, threads_count));
    pool->setMaxFreeThreads(std::max(1UL, threads_count));
    pool->setQueueSize(std::max(1UL, threads_count));

    for (size_t number = 0; number < threads_count; ++number)
        pool->scheduleOrThrowOnError([this] { threadFunction(); });

    if (!policy.empty())
        pending.updatePolicy(policy);
}

template <class Queue>
MergeTreeBackgroundExecutor<Queue>::~MergeTreeBackgroundExecutor()
{
    wait();
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::wait()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
        has_tasks.notify_all();
    }

    pool->wait();
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::increaseThreadsAndMaxTasksCount(size_t new_threads_count, size_t new_max_tasks_count)
{
    std::lock_guard lock(mutex);

    /// Do not throw any exceptions from global pool. Just log a warning and silently return.
    if (new_threads_count < threads_count)
    {
        LOG_WARNING(log, "Loaded new threads count for {}Executor from top level config, but new value ({}) is not greater than current {}", name, new_threads_count, threads_count);
        return;
    }

    if (new_max_tasks_count < max_tasks_count.load(std::memory_order_relaxed))
    {
        LOG_WARNING(log, "Loaded new max tasks count for {}Executor from top level config, but new value ({}) is not greater than current {}", name, new_max_tasks_count, max_tasks_count);
        return;
    }

    LOG_INFO(log, "Loaded new threads count ({}) and max tasks count ({}) for {}Executor", new_threads_count, new_max_tasks_count, name);

    pending.setCapacity(new_max_tasks_count);
    active.set_capacity(new_max_tasks_count);

    pool->setMaxThreads(std::max(1UL, new_threads_count));
    pool->setMaxFreeThreads(std::max(1UL, new_threads_count));
    pool->setQueueSize(std::max(1UL, new_threads_count));

    for (size_t number = threads_count; number < new_threads_count; ++number)
        pool->scheduleOrThrowOnError([this] { threadFunction(); });

    max_tasks_metric.changeTo(2 * new_max_tasks_count); // pending + active
    max_tasks_count.store(new_max_tasks_count, std::memory_order_relaxed);
    threads_count = new_threads_count;
}

template <class Queue>
size_t MergeTreeBackgroundExecutor<Queue>::getMaxThreads() const
{
    std::lock_guard lock(mutex);
    return threads_count;
}

template <class Queue>
size_t MergeTreeBackgroundExecutor<Queue>::getMaxTasksCount() const
{
    return max_tasks_count.load(std::memory_order_relaxed);
}

template <class Queue>
bool MergeTreeBackgroundExecutor<Queue>::trySchedule(ExecutableTaskPtr task)
{
    std::lock_guard lock(mutex);

    if (shutdown)
        return false;

    auto & value = CurrentMetrics::values[metric];
    if (value.load() >= static_cast<int64_t>(max_tasks_count))
        return false;

    pending.push(std::make_shared<TaskRuntimeData>(std::move(task), metric));

    has_tasks.notify_one();
    return true;
}

void printExceptionWithRespectToAbort(LoggerPtr log, const String & query_id)
{
    std::exception_ptr ex = std::current_exception();

    if (ex == nullptr)
        return;

    try
    {
        std::rethrow_exception(ex);
    }
    catch (const TestException &) // NOLINT
    {
        /// Exception from a unit test, ignore it.
    }
    catch (const Exception & e)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// Cancelled merging parts is not an error - log normally.
            if (e.code() == ErrorCodes::ABORTED)
                LOG_DEBUG(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            else
                tryLogCurrentException(log, "Exception while executing background task {" + query_id + "}");
        });
    }
    catch (...)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            tryLogCurrentException(log, "Exception while executing background task {" + query_id + "}");
        });
    }
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::removeTasksCorrespondingToStorage(StorageID id)
{
    std::vector<TaskRuntimeDataPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Erase storage related tasks from pending and select active tasks to wait for
        try
        {
            /// An exception context is needed to proper delete write buffers without finalization
            /// See WriteBuffer::~WriteBuffer for more context
            throw std::runtime_error("Storage is about to be deleted. Done pending task as if it was aborted.");
        }
        catch (...)
        {
            pending.remove(id);
        }

        /// Copy items to wait for their completion
        std::copy_if(active.begin(), active.end(), std::back_inserter(tasks_to_wait),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });

        for (auto & item : tasks_to_wait)
            item->is_currently_deleting = true;
    }

    /// Wait for each task to be executed
    for (auto & item : tasks_to_wait)
    {
        item->is_done.wait();
        item.reset();
    }
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::routine(TaskRuntimeDataPtr item)
{
    /// FIXME Review exception-safety of this, remove NOEXCEPT_SCOPE and ALLOW_ALLOCATIONS_IN_SCOPE if possible
    DENY_ALLOCATIONS_IN_SCOPE;

    /// All operations with queues are considered no to do any allocations
    /**
     * 将task从active队列移除。
     * 只要本轮执行结束，无论task是否需要继续执行，都需要从active中移除
     */
    auto erase_from_active = [this](TaskRuntimeDataPtr & item_) TSA_REQUIRES(mutex)
    {
        active.erase(std::remove(active.begin(), active.end(), item_), active.end());
    };

    auto on_task_done = [] (TaskRuntimeDataPtr && item_) TSA_REQUIRES(mutex)
    {
        /// We have to call reset() under a lock, otherwise a race is possible.
        /// Imagine, that task is finally completed (last execution returned false),
        /// we removed the task from both queues, but still have pointer.
        /// The thread that shutdowns storage will scan queues in order to find some tasks to wait for, but will find nothing.
        /// So, the destructor of a task and the destructor of a storage will be executed concurrently.
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            item_->task.reset(); // 解除智能指针，引用减去1，如果没有其它引用，那么对象就会销毁
        });
        item_->is_done.set(); // 将is_done置位，这样，其它在监听这个event的线程就可以收到通知解除阻塞
        item_.reset(); // 解除智能指针，引用减去1，如果没有其它引用，那么对象就会销毁
    };

    // 回调方法，当Task在协程中没有执行完，那么就会重新放入pending队列，等待下一次调度
    auto on_task_restart = [this](TaskRuntimeDataPtr && item_) TSA_REQUIRES(mutex)
    {
        /// After the `guard` destruction `item` has to be in moved from state
        /// Not to own the object it points to.
        /// Otherwise the destruction of the task won't be ordered with the destruction of the
        /// storage.
        pending.push(std::move(item_));
        has_tasks.notify_one();
    };

    String query_id;

    /**
     * 当task已经彻底执行完成，那么就无需再放入pending中了，可以完全标记执行万传给你
     */
    auto release_task = [this, &erase_from_active, &on_task_done, &query_id](TaskRuntimeDataPtr && item_)
    {
        std::lock_guard guard(mutex);
        // 从active queue中移除
        erase_from_active(item_);
        has_tasks.notify_one();

        try
        {
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// In a situation of a lack of memory this method can throw an exception,
            /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
            /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
            item_->task->onCompleted();
        }
        catch (...)
        {
            printExceptionWithRespectToAbort(log, query_id);
        }
        // 析构这个任务
        on_task_done(std::move(item_));
    };
    // task是否需要再次执行(本轮没有执行完)
    bool need_execute_again = false;

    try
    {
        ALLOW_ALLOCATIONS_IN_SCOPE;
        query_id = item->task->getQueryId();
        need_execute_again = item->task->executeStep(); // 调用IExecutableTask::executeStep()的实现
    }
    catch (...)
    {
        if (item->task->printExecutionException())
            printExceptionWithRespectToAbort(log, query_id);
        /// Release the task with exception context.
        /// An exception context is needed to proper delete write buffers without finalization
        release_task(std::move(item)); // 发生异常，不再执行，task失败
        return;
    }

    if (!need_execute_again) // 这个task已经执行结束
    {
        release_task(std::move(item));
        return;
    }
    // 这个task只执行了一部分，还需要继续执行
    {
        std::lock_guard guard(mutex);
        erase_from_active(item); // 先从active中移除，准备放回到pending接受再次调度，即只要task需要被再次调度，一定要经历active -> pending -> active的过程
        // 这个task还没有执行完，但是发现这个task正在被移除(这个task所在的storage或者表正在被关停)，因此也不再执行
        if (item->is_currently_deleting)
        {
            try
            {
                ALLOW_ALLOCATIONS_IN_SCOPE;
                /// An exception context is needed to proper delete write buffers without finalization
                throw Exception(ErrorCodes::ABORTED, "Storage is about to be deleted. Done active task as if it was aborted.");
            }
            catch (...)
            {
                printExceptionWithRespectToAbort(log, query_id);
                on_task_done(std::move(item)); // 析构这个任务
                return; // 直接返回，无需再执行了
            }
        }

        // 将这个task重新放回到pending队列，接受下次调度
        on_task_restart(std::move(item));
    }
}


template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::threadFunction()
{
    setThreadName(name.c_str());

    DENY_ALLOCATIONS_IN_SCOPE;

    while (true)
    {
        try
        {
            TaskRuntimeDataPtr item;
            {
                std::unique_lock lock(mutex);
                has_tasks.wait(lock, [this]() TSA_REQUIRES(mutex) { return !pending.empty() || shutdown; });

                if (shutdown)
                    break;

                item = std::move(pending.pop());
                active.push_back(item);
            }

            routine(std::move(item));
        }
        catch (...)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                tryLogCurrentException(__PRETTY_FUNCTION__);
            });
        }
    }
}


template class MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>;
template class MergeTreeBackgroundExecutor<PriorityRuntimeQueue>;
template class MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;
}
