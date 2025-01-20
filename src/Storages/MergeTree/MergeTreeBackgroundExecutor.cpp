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
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int INVALID_CONFIG_PARAMETER;
}

/**
 *
构造方法查看  shared->merge_mutate_executor = std::make_shared<MergeMutateBackgroundExecutor>
 */
template <class Queue>
MergeTreeBackgroundExecutor<Queue>::MergeTreeBackgroundExecutor(
    String name_,
    size_t threads_count_,
    size_t max_tasks_count_,
    CurrentMetrics::Metric metric_,
    CurrentMetrics::Metric max_tasks_metric_,
    std::string_view policy)
    : name(name_)
    , threads_count(threads_count_) // 16
    , max_tasks_count(max_tasks_count_) // 16 * 2
    , metric(metric_)  // 0 CurrentMetrics::BackgroundMergesAndMutationsPoolTask
    , max_tasks_metric(max_tasks_metric_, 2 * max_tasks_count) // active + pending CurrentMetrics::BackgroundMergesAndMutationsPoolSize
    , pool(std::make_unique<ThreadPool>(
          CurrentMetrics::MergeTreeBackgroundExecutorThreads, CurrentMetrics::MergeTreeBackgroundExecutorThreadsActive))
{
    if (max_tasks_count == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Task count for MergeTreeBackgroundExecutor must not be zero");

    pending.setCapacity(max_tasks_count); // 16 * 2
    active.set_capacity(max_tasks_count); // 16 * 2

    // Pool中的max_threads和queue_size都没有考虑background_merges_mutations_concurrency_ratio
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
size_t MergeTreeBackgroundExecutor<Queue>::getMaxTasksCount() const
{
    return max_tasks_count.load(std::memory_order_relaxed);
}

/**
 * 调用者是 bool BackgroundJobsAssignee::scheduleMergeMutateTask
 * 这是对这个Task的首次调度，以后，这个Task还会通过反复调度来实现状态机的转换
 * @tparam Queue
 * @param task
 * @return
 */
template <class Queue>
bool MergeTreeBackgroundExecutor<Queue>::trySchedule(ExecutableTaskPtr task)
{
    std::lock_guard lock(mutex);

    if (shutdown)
        return false;

    auto & value = CurrentMetrics::values[metric];
    if (value.load() >= static_cast<int64_t>(max_tasks_count)) // 已经超过了最大的task(16 * 2 = 32)的数量
        return false;
    // 向pending中添加，添加的时候就会将BackgroundMergesAndMutationsPoolTask加1，因此，这个BackgroundMergesAndMutationsPoolTask
    // 表示的是pending + active的所有的task的数量
    pending.push(std::make_shared<TaskRuntimeData>(std::move(task), metric));

    has_tasks.notify_one();
    return true;
}


template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::removeTasksCorrespondingToStorage(StorageID id)
{
    std::vector<TaskRuntimeDataPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Erase storage related tasks from pending and select active tasks to wait for
        pending.remove(id);

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

/**
 * 在 void MergeTreeBackgroundExecutor<Queue>::threadFunction() 中调用
 */
template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::routine(TaskRuntimeDataPtr item)
{
    /// FIXME Review exception-safety of this, remove NOEXCEPT_SCOPE and ALLOW_ALLOCATIONS_IN_SCOPE if possible
    DENY_ALLOCATIONS_IN_SCOPE;

    /// All operations with queues are considered no to do any allocations

    auto erase_from_active = [this, &item]() TSA_REQUIRES(mutex)
    {
        // 从active中取出来
        active.erase(std::remove(active.begin(), active.end(), item), active.end());
    };

    bool need_execute_again = false;

    try
    {
        ALLOW_ALLOCATIONS_IN_SCOPE;
        // 在Pool的当前线程中执行，如果返回true，代表还没执行完，需要继续执行这个task的下一个state
        need_execute_again = item->task->executeStep();
    }
    catch (const Exception & e)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            if (e.code() == ErrorCodes::ABORTED)    /// Cancelled merging parts is not an error - log as info.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            else
                tryLogCurrentException(__PRETTY_FUNCTION__);
        });
    }
    catch (...)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        });
    }

    if (need_execute_again) // 重新执行
    {
        std::lock_guard guard(mutex);
        erase_from_active();

        if (item->is_currently_deleting)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                item->task.reset();
            });
            item->is_done.set();
            item = nullptr;
            return;
        }

        /// After the `guard` destruction `item` has to be in moved from state
        /// Not to own the object it points to.
        /// Otherwise the destruction of the task won't be ordered with the destruction of the
        /// storage.
        // 任务需要重新放回pending中
        // 注意，这里不是重新构造一个Task，而是 1. 把这个Task从active中移除赖 2. 运行这个Task 3. 重新放回pending
        // 因此这时候BackgroundMergesAndMutationsPoolTask 不会增加
        pending.push(std::move(item));
        has_tasks.notify_one();
        item = nullptr;
        return;
    }

    {
        std::lock_guard guard(mutex);
        erase_from_active();
        has_tasks.notify_one();

        try
        {
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// In a situation of a lack of memory this method can throw an exception,
            /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
            /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
            item->task->onCompleted();
        }
        catch (const Exception & e)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                if (e.code() == ErrorCodes::ABORTED)    /// Cancelled merging parts is not an error - log as info.
                    LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                else
                    tryLogCurrentException(__PRETTY_FUNCTION__);
            });
        }
        catch (...)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                tryLogCurrentException(__PRETTY_FUNCTION__);
            });
        }


        /// We have to call reset() under a lock, otherwise a race is possible.
        /// Imagine, that task is finally completed (last execution returned false),
        /// we removed the task from both queues, but still have pointer.
        /// The thread that shutdowns storage will scan queues in order to find some tasks to wait for, but will find nothing.
        /// So, the destructor of a task and the destructor of a storage will be executed concurrently.
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                item->task.reset();
            });
        }

        item->is_done.set();
        item = nullptr;
    }
}


/**
 * Pool中每一个独立的线程都会支持持续执行这个threadFunction方法
 * @tparam Queue
 */
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

                item = std::move(pending.pop()); // 从pending中取出
                active.push_back(item); // 放入到active中
            }

            routine(std::move(item)); // 调用 MergeTreeBackgroundExecutor<Queue>::routine
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
