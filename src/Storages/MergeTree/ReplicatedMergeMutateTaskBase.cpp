#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Common/ProfileEventsScope.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int PART_IS_TEMPORARILY_LOCKED;
}

StorageID ReplicatedMergeMutateTaskBase::getStorageID() const
{
    return storage.getStorageID();
}

void ReplicatedMergeMutateTaskBase::onCompleted()
{
    bool successfully_executed = state == State::SUCCESS;
    task_result_callback(successfully_executed);
}

/**
 * 调用者 void MergeTreeBackgroundExecutor<Queue>::routine
 * 该方法返回的是 need_execute_again = item->task->executeStep();
 * 即返回true代表需要重新执行(可能是执行下一个state -> stage -> mini-task)，返回false不需要重新执行(最后一个State的最后一个Stage的最后一个mini-task)
 * @return
 */
bool ReplicatedMergeMutateTaskBase::executeStep()
{
    /// Metrics will be saved in the local profile_counters.
    ProfileEventsScope profile_events_scope(&profile_counters);

    std::exception_ptr saved_exception;

    bool retryable_error = false;
    try
    {
        /// We don't have any backoff for failed entries
        /// we just count amount of tries for each of them.

        try
        {
            return executeImpl(); // bool ReplicatedMergeMutateTaskBase::executeImpl()
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
            {
                /// If no one has the right part, probably not all replicas work; We will not write to log with Error level.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
            }
            else if (e.code() == ErrorCodes::ABORTED)
            {
                /// Interrupted merge or downloading a part is not an error.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
            }
            else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                /// Part cannot be added temporarily
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
                storage.cleanup_thread.wakeup();
            }
            else
                tryLogCurrentException(log, __PRETTY_FUNCTION__);

            /// This exception will be written to the queue element, and it can be looked up using `system.replication_queue` table.
            /// The thread that performs this action will sleep a few seconds after the exception.
            /// See `queue.processEntry` function.
            throw; // 抛出异常，继续执行
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            throw;
        }

    }
    // 抛出异常，继续执行下面的逻辑
    catch (...)
    {
        saved_exception = std::current_exception();
    }
    // 执行到这里，肯定是抛出异常了，这里查看是否是可重试的error
    if (!retryable_error && saved_exception)
    {
        std::lock_guard lock(storage.queue.state_mutex);

        auto & log_entry = selected_entry->log_entry;

        log_entry->exception = saved_exception;
        log_entry->last_exception_time = time(nullptr);

        if (log_entry->type == ReplicatedMergeTreeLogEntryData::MUTATE_PART)
        {
            /// Record the exception in the system.mutations table.
            Int64 result_data_version = MergeTreePartInfo::fromPartName(log_entry->new_part_name, storage.queue.format_version)
                .getDataVersion();
            auto source_part_info = MergeTreePartInfo::fromPartName(
                log_entry->source_parts.at(0), storage.queue.format_version);

            auto in_partition = storage.queue.mutations_by_partition.find(source_part_info.partition_id);
            if (in_partition != storage.queue.mutations_by_partition.end())
            {
                auto mutations_begin_it = in_partition->second.upper_bound(source_part_info.getDataVersion());
                auto mutations_end_it = in_partition->second.upper_bound(result_data_version);
                for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
                {
                    ReplicatedMergeTreeQueue::MutationStatus & status = *it->second;
                    status.latest_failed_part = log_entry->source_parts.at(0);
                    status.latest_failed_part_info = source_part_info;
                    status.latest_fail_time = time(nullptr);
                    status.latest_fail_reason = getExceptionMessage(saved_exception, false);
                }
            }
        }
    }

    if (retryable_error) // 可重试的error
        print_exception = false;

    if (saved_exception)
        std::rethrow_exception(saved_exception);

    return false; // 不需要重新执行，也就是说这个大的task现在宣告失败，不会再占用当前的Slot。如果有需要，后面会重新生成task
}

/**
 * 调用者是 ReplicatedMergeMutateTaskBase::executeStep()。而executeStep()直接返回了该方法的返回结果，结果代表是否需要重新执行
 * 返回true，需要重新执行，返回false，不需要重新执行
 * @return
 */
bool ReplicatedMergeMutateTaskBase::executeImpl()
{
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_mutate_entry)
        switcher.emplace((*merge_mutate_entry)->thread_group);
    // 删除已经处理的Entry的labmda
    auto remove_processed_entry = [&] () -> bool
    {
        try
        {
            storage.queue.removeProcessedEntry(storage.getZooKeeper(), selected_entry->log_entry); // 从queue中删除这个entry(也会从zookeeper上删除)
            state = State::SUCCESS;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return false;
    };


    auto execute_fetch = [&] (bool need_to_check_missing_part) -> bool
    {
        if (storage.executeFetch(entry, need_to_check_missing_part))
            return remove_processed_entry();

        return false;
    };


    switch (state)
    {
        case State::NEED_PREPARE :
        {
            {
                auto res = checkExistingPart();
                /// Depending on condition there is no need to execute a merge
                if (res == CheckExistingPartResult::PART_EXISTS)
                    return remove_processed_entry(); // 如果我们发现这个part已经存在，那么就删除这个LogEntry
            }

            auto prepare_result = prepare();

            part_log_writer = prepare_result.part_log_writer;

            /// Avoid rescheduling, execute fetch here, in the same thread.
            if (!prepare_result.prepared_successfully) // 没有准备好，在当前thread中先拉取part，再进入下一个state
                return execute_fetch(prepare_result.need_to_check_missing_part_in_fetch);

            state = State::NEED_EXECUTE_INNER_MERGE;
            return true; // 到达下一个state，需要继续执行。这里可以看到，肯定不会在当前Thread继续执行，而是肯定会退出,然后基于调度算法进行下一次的调度
        }
        case State::NEED_EXECUTE_INNER_MERGE :
        {
            try
            {
                // 可以看到，如果executeInnerTask()返回true，这里就不会到达State::NEED_FINALIZE，而是直接返回true
                if (!executeInnerTask()) // 真正执行，所以 executeInnerTask()返回false代表成功，返回true代表失败
                {
                    state = State::NEED_FINALIZE;
                    return true; // 到达下一个state，需要继续执行。这里可以看到，肯定不会在当前Thread继续执行，而是肯定会退出,然后基于调度算法进行下一次的调度
                }
            }
            catch (...)
            {
                if (part_log_writer)
                    part_log_writer(ExecutionStatus::fromCurrentException("", true));
                throw;
            }

            return true; //  还是当前的State，需要重新执行。这里可以看到，肯定不会在当前Thread继续执行，而是肯定会退出,然后基于调度算法进行下一次的调度
        }
        case State::NEED_FINALIZE :
        {
            try
            {
                if (!finalize(part_log_writer))
                    return execute_fetch(/* need_to_check_missing = */true);
            }
            catch (...)
            {
                if (part_log_writer)
                    part_log_writer(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
            // 执行完成以后才会删除,把entry从queue中删除
            return remove_processed_entry();
        }
        case State::SUCCESS :
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Do not call execute on previously succeeded task");
        }
    }
    return false; // 所有state -> 所有stage -> 所有task全部执行结束
}


ReplicatedMergeMutateTaskBase::CheckExistingPartResult ReplicatedMergeMutateTaskBase::checkExistingPart()
{
    /// If we already have this part or a part covering it, we do not need to do anything.
    /// The part may be still in the PreActive -> Active transition so we first search
    /// among PreActive parts to definitely find the desired part if it exists.
    MergeTreeData::DataPartPtr existing_part = storage.getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreActive});

    if (!existing_part)
        existing_part = storage.getActiveContainingPart(entry.new_part_name);

    /// Even if the part is local, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
    if (existing_part && storage.getZooKeeper()->exists(fs::path(storage.replica_path) / "parts" / existing_part->name))
    {
        LOG_DEBUG(log, "Skipping action for part {} because part {} already exists.", entry.new_part_name, existing_part->name);

        /// We will exit from all the execution process
        return CheckExistingPartResult::PART_EXISTS;
    }


    return CheckExistingPartResult::OK;
}


}
