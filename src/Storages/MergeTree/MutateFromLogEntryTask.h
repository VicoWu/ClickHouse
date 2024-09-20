#pragma once

#include <pcg_random.hpp>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/randomSeed.h>

namespace DB
{

class MutateFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{
public:
    template <typename Callback>
    MutateFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
        StorageReplicatedMergeTree & storage_,
        Callback && task_result_callback_)
        : ReplicatedMergeMutateTaskBase(
            &Poco::Logger::get(storage_.getStorageID().getShortName() + "::" + selected_entry_->log_entry->new_part_name + " (MutateFromLogEntryTask)"),
            storage_,
            selected_entry_,
            task_result_callback_)
        , rng(randomSeed())
        {}


    Priority getPriority() const override { return priority; }

private:

    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;

    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;
    /**
     * 搜索 bool ReplicatedMergeMutateTaskBase::executeImpl()  查看调用
     * 这个方法是 MutateFromLogEntryTask::executeInnerTask()
     * 如果executeInnerTask()返回false，调用者会进入下一个stage继续执行，返回true，调用者会在当前stage再次调度，即，会重新执行方法 executeInnerTask()
     * @return
     */
    bool executeInnerTask() override
    {
        return mutate_task->execute(); // MergeTask::execute()
    }

    Priority priority;

    TableLockHolder table_lock_holder{nullptr};
    ReservationSharedPtr reserved_space{nullptr};

    MergeTreePartInfo new_part_info;
    MutationCommandsConstPtr commands;

    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;
    StopwatchUniquePtr stopwatch_ptr{nullptr};

    MergeTreeData::MutableDataPartPtr new_part{nullptr};
    FutureMergedMutatedPartPtr future_mutated_part{nullptr};

    MutateTaskPtr mutate_task;
    pcg64 rng;
};


}
