#pragma once

#include <memory>
#include <utility>

#include <pcg_random.hpp>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ZeroCopyLock.h>


namespace DB
{

class MergeFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{
public:
    MergeFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_, // 选择的对应Merge Task的LogEntry
        StorageReplicatedMergeTree & storage_, // 对应的StorageReplicatedMergeTree
        // callback的定义是 common_assignee_trigger = [this] (bool delay) noexcept
        // 对应的callback，callback的调用发生在 void ReplicatedMergeMutateTaskBase::onCompleted()
        IExecutableTask::TaskResultCallback & task_result_callback_);

    Priority getPriority() const override { return priority; }

protected:
    /// Both return false if we can't execute merge.
    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;
    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;
    /**
     * 搜索 bool ReplicatedMergeMutateTaskBase::executeImpl()  查看调用
     * 这个方法是 MergeFromLogEntryTask::executeInnerTask()
     * 如果executeInnerTask()返回false，调用者会进入下一个stage继续执行，返回true，调用者会在当前stage再次调度，即，会重新执行方法 executeInnerTask()
     * @return
     */
    bool executeInnerTask() override
    {
        return merge_task->execute(); // MergeTask::execute()
    }

private:
    TableLockHolder table_lock_holder{nullptr};

    MergeTreeData::DataPartsVector parts;
    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;

    StopwatchUniquePtr stopwatch_ptr{nullptr};
    MergeTreeData::MutableDataPartPtr part;

    Priority priority;

    MergeTaskPtr merge_task;
    pcg64 rng;
};


using MergeFromLogEntryTaskPtr = std::shared_ptr<MergeFromLogEntryTask>;


}
