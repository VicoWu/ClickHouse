#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Interpreters/Context.h>
#include <pcg_random.hpp>
#include <random>

namespace DB
{

BackgroundJobsAssignee::BackgroundJobsAssignee(MergeTreeData & data_, BackgroundJobsAssignee::Type type_, ContextPtr global_context_)
    : WithContext(global_context_)
    , data(data_)
    , sleep_settings(global_context_->getBackgroundMoveTaskSchedulingSettings()) // 这里的settings是使用的getBackgroundMoveTaskSchedulingSettings()
    , rng(randomSeed())
    , type(type_)
{
}

void BackgroundJobsAssignee::trigger()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    /// Do not reset backoff factor if some task has appeared,
    /// but decrease it exponentially on every new task.
    no_work_done_count /= 2; // 任务成功，等待时间指数递减
    /// We have background jobs, schedule task as soon as possible
    //     using TaskInfo = BackgroundSchedulePoolTaskInfo;
    //    using TaskInfoPtr = std::shared_ptr<TaskInfo>;
    //    using TaskFunc = std::function<void()>;
    //    using TaskHolder = BackgroundSchedulePoolTaskHolder;
    holder->schedule(); // bool BackgroundSchedulePoolTaskInfo::schedule() -> void BackgroundSchedulePoolTaskInfo::scheduleImpl ->  BackgroundSchedulePool::scheduleTask(TaskInfoPtr task_info)
}

void BackgroundJobsAssignee::postpone()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    no_work_done_count += 1; // 任务调度失败的次数线性递增，如果失败的任务很多，那么递增时间也会递增
    double random_addition = std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);
    size_t next_time_to_execute = static_cast<size_t>(
        1000 * (std::min(
            // 计算出的延迟时间不会超过一个最大值 sleep_settings.task_sleep_seconds_when_no_work_max。这是为了避免延迟时间过长而导致任务长时间不被执行
            sleep_settings.task_sleep_seconds_when_no_work_max,
            // sleep_settings.task_sleep_seconds_when_no_work_multiplier 的 no_work_done_count 次方。这意味着随着失败次数的增加，延迟时间会按指数递增,默认是1.1
            sleep_settings.thread_sleep_seconds_if_nothing_to_do * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_count))
        + random_addition));

    holder->scheduleAfter(next_time_to_execute, false);
}


/**
 * 这里的task是 MergeFromLogEntryTask 或者 MutateFromLogEntryTask
 * @param merge_task
 * @return
 */
bool BackgroundJobsAssignee::scheduleMergeMutateTask(ExecutableTaskPtr merge_task)
{
    //  MergeMutateBackgroundExecutorPtr Context::getMergeMutateExecutor()
    //  bool MergeTreeBackgroundExecutor<Queue>::trySchedule(ExecutableTaskPtr task)
    //  using MergeMutateBackgroundExecutor = MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;
    //  using MergeMutateBackgroundExecutorPtr = std::shared_ptr<MergeMutateBackgroundExecutor>;
    bool res = getContext()->getMergeMutateExecutor()->trySchedule(merge_task); // 返回true代表调度成功，这个task随后会在对应的MergeMutateBackgroundExecutor的pool中执行
    res // 如果  MergeTreeBackgroundExecutor的pool能够调度成功，那么进行trigger
        ? trigger()  // 调用对应的holder的schedule()方法 ， BackgroundJobsAssignee::trigger() ->  bool BackgroundSchedulePoolTaskInfo::schedule() -> void BackgroundSchedulePoolTaskInfo::scheduleImpl
        : postpone(); // postpone的原因是task满了，延迟调度
    return res;
}


bool BackgroundJobsAssignee::scheduleFetchTask(ExecutableTaskPtr fetch_task)
{
    bool res = getContext()->getFetchesExecutor()->trySchedule(fetch_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleMoveTask(ExecutableTaskPtr move_task)
{
    bool res = getContext()->getMovesExecutor()->trySchedule(move_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleCommonTask(ExecutableTaskPtr common_task, bool need_trigger)
{

    bool schedule_res = getContext()->getCommonExecutor()->trySchedule(common_task);
    schedule_res && need_trigger ? trigger() : postpone();
    return schedule_res;
}


String BackgroundJobsAssignee::toString(Type type)
{
    switch (type)
    {
        case Type::DataProcessing:
            return "DataProcessing";
        case Type::Moving:
            return "Moving";
    }
    UNREACHABLE();
}

/**
 * 启动，并设置对应的holder
 */
void BackgroundJobsAssignee::start()
{
    std::lock_guard lock(holder_mutex);
    if (!holder)
        // BackgroundSchedulePool::createTask , 创建的holder中持有了 getContext()->getSchedulePool()
        holder = getContext()->getSchedulePool().createTask("BackgroundJobsAssignee:" + toString(type), [this]{ threadFunc(); });
    // 激活并且开始不断执行，bool BackgroundSchedulePoolTaskInfo::activateAndSchedule() -> BackgroundSchedulePoolTaskInfo::scheduleImpl -> void BackgroundSchedulePool::scheduleTask(TaskInfoPtr task_info)
    holder->activateAndSchedule(); // 在 getContext()->getSchedulePool()的这个pool中执行
}

void BackgroundJobsAssignee::finish()
{
    /// No lock here, because scheduled tasks could call trigger method
    if (holder)
    {
        holder->deactivate();

        auto storage_id = data.getStorageID();

        getContext()->getMovesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getFetchesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getMergeMutateExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getCommonExecutor()->removeTasksCorrespondingToStorage(storage_id);
    }
}

/**
 * 在void BackgroundJobsAssignee::start()中调用
 */
void BackgroundJobsAssignee::threadFunc()
try
{
    bool succeed = false;
    switch (type)
    {
        case Type::DataProcessing:
            // data的实例是 StorageReplicatedMergeTree
            succeed = data.scheduleDataProcessingJob(*this); // 搜索 StorageReplicatedMergeTree::scheduleDataProcessingJob，负责从queue中取出任务，交给线程池执行
            break;
        case Type::Moving:
            succeed = data.scheduleDataMovingJob(*this);
            break;
    }

    if (!succeed) // 任务调度失败，那么延迟调度
        postpone(); // 如果失败需要postpone，如果成功则不postpone
}
catch (...) /// Catch any exception to avoid thread termination.
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    postpone(); // 失败，那么延迟调度
}

BackgroundJobsAssignee::~BackgroundJobsAssignee()
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
