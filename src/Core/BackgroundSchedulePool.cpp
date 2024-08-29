#include "BackgroundSchedulePool.h"
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <chrono>


namespace DB
{

namespace ErrorCodes { extern const int CANNOT_SCHEDULE_TASK; }


BackgroundSchedulePoolTaskInfo::BackgroundSchedulePoolTaskInfo(
    BackgroundSchedulePool & pool_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_)
    : pool(pool_), log_name(log_name_), function(function_)
{
}

/**
 * 这个方法用于立即调度一个任务。具体操作步骤如下：
 */

bool BackgroundSchedulePoolTaskInfo::schedule()
{
    std::lock_guard lock(schedule_mutex);
    // 如果这个BackgroundSchedulePoolTaskInfo 是 deactivated或者scheduled状态，不再调度
    if (deactivated || scheduled)
        return false;
    // void BackgroundSchedulePoolTaskInfo::scheduleImpl
    scheduleImpl(lock); // 将这个task放在对应的BackgroundSchedulePool中执行
    return true;
}

bool BackgroundSchedulePoolTaskInfo::scheduleAfter(size_t milliseconds, bool overwrite, bool only_if_scheduled)
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;
    if (delayed && !overwrite) // 已经被延迟调度 (delayed == true)，并且 overwrite 参数为 false，则方法返回 false，表示不会重新调度任务。
        return false;
    if (!delayed && only_if_scheduled)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), milliseconds, lock);
    return true;
}

void BackgroundSchedulePoolTaskInfo::deactivate()
{
    std::lock_guard lock_exec(exec_mutex);
    std::lock_guard lock_schedule(schedule_mutex);

    if (deactivated)
        return;

    deactivated = true;
    scheduled = false;

    if (delayed)
        pool.cancelDelayedTask(shared_from_this(), lock_schedule);
}

void BackgroundSchedulePoolTaskInfo::activate()
{
    std::lock_guard lock(schedule_mutex);
    deactivated = false;
}

bool BackgroundSchedulePoolTaskInfo::activateAndSchedule()
{
    std::lock_guard lock(schedule_mutex);

    deactivated = false;
    if (scheduled)
        return false;

    scheduleImpl(lock); // BackgroundSchedulePoolTaskInfo::scheduleImpl
    return true;
}

std::unique_lock<std::mutex> BackgroundSchedulePoolTaskInfo::getExecLock()
{
    return std::unique_lock{exec_mutex};
}

void BackgroundSchedulePoolTaskInfo::execute()
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{pool.tasks_metric};

    std::lock_guard lock_exec(exec_mutex);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return;
        // 设置任务调度状态标记为false,如果任务执行期间没有其它地方重新执行schedule()，那么这里执行完了就不会再调度
        scheduled = false;
        executing = true;
    }

    try
    {
        function(); // 执行任务
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false && "Tasks in BackgroundSchedulePool cannot throw");
    }
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static constexpr UInt64 slow_execution_threshold_ms = 200;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_TRACE(&Poco::Logger::get(log_name), "Execution took {} ms.", milliseconds);

    {
        // 再次锁定 schedule_mutex 互斥锁，将 executing 标志设置为 false，表示任务执行完毕
        std::lock_guard lock_schedule(schedule_mutex);

        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
        /// on the queue. We don't call the function again here because this way all tasks
        /// will have their chance to execute
        // 检查任务在执行期间是否被重新调度（scheduled 为 true）。如果是，则将任务重新加入任务队列中执行。
        if (scheduled)
            pool.scheduleTask(shared_from_this()); // 将当前的Task推送到pool中
    }
}

void BackgroundSchedulePoolTaskInfo::scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock)
{
    scheduled = true; // scheduled 标记位置位，防止这个task被重复调度
    /**
     * 如果任务之前是被延迟执行的（即 delayed 为 true），
     * 那么就通过调用 pool.cancelDelayedTask() 取消这个延迟执行的计划。这样做的目的是确保任务不再被延迟，而是马上执行。
     */
    if (delayed) //
        pool.cancelDelayedTask(shared_from_this(), schedule_mutex_lock);

    /// If the task is not executing at the moment, enqueue it for immediate execution.
    /// But if it is currently executing, do nothing because it will be enqueued
    /// at the end of the execute() method.
    /**
     *  如果任务没有在执行，那么通过调用 pool.scheduleTask(shared_from_this()) 立即将任务加入到执行队列中。
        如果任务正在执行，则不再重新安排它，因为任务会在当前执行完成后自动重新加入队列等待下一次执行。
        void BackgroundSchedulePool::scheduleTask(TaskInfoPtr task_info)
     */
    if (!executing)
        // 这里的Pool是 BackgroundSchedulePool，不是真正的线程池ThreadPool
        pool.scheduleTask(shared_from_this()); // shared_from_this()代表当前的 BackgroundSchedulePoolTaskInfo 对象
}

Coordination::WatchCallback BackgroundSchedulePoolTaskInfo::getWatchCallback()
{
     return [task = shared_from_this()](const Coordination::WatchResponse &)
     {
        task->schedule(); // 这个回调执行的就是重新调度
     };
}


BackgroundSchedulePool::BackgroundSchedulePool(size_t size_, CurrentMetrics::Metric tasks_metric_, CurrentMetrics::Metric size_metric_, const char *thread_name_)
    : tasks_metric(tasks_metric_)
    , size_metric(size_metric_, size_)
    , thread_name(thread_name_)
{
    LOG_INFO(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Create BackgroundSchedulePool with {} threads", size_);

    threads.resize(size_);

    try
    {
        for (auto & thread : threads)
            thread = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });

        delayed_thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { delayExecutionThreadFunction(); });
    }
    catch (...)
    {
        LOG_FATAL(
            &Poco::Logger::get("BackgroundSchedulePool/" + thread_name),
            "Couldn't get {} threads from global thread pool: {}",
            size_,
            getCurrentExceptionCode() == DB::ErrorCodes::CANNOT_SCHEDULE_TASK
                ? "Not enough threads. Please make sure max_thread_pool_size is considerably "
                  "bigger than background_schedule_pool_size."
                : getCurrentExceptionMessage(/* with_stacktrace */ true));
        abort();
    }
}


void BackgroundSchedulePool::increaseThreadsCount(size_t new_threads_count)
{
    const size_t old_threads_count = threads.size();

    if (new_threads_count < old_threads_count)
    {
        LOG_WARNING(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name),
            "Tried to increase the number of threads but the new threads count ({}) is not greater than old one ({})", new_threads_count, old_threads_count);
        return;
    }

    threads.resize(new_threads_count);
    for (size_t i = old_threads_count; i < new_threads_count; ++i)
        threads[i] = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });

    size_metric.changeTo(new_threads_count);
}


BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        {
            std::lock_guard lock_tasks(tasks_mutex);
            std::lock_guard lock_delayed_tasks(delayed_tasks_mutex);

            shutdown = true;
        }

        tasks_cond_var.notify_all();
        delayed_tasks_cond_var.notify_all();

        LOG_TRACE(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Waiting for threads to finish.");
        delayed_thread->join();

        for (auto & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}



BackgroundSchedulePool::TaskHolder BackgroundSchedulePool::createTask(const std::string & name, const TaskFunc & function)
{
    // BackgroundSchedulePoolTaskHolder
    // using TaskHolder = BackgroundSchedulePoolTaskHolder;
    // 将当前的pool传入TaskHolder
    return TaskHolder(std::make_shared<TaskInfo>(*this, name, function));
}

void BackgroundSchedulePool::scheduleTask(TaskInfoPtr task_info)
{
    {
        std::lock_guard tasks_lock(tasks_mutex);
        tasks.push_back(std::move(task_info)); //将任务推送到tasks中
    }

    tasks_cond_var.notify_one();
}

void BackgroundSchedulePool::scheduleDelayedTask(const TaskInfoPtr & task, size_t ms, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_mutex);

        if (task->delayed)
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
        task->delayed = true;
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(const TaskInfoPtr & task, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    {
        std::lock_guard lock(delayed_tasks_mutex);
        delayed_tasks.erase(task->iterator);
        task->delayed = false;
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::threadFunction()
{
    setThreadName(thread_name.c_str());

    while (!shutdown)
    {
        // using TaskInfo = BackgroundSchedulePoolTaskInfo;
        // using TaskInfoPtr = std::shared_ptr<TaskInfo>;
        TaskInfoPtr task;
        {
            std::unique_lock<std::mutex> tasks_lock(tasks_mutex);

            tasks_cond_var.wait(tasks_lock, [&]()
            {
                return shutdown || !tasks.empty();
            });

            if (!tasks.empty())
            {
                task = tasks.front();
                tasks.pop_front();
            }
        }

        if (task)
            task->execute(); // 调用 BackgroundSchedulePoolTaskInfo::execute()
    }
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName((thread_name + "/D").c_str());

    while (!shutdown)
    {
        TaskInfoPtr task;
        bool found = false;

        {
            std::unique_lock lock(delayed_tasks_mutex);

            while (!shutdown)
            {
                Poco::Timestamp min_time;

                if (!delayed_tasks.empty())
                {
                    auto t = delayed_tasks.begin();
                    min_time = t->first;
                    task = t->second;
                }

                if (!task)
                {
                    delayed_tasks_cond_var.wait(lock);
                    continue;
                }

                Poco::Timestamp current_time;

                if (min_time > current_time)
                {
                    delayed_tasks_cond_var.wait_for(lock, std::chrono::microseconds(min_time - current_time));
                    continue;
                }
                else
                {
                    /// We have a task ready for execution
                    found = true;
                    break;
                }
            }
        }

        if (found)
            task->schedule();
    }
}

}
