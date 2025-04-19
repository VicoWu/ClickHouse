#include <Processors/Executors/ExecutorTasks.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ExecutorTasks::finish()
{
    {
        std::lock_guard lock(mutex);
        finished = true;
        async_task_queue.finish();
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
        context->wakeUp();
}

void ExecutorTasks::rethrowFirstThreadException()
{
    for (auto & executor_context : executor_contexts)
        executor_context->rethrowExceptionIfHas();
}

void ExecutorTasks::tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock)
{
    if (!task_queue.empty() && !threads_queue.empty() && !finished)
    {
        size_t next_thread = self.thread_number + 1 >= use_threads ? 0 : (self.thread_number + 1);
        auto thread_to_wake = task_queue.getAnyThreadWithTasks(next_thread);

        if (threads_queue.has(thread_to_wake))
            threads_queue.pop(thread_to_wake);
        else
            thread_to_wake = threads_queue.popAny();

        idle_threads--;
        if (thread_to_wake >= use_threads)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty queue without allocated thread");

        lock.unlock();
        executor_contexts[thread_to_wake]->wakeUp();
    }
}

void ExecutorTasks::tryGetTask(ExecutionThreadContext & context)
{
    {
        std::unique_lock lock(mutex);

    #if defined(OS_LINUX)
        if (num_threads == 1)
        {
            if (auto res = async_task_queue.tryGetReadyTask(lock))
            {
                context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
                return;
            }
        }
    #endif

        /// Try get async task assigned to this thread or any other task from queue.
        if (auto * async_task = context.tryPopAsyncTask())
        {
            context.setTask(async_task);
            --num_waiting_async_tasks;
        }
        else if (!task_queue.empty())
            context.setTask(task_queue.pop(context.thread_number));

        /// Task found.
        if (context.hasTask())
        {
            /// We have to wake up at least one thread if there are pending tasks.
            /// That thread will wake up other threads during its `tryGetTask()` call if any.
            tryWakeUpAnyOtherThreadWithTasks(context, lock);
            return;
        }

        /// This thread has no tasks to do and is going to wait.
        /// Finish execution if this was the last active thread.
        if (threads_queue.size() + 1 == use_threads && async_task_queue.empty() && num_waiting_async_tasks == 0)
        {
            lock.unlock();
            finish();
            return;
        }

    #if defined(OS_LINUX)
        if (num_threads == 1)
        {
            /// If we execute in single thread, wait for async tasks here.
            auto res = async_task_queue.wait(lock);
            if (!res)
            {
                if (finished)
                    return;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty task was returned from async task queue");
            }

            context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
            return;
        }
    #endif

        /// Enqueue thread into stack of waiting threads.
        threads_queue.push(context.thread_number);
        idle_threads++;
    }

    context.wait(finished);
}

/**
 * 把已经准备好的 processor 任务放入对应的执行队列中，供线程执行
 * @param queue 普通同步任务队列，包含一些 ready 状态的 ProcessorNode。
 * @param async_queue 异步任务队列，适用于 AsyncProcessor（如异步 IO）
 * @param context 当前线程的执行上下文对象（包含当前线程是否有任务、是否在运行等）
 */
void ExecutorTasks::pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context)
{
    context.setTask(nullptr);

    /// Take local task from queue if has one.
    if (!queue.empty() && !context.hasAsyncTasks()
        && context.num_scheduled_local_tasks < ExecutionThreadContext::max_scheduled_local_tasks)
    {
        ++context.num_scheduled_local_tasks;
        // 当前线程就可以直接执行这个 task. 优先把任务分配到“本地线程”，减少线程切换。如果线程刚好闲着并且任务数量不多，就给它塞一个。
        context.setTask(queue.front());
        queue.pop();
    }
    else
        context.num_scheduled_local_tasks = 0;

    if (!queue.empty() || !async_queue.empty())
    {
        // 这里开始进入线程安全区（拿锁了），准备处理剩下的队列
        std::unique_lock lock(mutex);

#if defined(OS_LINUX)
        while (!async_queue.empty() && !finished)
        {
            int fd = async_queue.front()->processor->schedule();
            async_task_queue.addTask(context.thread_number, async_queue.front(), fd);
            // schedule() 通常会返回一个 fd，它用来注册到 epoll 等机制上，等 IO 可读可写时再唤醒执行
            // 用到了基于文件描述符的 epoll reactor 模型。
            async_queue.pop(); // AsyncSource, AsyncSink 这样的异步 processor。
        }
#endif

        // 所有线程共用一个 task_queue，通过 thread number 做任务调度
        while (!queue.empty() && !finished)
        {
            // 把queue中的第一个元素放到task_queue中
            task_queue.push(queue.front(), context.thread_number);
            queue.pop(); // 已经放到task_queue中了，弹出这个元素
        }

        /// Wake up at least one thread that will wake up other threads if required
        tryWakeUpAnyOtherThreadWithTasks(context, lock);
    }
}

void ExecutorTasks::init(size_t num_threads_, size_t use_threads_, bool profile_processors, bool trace_processors, ReadProgressCallback * callback)
{
    num_threads = num_threads_;
    use_threads = use_threads_;
    idle_threads = 0;
    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>(i, profile_processors, trace_processors, callback));
    }
}

void ExecutorTasks::fill(Queue & queue, [[maybe_unused]] Queue & async_queue)
{
    std::lock_guard lock(mutex);

    size_t next_thread = 0;
#if defined(OS_LINUX)
    while (!async_queue.empty())
    {
        int fd = async_queue.front()->processor->schedule();
        async_task_queue.addTask(next_thread, async_queue.front(), fd);
        async_queue.pop();

        ++next_thread;

        /// It is important to keep queues empty for threads that are not started yet.
        /// Otherwise that thread can be selected by `tryWakeUpAnyOtherThreadWithTasks()`, leading to deadlock.
        if (next_thread >= use_threads)
            next_thread = 0;
    }
#endif

    while (!queue.empty())
    {
        task_queue.push(queue.front(), next_thread);
        queue.pop();

        ++next_thread;

        /// It is important to keep queues empty for threads that are not started yet.
        /// Otherwise that thread can be selected by `tryWakeUpAnyOtherThreadWithTasks()`, leading to deadlock.
        if (next_thread >= use_threads)
            next_thread = 0;
    }
}

void ExecutorTasks::upscale(size_t use_threads_)
{
    std::lock_guard lock(mutex);
    use_threads = std::max(use_threads, use_threads_);
}

void ExecutorTasks::processAsyncTasks()
{
#if defined(OS_LINUX)
    {
        /// Wait for async tasks.
        std::unique_lock lock(mutex);
        while (auto task = async_task_queue.wait(lock))
        {
            auto * node = static_cast<ExecutingGraph::Node *>(task.data);
            node->processor->onAsyncJobReady();

            executor_contexts[task.thread_num]->pushAsyncTask(node);
            ++num_waiting_async_tasks;

            if (threads_queue.has(task.thread_num))
            {
                threads_queue.pop(task.thread_num);
                idle_threads--;
                executor_contexts[task.thread_num]->wakeUp();
            }
        }
    }
#endif
}

}
