#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Common/CurrentMemoryTracker.h>


#ifdef MEMORY_TRACKER_DEBUG_CHECKS
thread_local bool memory_tracker_always_throw_logical_error_on_allocation = false;
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

/**
 * 这只是一个free function，不属于某个类或者对象
 * @return
 */
MemoryTracker * getMemoryTracker()
{
    // 获取当前线程的MemoryTracker, 如果有，则直接返回，没有则返回null
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        return thread_memory_tracker;

    /// Once the main thread is initialized,
    /// total_memory_tracker is initialized too.
    /// And can be used, since MainThreadStatus is required for profiling.
    if (DB::MainThreadStatus::get()) // 使用total_memory_tracker
        return &total_memory_tracker; // main thread的total_memory_tracker
    // 即没有从当前线程中找到对应的MemoryTracker，同时也没有MainThread(ClickHouseServer还没有启动？)
    return nullptr;
}

}

using DB::current_thread;

/**
 * 静态方法
 * @param size
 * @param throw_if_memory_exceeded
 * @return
 */
AllocationTrace CurrentMemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded)
{
#ifdef MEMORY_TRACKER_DEBUG_CHECKS
    if (unlikely(memory_tracker_always_throw_logical_error_on_allocation))
    {
        memory_tracker_always_throw_logical_error_on_allocation = false;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Memory tracker: allocations not allowed.");
    }
#endif
    // 优先尝试获取当前线程的MemoryTracker实例
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread) // 当前线程的thread_local的ThreadStatus对象
        {
            Int64 will_be = current_thread->untracked_memory + size; // 更新对象的

            if (will_be > current_thread->untracked_memory_limit)
            {
                // 将这个大小(untracked_memory + size)的内存记录到当前线程的memory_tracker实例中去
                auto res = memory_tracker->allocImpl(will_be, throw_if_memory_exceeded);
                current_thread->untracked_memory = 0;
                return res;
            }

            /// Update after successful allocations,
            /// since failed allocations should not be take into account.
            current_thread->untracked_memory = will_be; // untracked_memory变成will_be，即增加了size大小
        }
        /// total_memory_tracker only, ignore untracked_memory
        else // 当前线程没有设置 current_thread对象
        {
            // 将这个大小的内存记录到当前线程的memory_tracker实例中去
            return memory_tracker->allocImpl(size, throw_if_memory_exceeded);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::check()
{
    if (auto * memory_tracker = getMemoryTracker())
        std::ignore = memory_tracker->allocImpl(0, true);
}

AllocationTrace CurrentMemoryTracker::alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace CurrentMemoryTracker::free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                Int64 untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;
                return memory_tracker->free(-untracked_memory);
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->free(size);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}

