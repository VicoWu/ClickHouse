#pragma once

#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric QueryPreempted;
}


namespace DB
{

/** Implements query priorities in very primitive way.
  * Allows to freeze query execution if at least one query of higher priority is executed.
  *
  * Priority value is integer, smaller means higher priority.
  *
  * Priority 0 is special - queries with that priority is always executed,
  *  not depends on other queries and not affect other queries.
  * Thus 0 means - don't use priorities.
  *
  * NOTE Possibilities for improvement:
  * - implement limit on maximum number of running queries with same priority.
  * 全局只有一个 QueryPriorities对象
  */
class QueryPriorities
{
public:
    using Priority = int;

private:
    friend struct Handle;

    using Count = int;

    /// Number of currently running queries for each priority.
    using Container = std::map<Priority, Count>;

    std::mutex mutex;
    std::condition_variable condvar;
    Container container; // std::map<Priority, Count>;


    /** If there are higher priority queries - sleep until they are finish or timeout happens.
     * 调用者 void waitIfNeed(Duration timeout)
      */
    template <typename Duration>
    void waitIfNeed(Priority priority, Duration timeout)
    {
        // 如果当前我的priority是0，那么，肯定不需要wait
        if (0 == priority)
            return;
        // 如果我的priority不是0，那么，看看是否有更高优先级(priority值更小)的query
        std::unique_lock lock(mutex);

        /// Is there at least one more priority query?
        bool found = false;
        // 遍历，只要找到一个优先级更高(值更小)就退出循环然后sleep
        for (const auto & value : container)
        {
            /**
             * 由于 container 是按优先级的值升序排列(即按照优先级从高到低排列)的（Priority 小表示高优先级），
             * 所以一旦发现当前遍历的优先级(value.first)大于等于我们要执行的查询的优先级(priority)，
             * 我们就可以认为我们已经找到了优先级更高的查询（因为它们会排在前面，优先级小的值排前面）。
             */
            if (value.first >= priority)
                break;

            if (value.second > 0) // 存在一个优先级的值更小(优先级更高)的正在Running的query
            {
                found = true; // 找到了一个更高优先级的Query
                break;
            }
        }

        if (!found) // 没找到
            return;

        // 找到了，抢占的metrics 加 1
        // 注意，这个metrics是一个Gauge，当生命周期结束的时候，由于对象metric_increment的销毁，就会减去1
        CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryPreempted};

        /// Spurious wakeups are Ok. We allow to wait less than requested.
        // 在 condvar上面等待，直到收到通知，或者超时发生
        // 如果发生超时，wait_for() 会返回，并且锁 mutex 会被重新获取，继续执行后续代码
        condvar.wait_for(lock, timeout);
    }

public:
    struct HandleImpl
    {
    private:
        QueryPriorities & parent; // 这个HandleImpl所属的 QueryPriorities，记录了所有priority的优先级的统计信息
        QueryPriorities::Container::value_type & value; // 这个优先级上的Query的统计信息


        // 构造 , 在 Handle insert(Priority priority) 中调用
    public:
        HandleImpl(QueryPriorities & parent_, QueryPriorities::Container::value_type & value_)
            : parent(parent_), value(value_) {}

        ~HandleImpl()
        {
            {
                std::lock_guard lock(parent.mutex);
                --value.second;
            }
            // 注意，这里是notify_all
            parent.condvar.notify_all(); // 这个QueueStatus结束了，在condvar上执行通知，其它低优先级的Query会收到通知
        }

        // 调用者搜索 priority_handle->waitIfNeed， 实际上会调用QueryPriorities::waitIfNeed
        template <typename Duration>
        void waitIfNeed(Duration timeout)
        {
            // 调用父的 QueryPriorities::waitIfNeed， 因为 QueryPriorities管理了所有priorities的统计信息
            parent.waitIfNeed(value.first, timeout);
        }
    };

    using Handle = std::shared_ptr<HandleImpl>;

    /** Register query with specified priority.
      * Returns an object that remove record in destructor.
      * QueryPriorities::insert
      * 在 ProcessList::insert 中调用
      */
    Handle insert(Priority priority)
    {
        if (0 == priority)
            return {}; // 从这里可以看到，如果Query本身的priority是0，根本不会放到container中，这意味着，一个query的priority是0，既不会受影响，也不会影响别人

        std::lock_guard lock(mutex);
        // emplace方法在不存在 priority 的时候插入成功，在priority已经存在的时候插入失败
        // emplace 返回一个 std::pair，第一个元素是一个迭代器，指向容器中插入或查找的元素，第二个元素是一个布尔值，表示插入是否成功。
        auto it = container.emplace(priority, 0).first;
        ++it->second; // 将it->second递增，表示对应的priority的数量增加1
        return std::make_shared<HandleImpl>(*this, *it); // *it代表这个priority对应的std::pair
    }
};

}
