#include <Common/Exception.h>
#include <Common/GetPriorityForLoadBalancing.h>
#include <Common/Priority.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * 根据负载均衡策略，构造一个 "index -> priority" 的函数。
 * 这里的 priority 只是副本排序 key 的一部分，值越小表示越优先；
 * 后续还会和 error_count、config_priority、random 等一起参与最终排序。
 * @param load_balance
 * @param offset
 * @param pool_size
 * @return
 */
GetPriorityForLoadBalancing::Func
GetPriorityForLoadBalancing::getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const
{
    std::function<Priority(size_t index)> get_priority;
    switch (load_balance)
    {
        case LoadBalancing::NEAREST_HOSTNAME:
            /// 按“副本主机名”和“本地主机名”的前缀距离排序，距离越小越优先。
            if (hostname_prefix_distance.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_prefix_distance is not initialized");
            get_priority = [this](size_t i) { return Priority{static_cast<Int64>(hostname_prefix_distance[i])}; };
            break;
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            /// 按主机名的 Levenshtein 编辑距离排序，距离越小越优先。
            if (hostname_levenshtein_distance.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_levenshtein_distance is not initialized");
            get_priority = [this](size_t i) { return Priority{static_cast<Int64>(hostname_levenshtein_distance[i])}; };
            break;
        case LoadBalancing::IN_ORDER:
            /// 直接按配置中的下标顺序排序：0, 1, 2, ...
            get_priority = [](size_t i) { return Priority{static_cast<Int64>(i)}; };
            break;
        case LoadBalancing::RANDOM:
            /// 不额外指定 priority，后续会退化为依赖随机数打散同优先级副本。
            break;
        case LoadBalancing::FIRST_OR_RANDOM:
            /// 优先 offset 指定的那个副本；其他副本统一给较低优先级。
            get_priority = [offset](size_t i) { return i != offset ? Priority{1} : Priority{0}; };
            break;
        case LoadBalancing::ROUND_ROBIN:
            /// 每次调用都推进 last_used，把“上次使用的下一个副本”放到最高优先级。
            auto local_last_used = last_used % pool_size;
            ++last_used;

            // Example: pool_size = 5
            // | local_last_used | i=0 | i=1 | i=2 | i=3 | i=4 |
            // | 0               | 4   | 0   | 1   | 2   | 3   |
            // | 1               | 3   | 4   | 0   | 1   | 2   |
            // | 2               | 2   | 3   | 4   | 0   | 1   |
            // | 3               | 1   | 2   | 3   | 4   | 0   |
            // | 4               | 0   | 1   | 2   | 3   | 4   |

            get_priority = [pool_size, local_last_used](size_t i)
            {
                /// 把当前轮询起点映射成最小 priority，其余副本按环形顺序依次排在后面。
                size_t priority = pool_size - 1;
                if (i < local_last_used)
                    priority = pool_size - 1 - (local_last_used - i);
                if (i > local_last_used)
                    priority = i - local_last_used - 1;

                return Priority{static_cast<Int64>(priority)};
            };
            break;
    }
    return get_priority;
}

/// Some load balancing strategies (such as "nearest hostname") have preferred nodes to connect to.
/// Usually it's a node in the same data center/availability zone.
/// For other strategies there's no difference between nodes.
bool GetPriorityForLoadBalancing::hasOptimalNode() const
{
    switch (load_balancing)
    {
        case LoadBalancing::NEAREST_HOSTNAME:
            return true;
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            return true;
        case LoadBalancing::IN_ORDER:
            return false;
        case LoadBalancing::RANDOM:
            return false;
        case LoadBalancing::FIRST_OR_RANDOM:
            return true;
        case LoadBalancing::ROUND_ROBIN:
            return false;
    }
}

}
