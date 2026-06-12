#if defined(OS_LINUX)

#include <Client/HedgedConnectionsFactory.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Core/ProtocolDefines.h>

namespace ProfileEvents
{
    extern const Event HedgedRequestsChangeReplica;
    extern const Event DistributedConnectionFailAtAll;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int LOGICAL_ERROR;
}

/// 负责向多个 replica 异步建立 TCP 连接。连接分阶段进行：某个 replica 长时间无响应时，
/// 会开始连下一个 replica，但不会取消已经在进行中的连接。全程用 epoll 非阻塞调度。
HedgedConnectionsFactory::HedgedConnectionsFactory(
    const ConnectionPoolWithFailoverPtr & pool_, // 一个pool代表的是指向一个Shard内的不同的Replica的pool
    const Settings & settings_,
    const ConnectionTimeouts & timeouts_,
    UInt64 max_tries_,
    bool fallback_to_stale_replicas_,
    UInt64 max_parallel_replicas_,
    bool skip_unavailable_shards_,
    std::shared_ptr<QualifiedTableName> table_to_check_,
    GetPriorityForLoadBalancing::Func priority_func)
    : pool(pool_) // 一个pool代表的是指向一个Shard内的不同的Replica的pool
    , timeouts(timeouts_)
    , table_to_check(table_to_check_)
    , log(getLogger("HedgedConnectionsFactory"))
    , max_tries(max_tries_)
    , fallback_to_stale_replicas(fallback_to_stale_replicas_)
    , max_parallel_replicas(max_parallel_replicas_)
    , skip_unavailable_shards(skip_unavailable_shards_)
{
    /// 按负载均衡策略打乱本 shard 下各 replica 的连接池顺序。
    shuffled_pools = pool->getShuffledPools(settings_, priority_func, /* use_slowdown_count */ true);

    /// 为每个 replica 连接池创建一个异步建连器，后续由 epoll 驱动逐步完成握手。
    /// 从这里可以看到， shuffled_pools 和 std::vector<ReplicaStatus> replicas; 的索引是一致的
    for (const auto & shuffled_pool : shuffled_pools)
        replicas.emplace_back(  // std::vector<ReplicaStatus> replicas;
            std::make_unique<ConnectionEstablisherAsync>(shuffled_pool.pool, &timeouts, settings_, log, table_to_check.get()));
}

HedgedConnectionsFactory::~HedgedConnectionsFactory()
{
    /// 析构前取消所有还在建立中的连接，避免连接处于半握手状态被下一个查询误用。
    /// 例如若还在等 TablesStatusResponse 就中断，否则下一个使用者会读到错误的协议包。
    stopChoosingReplicas();

    pool->updateSharedError(shuffled_pools);
}

/// 构造阶段入口：按 pool_mode 异步建立第一路连接，返回已就绪的连接指针列表。
/// distributed 查询通常用 GET_ONE，即每个 shard 先连一个 replica。
/// 这里调用waitForReadyConnectionsImpl 采用的是阻塞blocking=True，所以，HedgedConnection调用getManyConnections()以后，不需要再监听HedgedConnectionsFactory.fd
std::vector<Connection *> HedgedConnectionsFactory::getManyConnections(PoolMode pool_mode, AsyncCallback async_callback)
{
    size_t min_entries = skip_unavailable_shards ? 0 : 1;

    // 基于POOL_MODE设置min_entries和max_entries
    size_t max_entries = 1;
    switch (pool_mode)
    {
        case PoolMode::GET_ALL:
        {
            min_entries = shuffled_pools.size();
            max_entries = shuffled_pools.size();
            break;
        }
        case PoolMode::GET_ONE:
        {
            max_entries = 1;
            break;
        }
        case PoolMode::GET_MANY:
        {
            max_entries = std::min(max_parallel_replicas, shuffled_pools.size());
            break;
        }
    }

    std::vector<Connection *> connections;
    connections.reserve(max_entries);
    Connection * connection = nullptr;

    /// 先尝试启动 max_entries 条建连任务。
    for (size_t i = 0; i != max_entries; ++i)
    {
        ++requested_connections_count;
        State state = startNewConnectionImpl(connection); // 在一台机器上启动连接
        if (state == State::READY)
            connections.push_back(connection); // 选出一个，放到connections
        if (state == State::CANNOT_CHOOSE) // 还没凑够就遇到了CANNOT_CHOOSE，说明已经没有可用的replica可选了
            break;
    }

    /// 若还没凑够 max_entries 条就绪连接，阻塞处理 epoll 事件直到够数或确定失败。
    while (connections.size() < max_entries)
    {
        // 阻塞式获取对应的connection
        auto state = waitForReadyConnectionsImpl(/*blocking = */true, connection, async_callback);
        if (state == State::READY)
            connections.push_back(connection);
        else if (state == State::CANNOT_CHOOSE)
        {
            if (connections.size() >= min_entries)
                break; // 已经满足要求了

            if (!fallback_to_stale_replicas && up_to_date_count < min_entries)
                throw Exception(DB::ErrorCodes::ALL_REPLICAS_ARE_STALE,
                    "Could not find enough connections to up-to-date replicas. Got: {}, needed: {}",
                    connections.size(), min_entries);
            if (usable_count < min_entries)
                throw NetException(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
                    "All connection tries failed. Log: \n\n{}\n", fail_messages);

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown reason of not enough replicas.");
        }
    }

    return connections;
}

/// 只会发生在 receive_data_timeout， HedgedConnections::startNewReplica()中调用，建立一个新的连接，
/// 运行时入口：HedgedConnections 在 receive_data_timeout 超时后调用，尝试连下一个 replica。
HedgedConnectionsFactory::State HedgedConnectionsFactory::startNewConnection(Connection *& connection_out)
{
    ++requested_connections_count;
    State state = startNewConnectionImpl(connection_out);
    /// 当前选中的 replica 暂时连不上，但 epoll 里还有在建连接，则返回 NOT_READY 表示稍后再试。
    if (state == State::CANNOT_CHOOSE && !epoll.empty())
        state = State::NOT_READY;

    return state;
}

/// 非阻塞地处理 epoll 上已有的事件，看是否有新连接就绪。
/// 开启一个新的replica(blocking = false): HedgedConnections::checkNewReplica() -> hedged_connections_factory.waitForReadyConnections
HedgedConnectionsFactory::State HedgedConnectionsFactory::waitForReadyConnections(Connection *& connection_out)
{
    AsyncCallback async_callback = {};
    // 处理 epoll 事件；若没有足够新的 replica，在允许时可降级使用 stale replica。
    // 这里blocking=false，非阻塞
    return waitForReadyConnectionsImpl(false, connection_out, async_callback);
}

/// 处理 epoll 事件；若没有足够新的 replica，在允许时可降级使用 stale replica。
/// 无论是HedgedConnections 初始化的时候getManyConnections(blocking = true)，
///     还是开启一个新的replica(blocking = false): HedgedConnections::checkNewReplica() -> hedged_connections_factory.waitForReadyConnections
/// 都需要调用这个方法来检查状态
HedgedConnectionsFactory::State HedgedConnectionsFactory::waitForReadyConnectionsImpl(bool blocking, Connection *& connection_out, AsyncCallback & async_callback)
{
    State state = processEpollEvents(blocking, connection_out, async_callback);
    if (state != State::CANNOT_CHOOSE)
        return state;
    // state是 CANNOT_CHOOSE
    /// 没有空闲的 up-to-date replica 了。若配置允许，尝试挑一个可用的 stale replica。
    if (!fallback_to_stale_replicas) // 不允许使用stale_replica
        return State::CANNOT_CHOOSE;
    // 允许使用stale replica
    return setBestUsableReplica(connection_out);
}

/// 在打乱后的 replica 列表里，选出下一个还可以尝试建连的在shuffled_pools中的下标。
/// 已连上、正在连、或重试次数耗尽的 replica 会跳过。找不到则返回 -1。
int HedgedConnectionsFactory::getNextIndex()
{
    // entries_count是整个Shard Pool中已经成功的replica，往往一个Replica一个entry
    // replicas_in_process_count 是整个Shard Pool中正在进行连接的entry，往往一个Replica一个entry
    // failed_pools_count 是整个Shard Pool中已经失败的连接的entry，往往一个Replica一个entry
    if (entries_count + replicas_in_process_count + failed_pools_count >= shuffled_pools.size())
        return -1;

    if (last_used_index == -1)
    {
        last_used_index = 0;
        return 0;
    }

    bool finish = false;
    int next_index = last_used_index;
    while (!finish)
    {
        next_index = (next_index + 1) % shuffled_pools.size();
        // shuffled_pools[next_index].error_count是这个replica上的失败次数，次数太多就不再试了
        if (replicas[next_index].connection_establisher->getResult().entry.isNull()
            && (max_tries == 0 || shuffled_pools[next_index].error_count < max_tries))
            finish = true; // 选定了满足要求的一个
        else if (next_index == last_used_index)
            return -1;
    }

    last_used_index = next_index; // 记录下选定的Connection的index
    return next_index;
}

/// 选一个 replica 并开始或继续异步建连；若当前 replica 不可用则循环尝试下一个。
/// 选一个replica的操作既可能发生在没有hedge以前的正常初始化多个连接getManyConnetions以前，也可能发生在connection_timeout以后选择第二路建立一个连接
HedgedConnectionsFactory::State HedgedConnectionsFactory::startNewConnectionImpl(Connection *& connection_out)
{
    int index;
    State state;
    do
    {
        index = getNextIndex(); // 在打乱后的 replica 列表里，选出下一个还可以尝试建连的在 shuffled_pools，返回它的下表
        if (index == -1) // 无可用replica可选，这是彻底失败，而不是某个replica的单独的失败
            return State::CANNOT_CHOOSE;

        state = resumeConnectionEstablisher(index, connection_out);
    }
    while (state == State::CANNOT_CHOOSE); // 无限循环，持续等待，直到state不再是 CANNOT_CHOOSE(可以是NOT_READY或者是 READY)

    return state;
}

/// waitForReadyConnectionsImpl() -> processEpollEvents，而 waitForReadyConnectionsImpl在getManyConnections() 和
/// epoll 事件循环：处理 socket 就绪、hedged 连接超时、以及建连完成。
HedgedConnectionsFactory::State HedgedConnectionsFactory::processEpollEvents(bool blocking, Connection *& connection_out, AsyncCallback & async_callback)
{
    int event_fd;
    while (!epoll.empty()) // 该EPoll上是否注册有事件(不一定就绪)
    {
        event_fd = getReadyFileDescriptor(blocking, async_callback); //看看这个EPoll上是否有事件处于就绪状态

        if (event_fd == -1)
            return State::NOT_READY;

        if (fd_to_replica_index.contains(event_fd)) // 等到了连接建立的event
        {
            int index = fd_to_replica_index[event_fd]; // 从event_fd中获取对应的replica的index
            State state = resumeConnectionEstablisher(index, connection_out);
            if (state == State::NOT_READY) // 还不ready，那么需要继续等待
                continue;
            // 已经 ready/失败，无论如何，都可以结束监听了
            removeReplicaFromEpoll(index, event_fd);
            // 已经ready了，直接返回
            if (state == State::READY)
                return state;
            // 已经失败，继续监听
        }
        else if (timeout_fd_to_replica_index.contains(event_fd)) // 等到了timeout fd，显然，这里的 timeout是 hedged_connection_timeout
        {
            /// hedged_connection_timeout 到期：当前 replica 建连太慢，统计后尝试下一个 replica。
            int index = timeout_fd_to_replica_index[event_fd];
            replicas[index].change_replica_timeout.reset(); // 关闭这个timeoutstd::vector<ReplicaStatus> replicas;
            ++shuffled_pools[index].slowdown_count; // slowdown 计数器，添加到这个replica
            ProfileEvents::increment(ProfileEvents::HedgedRequestsChangeReplica);
            // 继续监听，因为我们关心的是 replica fd
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown event from epoll");

        /// 超时或某次建连失败后，立刻尝试启动到下一个 replica 的连接。
        if (startNewConnectionImpl(connection_out) == State::READY)
            return State::READY;
        // 还没有ready，继续循环
    }

    return State::CANNOT_CHOOSE; // epoll已经空了，都还没有成功建立链接，返回CANNOT_CHOOSE
}

/// 等待 epoll 上报有一个文件描述符就绪。blocking 为真时一直等；为假时只试一次。
int HedgedConnectionsFactory::getReadyFileDescriptor(bool blocking, AsyncCallback & async_callback)
{
    epoll_event event;
    event.data.fd = -1;
    if (!blocking)
    {
        epoll.getManyReady(1, &event, false);
        return event.data.fd;
    }

    size_t events_count = 0;
    while (events_count == 0)
    {
        events_count = epoll.getManyReady(1, &event, !static_cast<bool>(async_callback));
        if (!events_count && async_callback)
            async_callback(epoll.getFileDescriptor(), 0, AsyncEventTimeoutType::NONE, epoll.getDescription(), AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);
    }
    return event.data.fd; // 返回这个事件的fd
}

/// 推进某个 replica 的异步建连一步：可能立刻完成，也可能需要继续等 epoll。
HedgedConnectionsFactory::State HedgedConnectionsFactory::resumeConnectionEstablisher(int index, Connection *& connection_out)
{
    /// 若该 replica 之前失败过，本次强制重新建连。
    replicas[index].connection_establisher->resumeConnectionWithForceOption(/*force_connected_*/ shuffled_pools[index].error_count != 0);

    if (replicas[index].connection_establisher->isCancelled())
        return State::CANNOT_CHOOSE;

    if (replicas[index].connection_establisher->isFinished()) // 结束，或者失败，或者timeout
        return processFinishedConnection(index, replicas[index].connection_establisher->getResult(), connection_out);

    //既没有cancel，也没有finish，所以可能正在进行中, 获取这个connection_establisher对应的epoll的epoll_fd
    int fd = replicas[index].connection_establisher->getFileDescriptor();
    if (!fd_to_replica_index.contains(fd)) // 如果这个fd还没有加入到 监听队列中，那么就开始启动对他的监听
        addNewReplicaToEpoll(index, fd); // 进行异步监听, 这里的fd是 ConnectionEstablisherAsync的control fd，而不是某一个具体的timeout fd或者socket fd, 因为 Linux 允许把 epoll 的 control fd 再 epoll_ctl 到另一个 epoll 上。

    return State::NOT_READY;
}

/// 某 replica 建连结束后的结果处理：成功则返回 READY 和连接指针，失败则记录并尝试别的 replica。
/// 由 resumeConnectionEstablisher 在 connection_establisher->isFinished() 为 true 时调用。
HedgedConnectionsFactory::State HedgedConnectionsFactory::processFinishedConnection(int index, TryResult result, Connection *& connection_out)
{
    /// 收集本 replica 建连器产生的失败/警告信息，供上层汇总报错。
    const std::string & fail_message = replicas[index].connection_establisher->getFailMessage();
    if (!fail_message.empty())
        fail_messages += fail_message + "\n";

    /// 分支一：TCP/握手成功，从连接池借到了 Connection（entry 非空）。
    /// 还需逐层检查 usable / up_to_date / 聚合协议兼容，全部通过才 READY。
    if (!result.entry.isNull())
    {
        ++entries_count; /// 统计：本 Factory 生命周期内已拿到 entry 的次数（含最终未选中的）

        if (result.is_usable)
        {
            ++usable_count; /// 统计：远端可用（表存在、协议可通信等）
            if (result.is_up_to_date)
            {
                ++up_to_date_count; /// 统计：副本延迟在 max_replica_delay 允许范围内
                /// 若配置要求跳过不兼容 replica，则检查服务端 revision 是否支持两级聚合。
                if (!skip_replicas_with_two_level_aggregation_incompatibility || !isTwoLevelAggregationIncompatible(&*result.entry))
                {
                    replicas[index].is_ready = true;
                    ++ready_replicas_count;
                    connection_out = &*result.entry;
                    return State::READY; /// 唯一成功出口：连接可用且可被本次查询接受
                }
            }
        }
        /// entry 非空但未通过上述检查（缺表、stale、只读、聚合不兼容等）：
        /// 不增 error_count，落到函数末尾 return CANNOT_CHOOSE，由 startNewConnectionImpl 试下一个 replica。
    }
    else
    {
        /// 分支二：建连彻底失败，未借到 Connection（网络错误、超时、DNS 等）。
        /// 对该 replica 的 ShuffledPool 累加 error_count；耗尽 max_tries 则 failed_pools_count++。
        ShuffledPool & shuffled_pool = shuffled_pools[index];
        LOG_INFO(log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), fail_message);

        // 记录这个replica对应的shuffled_pool的error_count和slowdown_count
        shuffled_pool.error_count = std::min(pool->getMaxErrorCap(), shuffled_pool.error_count + 1);
        shuffled_pool.slowdown_count = 0; /// 硬失败与 hedge 慢路径分开计数，失败时清零 slowdown

        if (shuffled_pool.error_count >= max_tries)
        {
            ++failed_pools_count; /// Factory 级：又有一个 replica 在本轮被判定耗尽重试
            ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
        }
    }

    return State::CANNOT_CHOOSE; /// 未 READY：上层 startNewConnectionImpl 会循环 getNextIndex 试别的 replica
}

/// 取消所有尚未 READY 的建连任务，并从 epoll 中移除相关文件描述符。
void HedgedConnectionsFactory::stopChoosingReplicas()
{
    for (auto & [fd, index] : fd_to_replica_index) // 不再监听这个建立链接的fd
    {
        --replicas_in_process_count;
        epoll.remove(fd);
        replicas[index].connection_establisher->cancel();
    }

    for (auto & [timeout_fd, index] : timeout_fd_to_replica_index) // 不再监听这个timeout的fd
    {
        replicas[index].change_replica_timeout.reset();
        epoll.remove(timeout_fd);
    }

    fd_to_replica_index.clear(); //  清空连接fd -> index的映射
    timeout_fd_to_replica_index.clear(); //  清空timeout fd -> index的映射
}

/// 某个 索引为index的replica 开始异步建连后，把它的 socket 和 hedged 连接超时定时器注册到 epoll。
void HedgedConnectionsFactory::addNewReplicaToEpoll(int index, int fd)
{
    ++replicas_in_process_count;
    epoll.add(fd); // 向epoll中添加对应的文件描述符
    fd_to_replica_index[fd] = index;

    /// hedged_connection_timeout：若在此时间内 TCP 未建完，会触发换下一个 replica。默认是50ms
    replicas[index].change_replica_timeout.setRelative(timeouts.hedged_connection_timeout);
    // HedgedConnections 和 HedgedConntionsFactory 都有change_replica_timeout，但是他们的原因不同：
    //     HedgedConnections的change_replica_timeout指的是receive_data_timeout
    //     HedgedConntionsFactory 指的是 hedged_connection_timeout
    epoll.add(replicas[index].change_replica_timeout.getDescriptor()); // 注册这个 ReplicaStatus的 change_replica_timeout
    timeout_fd_to_replica_index[replicas[index].change_replica_timeout.getDescriptor()] = index; // 记录 change_replica_timeout 这个FD对应的replica index
}

/// 某个 replica 建连结束（成功或失败）后，从 epoll 移除其 socket 和定时器。
void HedgedConnectionsFactory::removeReplicaFromEpoll(int index, int fd)
{
    --replicas_in_process_count;
    epoll.remove(fd); // 删除连接建立的fd
    fd_to_replica_index.erase(fd);

    replicas[index].change_replica_timeout.reset();
    epoll.remove(replicas[index].change_replica_timeout.getDescriptor()); // 删除change_replica_timeout的fd
    timeout_fd_to_replica_index.erase(replicas[index].change_replica_timeout.getDescriptor());
}

/// 仍在建立中的连接数量，供 HedgedConnections 判断是否需要监听 Factory 的 epoll。
size_t HedgedConnectionsFactory::numberOfProcessingReplicas() const
{
    if (epoll.empty())
        return 0;

    return requested_connections_count - ready_replicas_count;
}

/// 在允许使用 stale replica 时，从已连上但未选中的 replica 里挑延迟最小的一个。
HedgedConnectionsFactory::State HedgedConnectionsFactory::setBestUsableReplica(Connection *& connection_out)
{
    std::vector<int> indexes;
    for (size_t i = 0; i != replicas.size(); ++i)
    {
        TryResult result = replicas[i].connection_establisher->getResult();
        if (!result.entry.isNull()
            && result.is_usable
            && !replicas[i].is_ready
            && (!skip_replicas_with_two_level_aggregation_incompatibility || !isTwoLevelAggregationIncompatible(&*result.entry)))
            indexes.push_back(static_cast<int>(i));
    }

    if (indexes.empty())
        return State::CANNOT_CHOOSE;

    std::stable_sort(
        indexes.begin(),
        indexes.end(),
        [&](size_t lhs, size_t rhs)
        {
            return replicas[lhs].connection_establisher->getResult().delay < replicas[rhs].connection_establisher->getResult().delay;
        });

    replicas[indexes[0]].is_ready = true;
    TryResult result = replicas[indexes[0]].connection_establisher->getResult();
    connection_out = &*result.entry;
    return State::READY;
}

/// 检查 remote 服务端版本是否支持当前查询需要的两级聚合协议。
bool HedgedConnectionsFactory::isTwoLevelAggregationIncompatible(Connection * connection)
{
    return connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD;
}

}
#endif
