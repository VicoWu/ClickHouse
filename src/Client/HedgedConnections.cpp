#include <Core/Protocol.h>
#if defined(OS_LINUX)

#include <Client/HedgedConnections.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event HedgedRequestsChangeReplica;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_changing_replica_until_first_data_packet;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 connections_with_failover_max_tries;
    extern const SettingsDialect dialect;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsBool skip_unavailable_shards;
}

namespace ErrorCodes
{
    extern const int MISMATCH_REPLICAS_DATA_SOURCES;
    extern const int LOGICAL_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NOT_IMPLEMENTED;
}

/// 构造本 shard 的 hedged 连接组。
/// 一个 HedgedConnections 对象只对应 distributed 查询里的一个 shard（pool_ 是该 shard 下所有 replica 的 failover 池）。
/// 构造结束时：对每个 parallel_replica_offset 先建好「第一路」TCP 连接，并在本对象 epoll 上挂好收包与收包 hedge 定时器；
/// 若之后 receive_data_timeout 到期，再通过 hedged_connections_factory 异步建「第二路」replica，append 到同一 offset 的 replicas 列表。
HedgedConnections::HedgedConnections(
    const ConnectionPoolWithFailoverPtr & pool_,
    ContextPtr context_,
    // ConnectionTimeouts 不是 HedgedConnections 里 new 出来的，
    // 而是在 RemoteQueryExecutor 的 lambda 里按当前查询 Settings 现场组出来，再传进构造函数；HedgedConnections 自己不存一份，主要交给 hedged_connections_factory 保存
    const ConnectionTimeouts & timeouts_,
    const ThrottlerPtr & throttler_,
    PoolMode pool_mode,
    std::shared_ptr<QualifiedTableName> table_to_check_,
    AsyncCallback async_callback,
    GetPriorityForLoadBalancing::Func priority_func)
    /// hedged_connections_factory：在本 shard 内按负载均衡打乱 replica 顺序，用 epoll 异步完成 TCP 建连（含 50ms hedged 换 replica）。
    : hedged_connections_factory(
          pool_,
          context_->getSettingsRef(),
          timeouts_,
          context_->getSettingsRef()[Setting::connections_with_failover_max_tries].value,
          context_->getSettingsRef()[Setting::fallback_to_stale_replicas_for_distributed_queries].value,
          context_->getSettingsRef()[Setting::max_parallel_replicas].value,
          context_->getSettingsRef()[Setting::skip_unavailable_shards].value,
          table_to_check_,
          priority_func)
    , context(std::move(context_))
    , settings(context->getSettingsRef())
    , throttler(throttler_)
{
    /// getManyConnections：按 pool_mode 从本 shard 取已就绪连接。
    /// GET_ONE 时通常只有 1 条——本 shard 上第一个选中的 replica；
    /// GET_MANY 时最多 max_parallel_replicas 条——同一 shard 上 parallel_replica_offset 0、1、… 各一路。
    /// connections[k] 是已建好的 Connection*，指向远程某一 replica 节点上的 TCP 连接（尚未 sendQuery，只完成握手）。
    // getManyConnections 返回时，第一路建连已经在 Factory 内部「等完了」，Factory 的 epoll 通常是空的，没有东西需要 HedgedConnection 再监听。
    // blocking=true
    std::vector<Connection *> connections = hedged_connections_factory.getManyConnections(pool_mode, std::move(async_callback));

    if (connections.empty())
        return;

    /// offset_states：本 shard 内按 parallel_replica_offset 分组的状态表。
    /// 下标 i 与 settings parallel_replica_offset=i 对应；分布式里一个 shard 可能并行读多路，每路一个 OffsetState。
    // 如果没有开parallel replica， 那么一个shard只有一个connection
    offset_states.reserve(connections.size());
    for (size_t i = 0; i != connections.size(); ++i)
    {
        offset_states.emplace_back();

        /// offset_states[i]：负责 parallel_replica_offset = i 的这一路查询。
        /// 此时只有第一路 replica：replicas[0]；若收包超时触发 hedge，后续 replicas[1]、replicas[2]… 会 emplace_back 进来。
        offset_states[i].replicas.emplace_back(connections[i]);
        offset_states[i].active_connection_count = 1;

        /// ReplicaState：这一条远程连接的运行时状态——Connection、异步收包器 PacketReceiver、以及 2s 收包 hedge 用的 timerfd。
        ReplicaState & replica = offset_states[i].replicas.back();
        replica.connection->setThrottler(throttler_);

        /// epoll 注册（一）PacketReceiver 内层 epoll 的 control fd（不是 socket 本身）。
        /// 该 replica 的收包 fiber 在 socket 阻塞时挂起；socket 有数据后内层 epoll 就绪，外层 epoll_wait 返回此 fd，再 resumePacketReceiver。
        epoll.add(replica.packet_receiver->getFileDescriptor());
        fd_to_replica_location[replica.packet_receiver->getFileDescriptor()] = ReplicaLocation{i, 0};

        /// epoll 注册（二）change_replica_timeout 的 timerfd（receive_data_timeout，如 2s）
        /// 这里用timeout_fd_to_replica_location 保存了这个FD对应的ReplicaLocation，因此如果收到了timeout，就可以清楚知道这个Replica的身份
        /// 构造时只 add fd；sendQuery 后才 setRelative 启动计时。到期且仍无有效 Progress 时 startNewReplica 建 hedged 第二路。
        /// 与 Factory 里 50ms 建连用的 change_replica_timeout 不是同一个对象
        epoll.add(replica.change_replica_timeout.getDescriptor());
        timeout_fd_to_replica_location[replica.change_replica_timeout.getDescriptor()] = ReplicaLocation{i, 0};
    }

    /// 本 shard 当前活跃连接总数（各 offset 的 active_connection_count 之和，构造后等于 connections.size()）。
    active_connection_count = connections.size();

    /// 晚到的 hedged 连接在 processNewReplicaState 里也会走 pipeline，保证与第一路一样挂上 throttler。
    pipeline_for_new_replicas.add([throttler_](ReplicaState & replica_) { replica_.connection->setThrottler(throttler_); });
}

void HedgedConnections::Pipeline::add(std::function<void(ReplicaState & replica)> send_function)
{
    pipeline.push_back(send_function);
}

void HedgedConnections::Pipeline::run(ReplicaState & replica)
{
    for (auto & send_func : pipeline)
        send_func(replica);
}

void HedgedConnections::sendScalarsData(Scalars & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send scalars data: query not yet sent.");

    auto send_scalars_data = [&data](ReplicaState & replica) { replica.connection->sendScalarsData(data); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_scalars_data(replica);

    pipeline_for_new_replicas.add(send_scalars_data);
}

void HedgedConnections::sendQueryPlan(const QueryPlan & query_plan)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send query plan: query not yet sent.");

    auto send_query_plan = [&query_plan](ReplicaState & replica) { replica.connection->sendQueryPlan(query_plan); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_query_plan(replica);

    pipeline_for_new_replicas.add(send_query_plan);
}

void HedgedConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send external tables data: query not yet sent.");

    if (data.size() != size())
        throw Exception(ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES, "Mismatch between replicas and data sources");

    auto send_external_tables_data = [&](ReplicaState & replica)
    {
        size_t offset = fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset;
        replica.connection->sendExternalTablesData(data[offset]);
    };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_external_tables_data(replica);

    pipeline_for_new_replicas.add(send_external_tables_data);
}

void HedgedConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot send uuids after query is sent.");

    auto send_ignored_part_uuids = [&uuids](ReplicaState & replica) { replica.connection->sendIgnoredPartUUIDs(uuids); };

    for (auto & offset_state : offset_states)
        for (auto & replica : offset_state.replicas)
            if (replica.connection)
                send_ignored_part_uuids(replica);

    pipeline_for_new_replicas.add(send_ignored_part_uuids);
}

/**
 * 发送了query以后，会立刻设置 change_replica_timeout 的 timeout， 同时设置packet_receiver的timeout
 */
void HedgedConnections::sendQuery(
    const ConnectionTimeouts & timeouts,
    const String & query,
    const String & query_id,
    UInt64 stage,
    ClientInfo & client_info,
    bool with_pending_data,
    const std::vector<String> & external_roles)
{
    std::lock_guard lock(cancel_mutex);

    if (sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query already sent.");

    for (auto & offset_state : offset_states)
    {
        for (auto & replica : offset_state.replicas)
        {
            if (replica.connection->getServerRevision(timeouts) < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
            {
                disable_two_level_aggregation = true;
                break;
            }
        }
        if (disable_two_level_aggregation)
            break;
    }

    if (!disable_two_level_aggregation)
    {
        /// Tell hedged_connections_factory to skip replicas that doesn't support two-level aggregation.
        hedged_connections_factory.skipReplicasWithTwoLevelAggregationIncompatibility();
    }

    auto send_query = [this, timeouts, query, query_id, stage, client_info, with_pending_data, external_roles](ReplicaState & replica)
    {
        Settings modified_settings = settings;

        /// Queries in foreign languages are transformed to ClickHouse-SQL. Ensure the setting before sending.
        modified_settings[Setting::dialect] = Dialect::clickhouse;
        modified_settings[Setting::dialect].changed = false;

        if (disable_two_level_aggregation)
        {
            /// Disable two-level aggregation due to version incompatibility.
            modified_settings[Setting::group_by_two_level_threshold] = 0;
            modified_settings[Setting::group_by_two_level_threshold_bytes] = 0;
        }

        const bool enable_offset_parallel_processing = context->canUseOffsetParallelReplicas();

        if (offset_states.size() > 1 && enable_offset_parallel_processing)
        {
            modified_settings[Setting::parallel_replicas_count] = offset_states.size();
            modified_settings[Setting::parallel_replica_offset] = fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset;
        }

        /// FIXME: Remove once we will make `allow_experimental_analyzer` obsolete setting.
        /// Make the analyzer being set, so it will be effectively applied on the remote server.
        /// In other words, the initiator always controls whether the analyzer enabled or not for
        /// all servers involved in the distributed query processing.
        modified_settings.set("allow_experimental_analyzer", static_cast<bool>(modified_settings[Setting::allow_experimental_analyzer]));

        replica.connection->sendQuery(
            timeouts, query, /* query_parameters */ {}, query_id, stage, &modified_settings, &client_info, with_pending_data, external_roles, {});
        /**
         * 这个timeout在sendQuery以后设置
         */
        replica.change_replica_timeout.setRelative(timeouts.receive_data_timeout);
        replica.packet_receiver->setTimeout(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
    };

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            send_query(replica);

    pipeline_for_new_replicas.add(send_query);
    sent_query = true;
}

void HedgedConnections::disconnect()
{
    std::lock_guard lock(cancel_mutex);

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            if (replica.connection)
                finishProcessReplica(replica, true);

    if (hedged_connections_factory.hasEventsInProcess())
    {
        if (hedged_connections_factory.numberOfProcessingReplicas() > 0)
            epoll.remove(hedged_connections_factory.getFileDescriptor());

        hedged_connections_factory.stopChoosingReplicas();
    }
}

std::string HedgedConnections::dumpAddresses() const
{
    std::lock_guard lock(cancel_mutex);

    std::string addresses;
    bool is_first = true;

    for (const auto & offset_state : offset_states)
    {
        for (const auto & replica : offset_state.replicas)
        {
            if (replica.connection)
            {
                addresses += (is_first ? "" : "; ") + replica.connection->getDescription();
                is_first = false;
            }
        }
    }

    return addresses;
}

void HedgedConnections::sendCancel()
{
    std::lock_guard lock(cancel_mutex);

    if (!sent_query || cancelled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot cancel. Either no query sent or already cancelled.");

    /// All hedged connections should be stopped, since otherwise before the
    /// HedgedConnectionsFactory will be destroyed (that will happen from
    /// QueryPipeline dtor) they could still do some work.
    /// And not only this does not make sense, but it also could lead to
    /// use-after-free of the current_thread, since the thread from which they
    /// had been created differs from the thread where the dtor of
    /// QueryPipeline will be called and the initial thread could be already
    /// destroyed (especially when the system is under pressure).
    if (hedged_connections_factory.hasEventsInProcess())
        hedged_connections_factory.stopChoosingReplicas();

    cancelled = true;

    for (auto & offset_status : offset_states)
        for (auto & replica : offset_status.replicas)
            if (replica.connection)
                replica.connection->sendCancel();
}

Packet HedgedConnections::drain()
{
    std::lock_guard lock(cancel_mutex);

    if (!cancelled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot drain connections: cancel first.");

    Packet res;
    res.type = Protocol::Server::EndOfStream;

    while (!epoll.empty())
    {
        ReplicaLocation location = getReadyReplicaLocation();
        Packet packet = receivePacketFromReplica(location);
        switch (packet.type)
        {
            case Protocol::Server::PartUUIDs:
            case Protocol::Server::Data:
            case Protocol::Server::Progress:
            case Protocol::Server::ProfileInfo:
            case Protocol::Server::Totals:
            case Protocol::Server::Extremes:
            case Protocol::Server::EndOfStream:
                break;

            case Protocol::Server::Exception:
            default:
                /// If we receive an exception or an unknown packet, we save it.
                res = std::move(packet);
                break;
        }
    }

    return res;
}

/**
 * 重写了IConnections::receivePacket
 */
Packet HedgedConnections::receivePacket()
{
    std::lock_guard lock(cancel_mutex);
    return receivePacketUnlocked({});
}

UInt64 HedgedConnections::receivePacketTypeUnlocked(AsyncCallback)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'receivePacketTypeUnlocked()' not implemented for HedgedConnections");
}

/**
 * 等到「有一个 replica 的完整 packet 可读」，返回其 ReplicaLocation，然后从这个ReplicaLocation来接收packet
 */
Packet HedgedConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    if (!sent_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot receive packets: no query sent.");
    if (!hasActiveConnections())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No more packets are available.");

    if (epoll.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No pending events in epoll.");
    // 等到「有一个 replica 的完整 packet 可读」，返回其 ReplicaLocation，然后从这个ReplicaLocation上读取packet
    ReplicaLocation location = getReadyReplicaLocation(std::move(async_callback));
    // 从这个location接受Packet
    return receivePacketFromReplica(location);
}

/// 在 epoll 上等到「有一个 replica 的完整 packet 可读」，返回其 ReplicaLocation
/// 返回值供 receivePacketFromReplica() 使用；本函数本身不返回 Packet。
/// ReplicaLocation { offset, index }：
///   offset — shard / parallel_replica_offset（哪条赛道）
///   index  — 该 offset 下第几条 hedged 连接（0=第一路，1=hedged 第二路…）
HedgedConnections::ReplicaLocation HedgedConnections::getReadyReplicaLocation(AsyncCallback async_callback)
{
    /// 快捷路径 — 上一轮 receivePacketFromReplica 标记的 replica 若 socket 上还有未读数据，先继续读
    if (replica_with_last_received_packet)
    {
        ReplicaLocation location = replica_with_last_received_packet.value();
        replica_with_last_received_packet.reset();
        if (offset_states[location.offset].replicas[location.index].connection->hasReadPendingData() && resumePacketReceiver(location))
            return location; // 如果还有数据，那么继续读
        // 虽然上一轮 receivePacketFromReplica 标记的有replica，但是没有未读数据，不继续了
    }

    /// 主循环：在 epoll 上等到某个 replica 读满一个完整 packet，这时候完全有可能收到其他的fd，比如，居然收到了timeout，这时候就可以开始另外一路了
    int event_fd;
    while (true)
    {
        /// 阻塞或非阻塞等待 epoll 上任意 fd 就绪（socket / hedged timer / factory）
        event_fd = getReadyFileDescriptor(async_callback);

        /// Factory epoll — 异步建连完成，把新 replica 加入 offset_states 并补发 sendQuery
        if (event_fd == hedged_connections_factory.getFileDescriptor())
            checkNewReplica(); // 继续循环等待包，因为 HedgedConnections::getReadyReplicaLocation的目的是等待有包的ReplicaLocation
        /// 某 replica 的 socket 可读 — 尝试 resume 读包；读满则 return location
        else if (fd_to_replica_location.contains(event_fd))
        {
            ReplicaLocation location = fd_to_replica_location[event_fd];
            if (resumePacketReceiver(location)) // 读满一个包，退出循环，没有读满，继续读
                return location;
        }
        /// sendQuery 之后设置了setRelative，然后发生了timeout，就设置is_change_replica_timeout_expired = true;
        // 在还能换 replica 期间，长时间没有有效 Progress」 的 timer。
        /// 分支 C：receive_data_timeout 到期 — 2s 内无有效 Progress，启动 hedged 第二路
        /// 不是PacketReceiver的receive_timeout
        else if (timeout_fd_to_replica_location.contains(event_fd))
        {
            ReplicaLocation location = timeout_fd_to_replica_location[event_fd];
            offset_states[location.offset].replicas[location.index].change_replica_timeout.reset();
            offset_states[location.offset].replicas[location.index].is_change_replica_timeout_expired = true;
            offset_states[location.offset].next_replica_in_process = true; // 有第二路连接
            offsets_queue.push(static_cast<int>(location.offset));
            ProfileEvents::increment(ProfileEvents::HedgedRequestsChangeReplica);
            startNewReplica(); // 建立第二路
            /// 不 return：继续循环，等 socket 或新 replica 就绪
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown event from epoll");
    }
}

/// 推进 location 所指 replica 的收包 fiber 一步，并判断是否已经读出「一个完整协议包」。
///
/// 由 getReadyReplicaLocation 在 HedgedConnection epoll 上报该 replica 的 PacketReceiver 内层 epoll_fd 就绪时调用。
/// location.offset = parallel_replica_offset；location.index = 该 offset 下第几路 hedged 连接（0 第一路，1 第二路…）。
///
/// 返回值：
///   true  — 已读出完整 packet，存入 last_received_packet，调用方应接着 receivePacketFromReplica(location)；
///   false — 尚未读出完整包（fiber 再次挂起等 socket），或本步已结束该 replica（超时/异常路径里可能抛错）。
bool HedgedConnections::resumePacketReceiver(const HedgedConnections::ReplicaLocation & location)
{
    ReplicaState & replica_state = offset_states[location.offset].replicas[location.index];

    /// 继续 PacketReceiver 的 fiber：内部调用 Connection::receivePacket()；若阻塞则挂起并靠 PacketReceiver 内层 epoll 再等。
    replica_state.packet_receiver->resume();

    if (replica_state.packet_receiver->isPacketReady())
    {
        /// 完整包已就绪（!is_read_in_process）：刷新 socket 读超时，供下一次 resume 使用（ConnectionTimeouts::receive_timeout，非 2s hedge）。
        replica_state.packet_receiver->setTimeout(hedged_connections_factory.getConnectionTimeouts().receive_timeout);
        last_received_packet = replica_state.packet_receiver->getPacket(); // 这是HedgedConnections的成员变量
        return true;
    }

    if (replica_state.packet_receiver->isTimeoutExpired())
    {

        /// PacketReceiver 内层 epoll：receive_timeout 到期且 socket 仍无数据（与 change_replica_timeout 的 2s hedge 不同）
        /// 于是去掉这一路的replica
        const String & description = replica_state.connection->getDescription();
        // finishProcessReplica 之后判断的，已经把「当前这一路」从计数里减掉了。
        finishProcessReplica(replica_state, true);

        /// 该 offset 已无任何活跃连接，且也没有正在建的 hedged 第二路 → 向上抛 SOCKET_TIMEOUT
        /// 若第一路超时但 next_replica_in_process 或 replicas[1] 仍在，则不抛，由 hedge 或另一路继续。
        if (offset_states[location.offset].active_connection_count == 0 && !offset_states[location.offset].next_replica_in_process)
            throw NetException(
                ErrorCodes::SOCKET_TIMEOUT,
                "Timeout exceeded while reading from socket ({}, receive timeout {} ms)",
                description,
                replica_state.packet_receiver->getTimeout().totalMilliseconds());
    }
    else if (replica_state.packet_receiver->hasException())
    {
        /// fiber 内 receivePacket 抛错：摘掉该 replica，异常继续向上抛。
        finishProcessReplica(replica_state, true);
        std::rethrow_exception(replica_state.packet_receiver->getException());
    }

    /// fiber 挂起、包未读全：getReadyReplicaLocation 继续 epoll_wait 其他 fd 或同一 replica 下次就绪。
    return false;
}

/// 等待 epoll 上报「有一个文件描述符已经就绪」，并返回这个描述符的编号。
/// 就绪的可能包括：某个 replica 的 socket、hedged 超时定时器、或 Factory 建连完成通知。
int HedgedConnections::getReadyFileDescriptor(AsyncCallback async_callback)
{
    epoll_event event;
    event.data.fd = -1;
    size_t events_count = 0;

    /// 如果没有传入 async_callback，就在这里一直等到有事件（同步读路径）。
    /// 如果传了 async_callback，则只试一次；没有事件就交给上层异步调度，稍后再来。
    bool wait_until_event = !static_cast<bool>(async_callback);

    while (events_count == 0)
    {
        /// 从 epoll 取最多 1 个就绪事件。wait_until_event 为真时无限等待，为假时立即返回。
        events_count = epoll.getManyReady(1, &event, wait_until_event ? -1 : 0);

        /// 异步路径：当前没有任何就绪事件，通知上层「请先监听 epoll 这个 fd，就绪后再继续读」。
        if (!events_count && async_callback)
            async_callback(epoll.getFileDescriptor(), 0, AsyncEventTimeoutType::NONE, epoll.getDescription(), AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);
    }

    /// 返回就绪的文件描述符，由 getReadyReplicaLocation 判断它是 socket、定时器还是 Factory。
    return event.data.fd;
}

/// 从指定 replica 取走已就绪的数据包，并根据包类型更新 hedged 状态（是否还能换 replica、谁是赢家）。
/// offset = shard/并行 offset；index = 同一 offset 下第几条 hedged 连接（0=第一路，1=hedged 第二路，…）。
Packet HedgedConnections::receivePacketFromReplica(const ReplicaLocation & replica_location)
{
    ReplicaState & replica = offset_states[replica_location.offset].replicas[replica_location.index];
    Packet packet = std::move(last_received_packet);

    switch (packet.type)
    {
        // ── 段 1：Data — 非空结果块，在 allow_changing=true 时才是「定赢家」信号 ──
        case Protocol::Server::Data:
            /// If we received the first not empty data packet and still can change replica,
            /// disable changing replica with this offset.
            /// 收到第一个 rows>0 的 Data：锁定该 replica 为赢家，cancel 同 offset 的其他连接。
            if (offset_states[replica_location.offset].can_change_replica && packet.block.rows() > 0)
                disableChangingReplica(replica_location); // 只要收到了data packet，就定胜负了
            //如果 can_change_replica = false，说明已经hedge过了(can_change_replica默认是true)，因此不需要disableChangingReplica
            replica_with_last_received_packet = replica_location;
            break;

        // ── 段 2：Progress — hedged 定赢家 / 续期 timer 的核心逻辑 ──
        case Protocol::Server::Progress:
            /// Check if we have made some progress and still can change replica.
            /// 仅 progress.read_bytes>0 才算「有效 progress」（remote 已从源表读出字节），当前还没有定胜负
            if (offset_states[replica_location.offset].can_change_replica && packet.progress.read_bytes > 0)
            {
                /// If we are allowed to change replica until the first data packet,
                /// just restart timeout (if it hasn't expired yet). Otherwise disable changing replica with this offset.
                /// 若 allow_changing=true，可在第一个 Data 之前，只要is_change_replica_timeout_expired还没有被置位，那么就反复 setRelative进行重置。
                if (settings[Setting::allow_changing_replica_until_first_data_packet] && !replica.is_change_replica_timeout_expired)
                    /// allow_changing=true：progress 只表示「还在读」，重置 receive_data_timeout，仍可能再 hedge
                    /// 因此：只要每隔不到 2s 就有一次 read_bytes > 0 的 Progress，就不会走 timeout_fd_to_replica_location → startNewReplica()。
                    replica.change_replica_timeout.setRelative(hedged_connections_factory.getConnectionTimeouts().receive_data_timeout);
                else
                    /// allow_changing=false（默认）：第一个 positive progress 即定赢家，不再换 replica。
                    disableChangingReplica(replica_location);
            }
            /// read_bytes=0 的 Progress 不改变 can_change_replica，hedged timer 继续走。
            replica_with_last_received_packet = replica_location;
            break;

        // ── 段 3：元数据/辅助包 — 不参与 hedged 决策，只记录来源 replica ──
        case Protocol::Server::TimezoneUpdate:
        case Protocol::Server::PartUUIDs:
        case Protocol::Server::ProfileInfo:
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
        case Protocol::Server::ProfileEvents:
            replica_with_last_received_packet = replica_location;
            break;

        // ── 段 4：EndOfStream — 无数据可查的空结果，结束该 replica ──
        case Protocol::Server::EndOfStream:
            /// Check case when we receive EndOfStream before first not empty data packet
            /// or positive progress. It may happen if max_parallel_replicas > 1 and
            /// there is no way to sample data in this query.
            /// 在尚未定赢家时收到 EOS：锁定当前 replica 并正常结束该连接。
            if (offset_states[replica_location.offset].can_change_replica)
                disableChangingReplica(replica_location);
            finishProcessReplica(replica, false);
            break;

        // ── 段 5：Exception / 未知包 — 出错结束该 replica ──
        case Protocol::Server::Exception:
        default:
            /// Check case when we receive Exception before first not empty data packet
            /// or positive progress. It may happen if max_parallel_replicas > 1 and
            /// there is no way to sample data in this query.
            /// 异常或未识别包：若还能换 replica 则锁定当前路，并以 error 结束该连接。
            if (offset_states[replica_location.offset].can_change_replica)
                disableChangingReplica(replica_location);
            finishProcessReplica(replica, true);
            break;
    }

    return packet;
}

/// 为某个 shard 选定 replica 赢家：关闭该 shard 上其余 hedged 连接，并不再尝试换 replica。
void HedgedConnections::disableChangingReplica(const ReplicaLocation & replica_location)
{
    OffsetState & offset_state = offset_states[replica_location.offset];

    /// 赢家这条连接的「等待数据超时」定时器可以关掉了(是关闭计时，而不是重新开始计时)，已经不需要再 hedge了
    offset_state.replicas[replica_location.index].change_replica_timeout.reset();
    ++offsets_with_disabled_changing_replica; // 整个Shard(HedgedConnections的offsets_with_disabled_changing_replica计数器++)
    /// 这个 Offset 上已经不能再换 replica 了
    offset_state.can_change_replica = false;

    /// 当前的OffsetState(同 shard、同offset)中，除了当前赢家ReplicaLocation， 以外的其他连接，全部发 cancel 并清理掉。
    for (size_t i = 0; i != offset_state.replicas.size(); ++i)
    {
        // 把不是当前ReplicaState的其他所有的ReplicaState，全部cancel并清理掉
        if (i != replica_location.index && offset_state.replicas[i].connection)
        {
            offset_state.replicas[i].connection->sendCancel();
            finishProcessReplica(offset_state.replicas[i], true);
        }
    }

    /// 如果当前Shard的Factory的EPoll上还有注册的事件， 并且当前已经被disable掉的offset 已经等于 offset_states中的OffsetState的数量(全部的OffsetState都已经走了disableChangingReplica)
    /// 那么根本不可能再有新的Hedge了，因此，可以把HedgedConnectionsFactory的epoll总HedgedConnections的epoll中删除了
    if (hedged_connections_factory.hasEventsInProcess() && offsets_with_disabled_changing_replica == offset_states.size())
    {
        if (hedged_connections_factory.numberOfProcessingReplicas() > 0)
            epoll.remove(hedged_connections_factory.getFileDescriptor()); // 把建连的event也删掉
        hedged_connections_factory.stopChoosingReplicas(); // 取消HedgedConnectionsFactory中所有尚未 READY 的建连任务，并从 epoll 中移除相关文件描述符。
    }
}

/// 这个方法只有在收到 receive_data_timeout 后：向 Factory 申请连下一个 replica，并交给 processNewReplicaState 处理结果。
void HedgedConnections::startNewReplica()
{
    Connection * connection = nullptr;
    //   建立一个新的连接，这个发生在timeout以后。
    // 这个方法返回一个 HedgedConnectionsFactory::State state，后续要做的就是让epoll监听 hedged_connections_factory.getFileDescriptor()
    HedgedConnectionsFactory::State state = hedged_connections_factory.startNewConnection(connection);

    /// 建连还在进行中，且这是 Factory 里唯一一个在连的连接时，需要监听 Factory 的文件描述符，等它连好。
    if (state == HedgedConnectionsFactory::State::NOT_READY && hedged_connections_factory.numberOfProcessingReplicas() == 1)
        epoll.add(hedged_connections_factory.getFileDescriptor()); // 监听这个HedgedConnectionsFactory对应的control fd

    processNewReplicaState(state, connection);
}

/// 在 HedgedConnections::startNewReplica() 中注册了对 hedged_connections_factory.getFileDescriptor()的监听，
// 这个方法就是在监听到hedged_connections_factory.getFileDescriptor() ready以后，进行处理
/// Factory 的文件描述符就绪时调用：看看异步建连是否完成，并把新 replica 接进 offset_states。
void HedgedConnections::checkNewReplica()
{
    Connection * connection = nullptr;
    // 获取一个ready的connection， non-blocking mode
    HedgedConnectionsFactory::State state = hedged_connections_factory.waitForReadyConnections(connection);

    if (cancelled)
    {
        /// 查询已经取消，即使连上了也不要再用，直接断开
        if (connection)
            connection->disconnect();

        state = HedgedConnectionsFactory::State::CANNOT_CHOOSE;
    }

    processNewReplicaState(state, connection);

    /// Factory 里已经没有正在建立的连接了，就不再监听它的文件描述符了
    if (hedged_connections_factory.numberOfProcessingReplicas() == 0)
        epoll.remove(hedged_connections_factory.getFileDescriptor());
}

/// 处理 hedged 第二路建连的结果（由 startNewReplica 或 checkNewReplica 在拿到 Factory::State 后调用）。
/// 本对象只对应一个 shard；offsets_queue 里存的是 parallel_replica_offset（哪一路并行读在等第二路 replica）。
void HedgedConnections::processNewReplicaState(HedgedConnectionsFactory::State state, Connection * connection)
{
    switch (state)
    {
        case HedgedConnectionsFactory::State::READY: // 第二路replica完全准备好了
        {
            /// getReadyReplicaLocation 在 2s timer 到期时把 offset 入队；此处假定队非空且与本次 READY 的 connection 对应。
            size_t offset = offsets_queue.front();
            offsets_queue.pop();

            /// 新建 ReplicaState（内含 Connection* 与 PacketReceiver）；挂在 replicas 末尾，index 一般为 1（第二路 hedge）。
            /// 第一路 replicas[0] 仍在 epoll 上并行收包，直到 disableChangingReplica 定赢家。
            offset_states[offset].replicas.emplace_back(connection);
            ++offset_states[offset].active_connection_count; // 当前这个offset的 active connection 的数量加1
            offset_states[offset].next_replica_in_process = false; // 目前没有正在进行的replica
            ++active_connection_count; // 当前这个shard的所有的active connection 的数量，如果是parallel replica，则是所有的offset的active_connection_count的和

            ReplicaState & replica = offset_states[offset].replicas.back();
            const size_t index = offset_states[offset].replicas.size() - 1;

            /// 在 HedgedConnection 层 epoll 注册第二路：PacketReceiver 内层 epoll_fd（嵌套，就绪后 resumePacketReceiver）。
            epoll.add(replica.packet_receiver->getFileDescriptor());
            fd_to_replica_location[replica.packet_receiver->getFileDescriptor()] = ReplicaLocation{offset, index};

            /// 注册 receive_data_timeout 的 timerfd（fd 先挂上；pipeline 里 sendQuery 会 setRelative 启动计时）。
            epoll.add(replica.change_replica_timeout.getDescriptor());
            timeout_fd_to_replica_location[replica.change_replica_timeout.getDescriptor()] = ReplicaLocation{offset, index};

            /// 对晚到的 replica 重放构造后累积的 pipeline（至少含重新sendQuery()、重新setThrottler），与第一路查询一致。
            pipeline_for_new_replicas.run(replica);
            break;
        }
        case HedgedConnectionsFactory::State::CANNOT_CHOOSE:
        {
            /// Factory 已无法为本 shard 再提供新连接（各 replica 用尽或均失败）。
            /// 清空 offsets_queue 中因 2s 超时而入队的 offset，并清除 next_replica_in_process。
            /// 若某个 offset 上已无任何活跃连接（第一路也挂了），则整个 hedged 组失败。
            while (!offsets_queue.empty())
            {
                const size_t offset = static_cast<size_t>(offsets_queue.front());
                if (offset_states[offset].active_connection_count == 0)
                    throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot find enough connections to replicas");
                offset_states[offset].next_replica_in_process = false;
                offsets_queue.pop();
            }
            break;
        }
        case HedgedConnectionsFactory::State::NOT_READY:
            /// startNewConnection 已启动建连 fiber 并登记 Factory epoll，但 TCP 尚未就绪；connection 无效。
            /// 保持 offsets_queue 与 next_replica_in_process，待 checkNewReplica 再次进入本函数。
            break;
    }
}

/// 结束对某条 replica 连接的使用：从 epoll 摘掉、取消读包、可选断开 TCP，并更新计数。
void HedgedConnections::finishProcessReplica(ReplicaState & replica, bool disconnect)
{
    /// 必须先于 cancel 从 epoll 移除 socket，否则别的线程可能再次用这个描述符去读已取消的连接。
    epoll.remove(replica.packet_receiver->getFileDescriptor());
    epoll.remove(replica.change_replica_timeout.getDescriptor());

    replica.packet_receiver->cancel();
    replica.change_replica_timeout.reset();

    --offset_states[fd_to_replica_location[replica.packet_receiver->getFileDescriptor()].offset].active_connection_count;
    fd_to_replica_location.erase(replica.packet_receiver->getFileDescriptor());
    timeout_fd_to_replica_location.erase(replica.change_replica_timeout.getDescriptor());

    --active_connection_count;

    if (disconnect)
        replica.connection->disconnect();
    replica.connection = nullptr;
}

void HedgedConnections::setAsyncCallback(AsyncCallback async_callback)
{
    for (auto & offset_status : offset_states)
    {
        for (auto & replica : offset_status.replicas)
        {
            if (replica.connection)
                replica.connection->setAsyncCallback(async_callback);
        }
    }
}

}
#endif
