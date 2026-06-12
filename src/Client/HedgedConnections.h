#pragma once
#if defined(OS_LINUX)

#include <functional>
#include <queue>
#include <optional>

#include <Client/HedgedConnectionsFactory.h>
#include <Client/IConnections.h>
#include <Client/PacketReceiver.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** To receive data from multiple replicas (connections) from one shard asynchronously.
  * The principe of Hedged Connections is used to reduce tail latency:
  * if we don't receive data from replica and there is no progress in query execution
  * for a long time, we try to get new replica and send query to it,
  * without cancelling working with previous replica. This class
  * supports all functionality that MultipleConnections has.
  */
class HedgedConnections : public IConnections
{
public:
    using PacketReceiverPtr = std::unique_ptr<PacketReceiver>;
    struct ReplicaState
    {
        // 通过一个Connection，来构造这个Replica的ReplicaState
        explicit ReplicaState(Connection * connection_) : connection(connection_), packet_receiver(std::make_unique<PacketReceiver>(connection_))
        {
        }

        Connection * connection = nullptr;
        PacketReceiverPtr packet_receiver;
        // 构造 ReplicaState 的时候，会构造一个 TimerDescriptor 对象，这个timeout在HedgedConnection层，通过 receive_data_timeout 来设置。 需要和ReplicaStatus区分开。
        TimerDescriptor change_replica_timeout;
        bool is_change_replica_timeout_expired = false;
    };


    // 一个OffsetState中存放了相同的offset的一组replica
    struct OffsetState
    {
        /// Replicas with the same offset.
        std::vector<ReplicaState> replicas;
        /// An amount of active replicas. When can_change_replica is false,
        /// active_connection_count is always <= 1 (because we stopped working with
        /// other replicas with the same offset)
        size_t active_connection_count = 0;
        bool can_change_replica = true; // 初始状态下，因为还没有开始hedge，因此是可以change_replica的

        /// This flag is true when this offset is in queue for
        /// new replicas. It's needed to process receive timeout
        /// (throw an exception when receive timeout expired and there is no
        /// new replica in process)
        /**
         * 2s receive_data_timeout 到期（getReadyReplicaLocation）
            → next_replica_in_process = true
            → offsets_queue.push(offset)
            → startNewReplica()                    // Factory 开始建「下一 replica」
            …… 可能 NOT_READY，Factory epoll 等 ……
            processNewReplicaState(READY)
            → emplace_back 第二路、active_connection_count++
            → pipeline.run(sendQuery)              // 第二路才开始跑查询
            → next_replica_in_process = false
         */
        bool next_replica_in_process = false;
    };

    /// epoll_wait returns a file descriptor (socket, receive_data_timeout timerfd, etc.).
    /// We need to map that fd back to the corresponding replica connection.
    /// Maps fd_to_replica_location and timeout_fd_to_replica_location store this mapping.
    ///
    /// Replica data is organized as offset_states[offset].replicas[index]:
    ///   offset — which parallel read lane within this shard (same as parallel_replica_offset setting).
    ///            When max_parallel_replicas > 1, one shard may have several lanes reading different
    ///            parts of data in parallel (offset 0, 1, 2, ...). This is NOT the shard number.
    ///   index  — which hedged connection within the same offset (0 = first connection,
    ///            1 = second connection started after receive_data_timeout expired, ...).
    ///            index is needed because hedged requests keep the old replica alive and add a new one
    ///            with the same offset.
    ///
    /// Example:
    ///   offset_states[0].replicas[0] — lane 0, first replica (initial connection)
    ///   offset_states[0].replicas[1] — lane 0, hedged second replica (after 2s timeout)
    ///   offset_states[1].replicas[0] — lane 1, first replica
    struct ReplicaLocation
    {
        size_t offset; /// parallel_replica_offset：本 shard 内第几路并行读，0/1/2…，不是 shard 编号
        size_t index;  /// 同一 offset 下第几条 hedged 连接：0=第一路，1=receive_data_timeout 后的第二路…
    };

    HedgedConnections(
        const ConnectionPoolWithFailoverPtr & pool_,
        ContextPtr context_,
        const ConnectionTimeouts & timeouts_,
        const ThrottlerPtr & throttler,
        PoolMode pool_mode,
        std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr,
        /// using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;
        AsyncCallback async_callback = {},
        GetPriorityForLoadBalancing::Func priority_func = {});

    void sendScalarsData(Scalars & data) override;

    void sendExternalTablesData(std::vector<ExternalTablesData> & data) override;

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 stage,
        ClientInfo & client_info,
        bool with_pending_data,
        const std::vector<String> & external_roles) override;

    void sendQueryPlan(const QueryPlan & query_plan) override;

    void sendClusterFunctionReadTaskResponse(const ClusterFunctionReadTaskResponse &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "sendReadTaskResponse in not supported with HedgedConnections");
    }

    void sendMergeTreeReadTaskResponse(const ParallelReadResponse &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "sendMergeTreeReadTaskResponse in not supported with HedgedConnections");
    }

    /**
     *
     */
    Packet receivePacket() override;

    Packet receivePacketUnlocked(AsyncCallback async_callback) override;

    UInt64 receivePacketTypeUnlocked(AsyncCallback async_callback) override;

    void disconnect() override;

    void sendCancel() override;

    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) override;

    Packet drain() override;

    std::string dumpAddresses() const override;

    size_t size() const override { return offset_states.size(); }

    bool hasActiveConnections() const override { return active_connection_count > 0; }

    void setReplicaInfo(ReplicaInfo value) override { replica_info = value; }

    void setAsyncCallback(AsyncCallback async_callback) override;

private:
    /// If we don't receive data from replica and there is no progress in query
    /// execution for receive_data_timeout, we are trying to get new
    /// replica and send query to it. Beside sending query, there are some
    /// additional actions like sendScalarsData or sendExternalTablesData and we need
    /// to perform these actions in the same order on the new replica. So, we will
    /// save actions with replicas in pipeline to perform them on the new replicas.
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaState &)> send_function);

        void run(ReplicaState & replica);
    private:
        std::vector<std::function<void(ReplicaState &)>> pipeline;
    };

    Packet receivePacketFromReplica(const ReplicaLocation & replica_location);

    ReplicaLocation getReadyReplicaLocation(AsyncCallback async_callback = {});

    bool resumePacketReceiver(const ReplicaLocation & replica_location);

    void disableChangingReplica(const ReplicaLocation & replica_location);

    void startNewReplica();

    void checkNewReplica();

    void processNewReplicaState(HedgedConnectionsFactory::State state, Connection * connection);

    void finishProcessReplica(ReplicaState & replica, bool disconnect);

    int getReadyFileDescriptor(AsyncCallback async_callback = {});

    HedgedConnectionsFactory hedged_connections_factory;

    /// All replicas in offset_states[offset] is responsible for process query
    /// with setting parallel_replica_offset = offset. In common situations
    /// replica_states[offset].replicas.size() = 1 (like in MultiplexedConnections).
    std::vector<OffsetState> offset_states;

    /// Map socket file descriptor to replica location (it's offset and index in OffsetState.replicas).
    std::unordered_map<int, ReplicaLocation> fd_to_replica_location;

    /// Map receive data timeout file descriptor to replica location.
    std::unordered_map<int, ReplicaLocation> timeout_fd_to_replica_location;

    /// A queue of offsets for new replicas. When we get RECEIVE_DATA_TIMEOUT from
    /// the replica, we push it's offset to this queue and start trying to get
    /// new replica.
    std::queue<int> offsets_queue;

    /// The current number of valid connections to the replicas of this shard.
    size_t active_connection_count = 0; // 当前这个shard的所有的active connection 的数量，如果是parallel replica，则是所有的offset的active_connection_count的和

    /// We count offsets in which we can't change replica anymore,
    /// it's needed to cancel choosing new replicas when we
    /// disabled replica changing in all offsets.
    size_t offsets_with_disabled_changing_replica = 0;

    Pipeline pipeline_for_new_replicas;

    /// New replica may not support two-level aggregation due to version incompatibility.
    /// If we didn't disabled it, we need to skip this replica.
    bool disable_two_level_aggregation = false;

    /// We will save replica with last received packet
    /// (except cases when packet type is EndOfStream or Exception)
    /// to resume it's packet receiver when new packet is needed.
    std::optional<ReplicaLocation> replica_with_last_received_packet;

    Packet last_received_packet;

    Epoll epoll; // 构造了一个EPoll对象
    ContextPtr context;
    const Settings & settings;
    ThrottlerPtr throttler;
    bool sent_query = false;
    bool cancelled = false;

    ReplicaInfo replica_info;

    mutable std::mutex cancel_mutex;
};

}
#endif
