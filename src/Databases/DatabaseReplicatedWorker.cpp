#include <Databases/DatabaseReplicatedWorker.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/DDLTask.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int NOT_A_LEADER;
    extern const int UNFINISHED;
}

static constexpr const char * FORCE_AUTO_RECOVERY_DIGEST = "42";

DatabaseReplicatedDDLWorker::DatabaseReplicatedDDLWorker(DatabaseReplicated * db, ContextPtr context_)
    : DDLWorker(/* pool_size */ 1, db->zookeeper_path + "/log", context_, nullptr, {}, fmt::format("DDLWorker({})", db->getDatabaseName()))
    , database(db)
{
    /// Pool size must be 1 to avoid reordering of log entries.
    /// TODO Make a dependency graph of DDL queries. It will allow to execute independent entries in parallel.
    /// We also need similar graph to load tables on server startup in order of topsort.
}

bool DatabaseReplicatedDDLWorker::initializeMainThread()
{
    {
        std::lock_guard lock(initialization_duration_timer_mutex);
        initialization_duration_timer.emplace();
        initialization_duration_timer->start();
    }

    while (!stop_flag)
    {
        try
        {
            chassert(!database->is_probably_dropped);
            auto zookeeper = getAndSetZooKeeper();
            if (database->is_readonly)
                database->tryConnectToZooKeeperAndInitDatabase(LoadingStrictnessLevel::ATTACH);
            if (database->is_probably_dropped)
            {
                /// The flag was set in tryConnectToZooKeeperAndInitDatabase
                LOG_WARNING(log, "Exiting main thread, because the database was probably dropped");
                /// NOTE It will not stop cleanup thread until DDLWorker::shutdown() call (cleanup thread will just do nothing)
                break;
            }

            if (database->db_settings.max_retries_before_automatic_recovery &&
                database->db_settings.max_retries_before_automatic_recovery <= subsequent_errors_count)
            {
                String current_task_name;
                {
                    std::unique_lock lock{mutex};
                    current_task_name = current_task;
                }
                LOG_WARNING(log, "Database got stuck at processing task {}: it failed {} times in a row with the same error. "
                                 "Will reset digest to mark our replica as lost, and trigger recovery from the most up-to-date metadata "
                                 "from ZooKeeper. See max_retries_before_automatic_recovery setting. The error: {}",
                            current_task, subsequent_errors_count, last_unexpected_error);

                String digest_str;
                zookeeper->tryGet(database->replica_path + "/digest", digest_str);
                LOG_WARNING(log, "Resetting digest from {} to {}", digest_str, FORCE_AUTO_RECOVERY_DIGEST);
                zookeeper->trySet(database->replica_path + "/digest", FORCE_AUTO_RECOVERY_DIGEST);
            }

            initializeReplication();
            initialized = true;
            {
                std::lock_guard lock(initialization_duration_timer_mutex);
                initialization_duration_timer.reset();
            }
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Error on initialization of {}", database->getDatabaseName()));
            sleepForSeconds(5);
        }
    }

    {
        std::lock_guard lock(initialization_duration_timer_mutex);
        initialization_duration_timer.reset();
    }

    return false;
}

void DatabaseReplicatedDDLWorker::shutdown()
{
    DDLWorker::shutdown();
    wait_current_task_change.notify_all();
}

void DatabaseReplicatedDDLWorker::initializeReplication()
{
    /// Check if we need to recover replica.
    /// Invariant: replica is lost if it's log_ptr value is less then max_log_ptr - logs_to_keep.

    auto zookeeper = getAndSetZooKeeper();

    /// Create "active" node (remove previous one if necessary)
    String active_path = fs::path(database->replica_path) / "active";
    String active_id = toString(ServerUUID::get());
    zookeeper->deleteEphemeralNodeIfContentMatches(active_path, active_id);
    if (active_node_holder)
        active_node_holder->setAlreadyRemoved();
    active_node_holder.reset();

    String log_ptr_str = zookeeper->get(database->replica_path + "/log_ptr");
    UInt32 our_log_ptr = parse<UInt32>(log_ptr_str);
    UInt32 max_log_ptr = parse<UInt32>(zookeeper->get(database->zookeeper_path + "/max_log_ptr"));
    logs_to_keep = parse<UInt32>(zookeeper->get(database->zookeeper_path + "/logs_to_keep"));

    UInt64 digest;
    String digest_str;
    UInt64 local_digest;
    if (zookeeper->tryGet(database->replica_path + "/digest", digest_str))
    {
        digest = parse<UInt64>(digest_str);
        std::lock_guard lock{database->metadata_mutex};
        local_digest = database->tables_metadata_digest;
    }
    else
    {
        LOG_WARNING(log, "Did not find digest in ZooKeeper, creating it");
        /// Database was created by old ClickHouse versions, let's create the node
        std::lock_guard lock{database->metadata_mutex};
        digest = local_digest = database->tables_metadata_digest;
        digest_str = toString(digest);
        zookeeper->create(database->replica_path + "/digest", digest_str, zkutil::CreateMode::Persistent);
    }

    LOG_TRACE(log, "Trying to initialize replication: our_log_ptr={}, max_log_ptr={}, local_digest={}, zk_digest={}",
              our_log_ptr, max_log_ptr, local_digest, digest);

    bool is_new_replica = our_log_ptr == 0;
    bool lost_according_to_log_ptr = our_log_ptr + logs_to_keep < max_log_ptr;
    bool lost_according_to_digest = database->db_settings.check_consistency && local_digest != digest;

    if (is_new_replica || lost_according_to_log_ptr || lost_according_to_digest)
    {
        if (!is_new_replica)
            LOG_WARNING(log, "Replica seems to be lost: our_log_ptr={}, max_log_ptr={}, local_digest={}, zk_digest={}",
                        our_log_ptr, max_log_ptr, local_digest, digest);
        database->recoverLostReplica(zookeeper, our_log_ptr, max_log_ptr);
        zookeeper->set(database->replica_path + "/log_ptr", toString(max_log_ptr));
        initializeLogPointer(DDLTaskBase::getLogEntryName(max_log_ptr));
    }
    else
    {
        String log_entry_name = DDLTaskBase::getLogEntryName(our_log_ptr);
        last_skipped_entry_name.emplace(log_entry_name);
        initializeLogPointer(log_entry_name);
    }

    {
        std::lock_guard lock{database->metadata_mutex};
        if (!database->checkDigestValid(context, false))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent database metadata after reconnection to ZooKeeper");
    }

    zookeeper->create(active_path, active_id, zkutil::CreateMode::Ephemeral);
    active_node_holder_zookeeper = zookeeper;
    active_node_holder = zkutil::EphemeralNodeHolder::existing(active_path, *active_node_holder_zookeeper);
}

/**
 *  在DDLWorker中是virtual方法，动态绑定，因此DatabaseReplicatedDDLWorker::enqueueQuery重写了DDLWorker::enqueueQuery
 */
String DatabaseReplicatedDDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    auto zookeeper = getAndSetZooKeeper();
    return enqueueQueryImpl(zookeeper, entry, database);
}


bool DatabaseReplicatedDDLWorker::waitForReplicaToProcessAllEntries(UInt64 timeout_ms)
{
    auto zookeeper = getAndSetZooKeeper();
    const auto our_log_ptr_path = database->replica_path + "/log_ptr";
    const auto max_log_ptr_path = database->zookeeper_path + "/max_log_ptr";
    UInt32 our_log_ptr = parse<UInt32>(zookeeper->get(our_log_ptr_path));
    UInt32 max_log_ptr = parse<UInt32>(zookeeper->get(max_log_ptr_path));
    chassert(our_log_ptr <= max_log_ptr);

    /// max_log_ptr is the number of the last successfully executed request on the initiator
    /// The log could contain other entries which are not committed yet
    /// This equality is enough to say that current replicas is up-to-date
    if (our_log_ptr == max_log_ptr)
        return true;

    auto max_log =  DDLTask::getLogEntryName(max_log_ptr);

    {
        std::unique_lock lock{mutex};
        LOG_TRACE(log, "Waiting for worker thread to process all entries before {}, current task is {}", max_log, current_task);
        bool processed = wait_current_task_change.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]()
        {
            return zookeeper->expired() || current_task >= max_log || stop_flag;
        });

        if (!processed)
            return false;
    }

    /// Lets now wait for max_log_ptr to be processed
    Coordination::Stat stat;
    auto event_ptr = std::make_shared<Poco::Event>();
    auto new_log = zookeeper->get(our_log_ptr_path, &stat, event_ptr);

    if (new_log == toString(max_log_ptr))
        return true;

    return event_ptr->tryWait(timeout_ms);
}

/**
 * 调用者是 DatabaseReplicatedDDLWorker::enqueueQuery
 * 父类DDLWorker没有这个方法
 * 它的作用是将一个 DDL 查询写入 ZooKeeper 中的 DDL 日志队列，
 * 并为它分配一个唯一递增的编号，以确保所有副本都按相同的顺序执行 DDL。
 * @param zookeeper
 * @param entry
 * @param database
 * @param committed
 * @return
 */
String DatabaseReplicatedDDLWorker::enqueueQueryImpl(const ZooKeeperPtr & zookeeper, DDLLogEntry & entry,
                               DatabaseReplicated * const database, bool committed)
{
    // 可以看到，这里的Query Path是database->zookeeper_path, 即这个ReplicatedDatbase的zookeeper path
    const String query_path_prefix = database->zookeeper_path + "/log/query-";

    /// We cannot create sequential node and it's ephemeral child in a single transaction, so allocate sequential number another way
    String counter_prefix = database->zookeeper_path + "/counter/cnt-";
    // 用于防止多个副本并发创建 cnt- 节点时竞争冲突
    String counter_lock_path = database->zookeeper_path + "/counter_lock";

    String counter_path;
    size_t iters = 1000;
    while (--iters) // 反复重试，直到成功创建一个sequential节点
    {
        Coordination::Requests ops;
        //  ephemeral 节点，抢锁
        ops.emplace_back(zkutil::makeCreateRequest(counter_lock_path, database->getFullReplicaName(), zkutil::CreateMode::Ephemeral));
        // 创建一个 /counter/cnt为前缀的ephemeral的sequential节点，比如， /counter/cnt-00000123 节点
        ops.emplace_back(zkutil::makeCreateRequest(counter_prefix, "", zkutil::CreateMode::EphemeralSequential));
        Coordination::Responses res;
        // 创建对应的counter节点
        Coordination::Error code = zookeeper->tryMulti(ops, res);
        if (code == Coordination::Error::ZOK)
        {   // 成功创建了对应的sequential节点，这时候counter_path就含有了对应的序号
            counter_path = dynamic_cast<const Coordination::CreateResponse &>(*res.back()).path_created;
            break;
        }
        else if (code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, res);
    }
    // 没有创建成功
    if (counter_path.empty())
        throw Exception(ErrorCodes::UNFINISHED,
                        "Cannot enqueue query, because some replica are trying to enqueue another query. "
                        "It may happen on high queries rate or, in rare cases, after connection loss. Client should retry.");

    // 根据前面创建的 sequential 节点路径 counter_path，生成最终的 DDL 日志节点路径 node_path，
    // 比如，counter_path是/clickhouse/test_db/log/counter/cnt-0012，那么对应的node_path就是 /clickhouse/test_db/log/query-0012
    // /clickhouse/databases/db1/log/query-00000123
    String node_path = query_path_prefix + counter_path.substr(counter_prefix.size());

    /// Now create task in queue
    Coordination::Requests ops;
    /// Query is not committed yet, but we have to write it into log to avoid reordering
    ops.emplace_back(zkutil::makeCreateRequest(node_path, entry.toString(), zkutil::CreateMode::Persistent));
    /// '/try' will be replaced with '/committed' or will be removed due to expired session or other error
    if (committed)
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/committed", database->getFullReplicaName(), zkutil::CreateMode::Persistent));
    else
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/try", database->getFullReplicaName(), zkutil::CreateMode::Ephemeral));
    /// We don't need it anymore
    // 已经成功创建了query节点，因此不再需要这个CreateMode::EphemeralSequential节点了
    ops.emplace_back(zkutil::makeRemoveRequest(counter_path, -1));
    /// Unlock counters
    // 也不再需要counter_lock_path，即不再需要这个分布式互斥锁了
    ops.emplace_back(zkutil::makeRemoveRequest(counter_lock_path, -1));
    /// Create status dirs
    // 在query-00000012下一次性创建对应的状态子目录，用于各副本跟踪执行进度
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/active", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/finished", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(node_path + "/synced", "", zkutil::CreateMode::Persistent));
    zookeeper->multi(ops); // 一次性执行多个命令


    return node_path;
}

/**
 * 这个方法与DDLWorker无关，是 DatabaseReplicatedDDLWorker 独有的方法
 * 在 DatabaseReplicated::tryEnqueueReplicatedDDL中执行，即DatabaseReplicated委托DatabaseReplicatedDDLWorker来执行
 * @param entry
 * @param query_context
 * @return
 */
String DatabaseReplicatedDDLWorker::tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context)
{
    /// NOTE Possibly it would be better to execute initial query on the most up-to-date node,
    /// but it requires more complex logic around /try node.

    OpenTelemetry::SpanHolder span(__FUNCTION__);
    span.addAttribute("clickhouse.cluster", database->getDatabaseName());
    entry.tracing_context = OpenTelemetry::CurrentContext();

    auto zookeeper = getAndSetZooKeeper();
    // 获取当前的max_id值
    UInt32 our_log_ptr = getLogPointer();
    // 当前keeper上记录的max_log_ptr的值
    UInt32 max_log_ptr = parse<UInt32>(zookeeper->get(database->zookeeper_path + "/max_log_ptr"));
    // 如果当前在zookeepeer上，这个数据库允许的最大的lag的数量已经超出限制，则拒绝执行
    if (our_log_ptr + database->db_settings.max_replication_lag_to_enqueue < max_log_ptr)
        throw Exception(ErrorCodes::NOT_A_LEADER, "Cannot enqueue query on this replica, "
                        "because it has replication lag of {} queries. Try other replica.", max_log_ptr - our_log_ptr);
    // 放入到zookeeper队列中，返回对应的query节点  /log/query-00123
    String entry_path = enqueueQuery(entry);
    auto try_node = zkutil::EphemeralNodeHolder::existing(entry_path + "/try", *zookeeper);
    String entry_name = entry_path.substr(entry_path.rfind('/') + 1);
    auto task = std::make_unique<DatabaseReplicatedTask>(entry_name, entry_path, database);
    task->entry = entry;
    task->parseQueryFromEntry(context);
    chassert(!task->entry.query.empty());
    assert(!zookeeper->exists(task->getFinishedNodePath()));
    task->is_initial_query = true; // 这是初始的query对应的task

    LOG_DEBUG(log, "Waiting for worker thread to process all entries before {}", entry_name);
    UInt64 timeout = query_context->getSettingsRef().database_replicated_initial_query_timeout_sec;
    {
        std::unique_lock lock{mutex};
        bool processed = wait_current_task_change.wait_for(lock, std::chrono::seconds(timeout), [&]()
        {
            assert(zookeeper->expired() || current_task <= entry_name);
            return zookeeper->expired() || current_task == entry_name || stop_flag;
        });

        if (!processed)
            throw Exception(ErrorCodes::UNFINISHED, "Timeout: Cannot enqueue query on this replica, "
                            "most likely because replica is busy with previous queue entries");
    }

    if (zookeeper->expired() || stop_flag)
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "ZooKeeper session expired or replication stopped, try again");
    // 调用 DDLWorker::processTask， 这里可以看到，这个DatabaseReplicatedWorker在往keerp上写入Entry以后，自己直接执行属于自己的这部分task，而不是
    // 同其他follower一起领取任务
    processTask(*task, zookeeper);
    // 任务没有执行成功，抛出异常
    if (!task->was_executed)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Entry {} was executed, but was not committed: code {}: {}",
            task->entry_name,
            task->execution_status.code,
            task->execution_status.message);
    }

    try_node->setAlreadyRemoved();

    return entry_path;
}

/**
 * virtual方法initAndCheckTask，   重写了 DDLWorker::initAndCheckTask,
 * @param entry_name
 * @param out_reason
 * @param zookeeper
 * @return
 */
DDLTaskPtr DatabaseReplicatedDDLWorker::initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper)
{
    {
        std::lock_guard lock{mutex};
        if (current_task < entry_name)
        {
            current_task = entry_name;
            wait_current_task_change.notify_all();
        }
    }

    UInt32 our_log_ptr = getLogPointer();
    UInt32 entry_num = DatabaseReplicatedTask::getLogEntryNumber(entry_name);

    if (entry_num <= our_log_ptr)
    {
        out_reason = fmt::format("Task {} already executed according to log pointer {}", entry_name, our_log_ptr);
        return {};
    }

    String entry_path = fs::path(queue_dir) / entry_name;
    auto task = std::make_unique<DatabaseReplicatedTask>(entry_name, entry_path, database);

    String initiator_name;
    zkutil::EventPtr wait_committed_or_failed = std::make_shared<Poco::Event>();

    String try_node_path = fs::path(entry_path) / "try";
    if (zookeeper->tryGet(try_node_path, initiator_name, nullptr, wait_committed_or_failed))
    {
        task->is_initial_query = initiator_name == task->host_id_str;

        /// Query is not committed yet. We cannot just skip it and execute next one, because reordering may break replication.
        LOG_TRACE(log, "Waiting for initiator {} to commit or rollback entry {}", initiator_name, entry_path);
        constexpr size_t wait_time_ms = 1000;
        size_t max_iterations = database->db_settings.wait_entry_commited_timeout_sec;
        size_t iteration = 0;

        while (!wait_committed_or_failed->tryWait(wait_time_ms))
        {
            if (stop_flag)
            {
                /// We cannot return task to process and we cannot return nullptr too,
                /// because nullptr means "task should not be executed".
                /// We can only exit by exception.
                throw Exception(ErrorCodes::UNFINISHED, "Replication was stopped");
            }

            if (max_iterations <= ++iteration)
            {
                /// What can we do if initiator hangs for some reason? Seems like we can remove /try node.
                /// Initiator will fail to commit ZooKeeperMetadataTransaction (including ops for replicated table) if /try does not exist.
                /// But it's questionable.

                /// We use tryRemove(...) because multiple hosts (including initiator) may try to do it concurrently.
                auto code = zookeeper->tryRemove(try_node_path);
                if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
                    throw Coordination::Exception::fromPath(code, try_node_path);

                if (!zookeeper->exists(fs::path(entry_path) / "committed"))
                {
                    out_reason = fmt::format("Entry {} was forcefully cancelled due to timeout", entry_name);
                    return {};
                }
            }
        }
    }

    if (!zookeeper->exists(fs::path(entry_path) / "committed"))
    {
        out_reason = fmt::format("Entry {} hasn't been committed", entry_name);
        return {};
    }

    if (task->is_initial_query)
    {
        assert(!zookeeper->exists(fs::path(entry_path) / "try"));
        assert(zookeeper->exists(fs::path(entry_path) / "committed") == (zookeeper->get(task->getFinishedNodePath()) == ExecutionStatus(0).serializeText()));
        out_reason = fmt::format("Entry {} has been executed as initial query", entry_name);
        return {};
    }

    String node_data;
    if (!zookeeper->tryGet(entry_path, node_data))
    {
        LOG_ERROR(log, "Cannot get log entry {}", entry_path);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "should be unreachable");
    }

    task->entry.parse(node_data);

    if (task->entry.query.empty())
    {
        /// Some replica is added or removed, let's update cached cluster
        database->setCluster(database->getClusterImpl());
        if (!database->replica_group_name.empty())
            database->setCluster(database->getClusterImpl(/*all_groups*/ true), /*all_groups*/ true);
        out_reason = fmt::format("Entry {} is a dummy task", entry_name);
        return {};
    }

    task->parseQueryFromEntry(context);

    if (zookeeper->exists(task->getFinishedNodePath()))
    {
        out_reason = fmt::format("Task {} has been already processed", entry_name);
        return {};
    }

    return task;
}

bool DatabaseReplicatedDDLWorker::canRemoveQueueEntry(const String & entry_name, const Coordination::Stat &)
{
    UInt32 entry_number = DDLTaskBase::getLogEntryNumber(entry_name);
    UInt32 max_log_ptr = parse<UInt32>(getAndSetZooKeeper()->get(fs::path(database->zookeeper_path) / "max_log_ptr"));
    return entry_number + logs_to_keep < max_log_ptr;
}

void DatabaseReplicatedDDLWorker::initializeLogPointer(const String & processed_entry_name)
{
    updateMaxDDLEntryID(processed_entry_name);
}

UInt32 DatabaseReplicatedDDLWorker::getLogPointer() const
{
    // 在 DDLWorker::updateMaxDDLEntryID 方法中会更新max_id的值
    /// NOTE it may not be equal to the log_ptr in zk:
    ///  - max_id can be equal to log_ptr - 1 due to race condition (when it's updated in zk, but not updated in memory yet)
    ///  - max_id can be greater than log_ptr, because log_ptr is not updated for failed and dummy entries
    return max_id.load();
}

UInt64 DatabaseReplicatedDDLWorker::getCurrentInitializationDurationMs() const
{
    std::lock_guard lock(initialization_duration_timer_mutex);
    return initialization_duration_timer ? initialization_duration_timer->elapsedMilliseconds() : 0;
}

}
