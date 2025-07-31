#include <filesystem>

#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ParserQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/IStorage.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/setThreadName.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>
#include <Common/isLocalAddress.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Timestamp.h>
#include <base/sleep.h>
#include <base/getFQDNOrHostName.h>
#include <Common/logger_useful.h>
#include <base/sort.h>
#include <memory>
#include <random>
#include <pcg_random.hpp>
#include <Common/scope_guard_safe.h>
#include <Common/ThreadPool.h>

#include <Interpreters/ZooKeeperLog.h>

namespace fs = std::filesystem;


namespace CurrentMetrics
{
    extern const Metric DDLWorkerThreads;
    extern const Metric DDLWorkerThreadsActive;
    extern const Metric DDLWorkerThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNFINISHED;
    extern const int NOT_A_LEADER;
    extern const int TABLE_IS_READ_ONLY;
    extern const int KEEPER_EXCEPTION;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
}

constexpr const char * TASK_PROCESSED_OUT_REASON = "Task has been already processed";


DDLWorker::DDLWorker(
    int pool_size_,
    // // 参考 DatabaseReplicatedDDLWorker::DatabaseReplicatedDDLWorker构造方法，可以看到DatabaseReplicatedDDLWorker的queue_dir是对应的Database的dir
    const std::string & zk_root_dir,
    ContextPtr context_,
    const Poco::Util::AbstractConfiguration * config,
    const String & prefix, // DDLWorker
    const String & logger_name,
    const CurrentMetrics::Metric * max_entry_metric_,
    const CurrentMetrics::Metric * max_pushed_entry_metric_)
    : context(Context::createCopy(context_))
    , log(getLogger(logger_name))
    , pool_size(pool_size_)
    , max_entry_metric(max_entry_metric_)
    , max_pushed_entry_metric(max_pushed_entry_metric_)
{
    if (max_entry_metric)
        CurrentMetrics::set(*max_entry_metric, 0);

    if (max_pushed_entry_metric)
        CurrentMetrics::set(*max_pushed_entry_metric, 0);

    if (1 < pool_size)
    {
        LOG_WARNING(log, "DDLWorker is configured to use multiple threads. "
                         "It's not recommended because queries can be reordered. Also it may cause some unknown issues to appear.");
        worker_pool = std::make_unique<ThreadPool>(CurrentMetrics::DDLWorkerThreads, CurrentMetrics::DDLWorkerThreadsActive, CurrentMetrics::DDLWorkerThreadsScheduled, pool_size);
    }

    queue_dir = zk_root_dir;
    if (queue_dir.back() == '/')
        queue_dir.resize(queue_dir.size() - 1);

    if (config)
    {
        task_max_lifetime = config->getUInt64(prefix + ".task_max_lifetime", static_cast<UInt64>(task_max_lifetime));
        cleanup_delay_period = config->getUInt64(prefix + ".cleanup_delay_period", static_cast<UInt64>(cleanup_delay_period));
        max_tasks_in_queue = std::max<UInt64>(1, config->getUInt64(prefix + ".max_tasks_in_queue", max_tasks_in_queue));

        if (config->has(prefix + ".host_name"))
            config_host_name = config->getString(prefix + ".host_name"); // DDLWorker.host_name

        if (config->has(prefix + ".profile"))
            context->setSetting("profile", config->getString(prefix + ".profile"));
    }

    if (context->getSettingsRef().readonly)
    {
        LOG_WARNING(log, "Distributed DDL worker is run with readonly settings, it will not be able to execute DDL queries Set appropriate system_profile or distributed_ddl.profile to fix this.");
    }

    host_fqdn = getFQDNOrHostName();
    host_fqdn_id = Cluster::Address::toString(host_fqdn, context->getTCPPort());
}

void DDLWorker::startup()
{
    [[maybe_unused]] bool prev_stop_flag = stop_flag.exchange(false);
    chassert(prev_stop_flag);
    main_thread = std::make_unique<ThreadFromGlobalPool>(&DDLWorker::runMainThread, this);
    cleanup_thread = std::make_unique<ThreadFromGlobalPool>(&DDLWorker::runCleanupThread, this);
}

void DDLWorker::shutdown()
{
    bool prev_stop_flag = stop_flag.exchange(true);
    if (!prev_stop_flag)
    {
        queue_updated_event->set();
        cleanup_event->set();
        if (main_thread)
            main_thread->join();
        if (cleanup_thread)
            cleanup_thread->join();
        worker_pool.reset();
    }
}

DDLWorker::~DDLWorker()
{
    DDLWorker::shutdown();
}


ZooKeeperPtr DDLWorker::tryGetZooKeeper() const
{
    std::lock_guard lock(zookeeper_mutex);
    return current_zookeeper;
}

ZooKeeperPtr DDLWorker::getAndSetZooKeeper()
{
    std::lock_guard lock(zookeeper_mutex);

    if (!current_zookeeper || current_zookeeper->expired())
        current_zookeeper = context->getZooKeeper();

    return current_zookeeper;
}

/**
 *  DatabaseReplicatedDDLWorker::initAndCheckTask 重写了该方法，这是一个virtual方法
 *  将Entry转换成对应的DDLTask
 * @param entry_name
 * @param out_reason
 * @param zookeeper
 * @return
 */
DDLTaskPtr DDLWorker::initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper)
{
    if (entries_to_skip.contains(entry_name))
        return {};

    String node_data; // 这个task entry的内容
    String entry_path = fs::path(queue_dir) / entry_name;
    // 创建对应的DDLTask
    auto task = std::make_unique<DDLTask>(entry_name, entry_path);

    if (!zookeeper->tryGet(entry_path, node_data))
    {
        /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
        out_reason = "The task was deleted";
        return {};
    }

    auto write_error_status = [&](const String & host_id, const ExecutionStatus & status, const String & reason)
    {
        LOG_ERROR(log, "Cannot parse DDL task {}: {}. Will try to send error status: {}", entry_name, reason, status.message);
        createStatusDirs(entry_path, zookeeper);
        zookeeper->tryCreate(fs::path(entry_path) / "finished" / host_id, status.serializeText(), zkutil::CreateMode::Persistent);
    };

    auto add_to_skip_set = [&]()
    {
        entries_to_skip.insert(entry_name);
        return nullptr;
    };

    try
    {
        /// Stage 1: parse entry
        task->entry.parse(node_data);
    }
    catch (...)
    {
        /// What should we do if we even cannot parse host name and therefore cannot properly submit execution status?
        /// We can try to create fail node using FQDN if it equal to host name in cluster config attempt will be successful.
        /// Otherwise, that node will be ignored by DDLQueryStatusSource.
        out_reason = "Incorrect task format";
        write_error_status(host_fqdn_id, ExecutionStatus::fromCurrentException(), out_reason);
        return add_to_skip_set();
    }

    /// Stage 2: resolve host_id and check if we should execute query or not
    /// Multiple clusters can use single DDL queue path in ZooKeeper,
    /// So we should skip task if we cannot find current host in cluster hosts list.
    // 在这个task的host list中查看有没有config_host_name,以确定自己是否需要执行这个Task
    // 如果用户配置了DDLWorker.host_name，那么config_host_name就是用户配置的值
    // 如果这个DDLTask中的hosts list中根本没有本机，那么本机不会执行
    if (!task->findCurrentHostID(context, log, zookeeper, config_host_name))
    {
        out_reason = "There is no a local address in host list";
        return add_to_skip_set();
    }

    try
    {
        /// Stage 3.1: parse query
        task->parseQueryFromEntry(context);
        /// Stage 3.2: check cluster and find the host in cluster
        task->setClusterInfo(context, log);
        /// Stage 3.3: output rewritten query back to string
        task->formatRewrittenQuery(context);
    }
    catch (...)
    {
        out_reason = "Cannot parse query or obtain cluster info";
        write_error_status(task->host_id_str, ExecutionStatus::fromCurrentException(), out_reason);
        return add_to_skip_set(); // 将当前的entry添加到skip list中
    }
    // 这个task已经被 **当前机器**执行完成，即task->getFinishedNodePath()确认的是，这个task对应的当前机器的FINISH节点
    if (zookeeper->exists(task->getFinishedNodePath()))
    {
        out_reason = TASK_PROCESSED_OUT_REASON;
        return add_to_skip_set();
    }

    /// Now task is ready for execution
    return task;
}


static void filterAndSortQueueNodes(Strings & all_nodes)
{
    std::erase_if(all_nodes, [] (const String & s) { return !startsWith(s, "query-"); });
    ::sort(all_nodes.begin(), all_nodes.end());
}

/**
 * reinitialized = True 代表刚刚经历过一次keeper的重试并且恢复了过来，因此需要特别消息的处理任务
 * ClickHouse 的 DDLWorker::scheduleTasks(bool reinitialized) 会确保任务按顺序（即 DDL queue 中的 entry ID 递增）被依次执行，避免顺序混乱、跳过或重复执行
 * @param reinitialized
 */
void DDLWorker::scheduleTasks(bool reinitialized)
{
    LOG_DEBUG(log, "Scheduling tasks");
    auto zookeeper = tryGetZooKeeper();

    /// Main thread of DDLWorker was restarted, probably due to lost connection with ZooKeeper.
    /// We have some unfinished tasks.
    /// To avoid duplication of some queries we should try to write execution status again.
    /// To avoid skipping of some entries which were not executed we should be careful when choosing begin_node to start from.
    /// NOTE: It does not protect from all cases of query duplication, see also comments in processTask(...)
    // reinitialized = true, 说明主线程因为 ZooKeeper 掉线等异常重启过, 所以 current_tasks 里面可能有未写 status 的任务
    // 这时我们要：
    // - 检查这些旧任务是否已经在 ZooKeeper 上写入了 finished/ 节点
    // - 如果没写就重新写（避免任务丢失）
    // - 如果写了一半 ZooKeeper 掉了，也要补完 status

    if (reinitialized)
    {
        if (current_tasks.empty())
            LOG_TRACE(log, "Don't have unfinished tasks after restarting");
        else
            LOG_INFO(log, "Have {} unfinished tasks, will check them", current_tasks.size());

        assert(current_tasks.size() <= pool_size + (worker_pool != nullptr));
        auto task_it = current_tasks.begin();
        // 优先将当前还没有执行的任务进行执行，随后才会尝试从Zookeeper上尝试获取新的任务
        while (task_it != current_tasks.end())
        {
            auto & task = *task_it;
            if (task->completely_processed) // 任务已经完全完成了，keeper上的状态也成功更新了
            {
                // 一个已经 completely_processed的task，肯定是已经executed的状态
                chassert(task->was_executed);
                /// Status must be written (but finished/ node may not exist if entry was deleted).
                /// If someone is deleting entry concurrently, then /active status dir must not exist.
                // 如果 task->completely_processed， 那么 不可能 query-000000032/finished/hostname:port不存在，并且 query-000000032/active 存在
                assert(zookeeper->exists(task->getFinishedNodePath()) || !zookeeper->exists(fs::path(task->entry_path) / "active"));
                ++task_it; // 已经完全处理了，因此处理下一个任务
            }
            else if (task->was_executed) // 任务已经执行完成了，但是从  was_executed -> completely_processed 的状态没完成，可能是任务完成以后，keeper连接失效
            {
                // `/clickhouse/task_queue/ddl/query-0000000123/finished/host1:port`
                /// Connection was lost on attempt to write status. Will retry.
                bool status_written = zookeeper->exists(task->getFinishedNodePath());
                /// You might think that the following condition is redundant, because status_written implies completely_processed.
                /// But it's wrong. It's possible that (!task->completely_processed && status_written)
                /// if ZooKeeper successfully received and processed our request
                /// but we lost connection while waiting for the response.
                /// Yeah, distributed systems is a zoo.
                if (status_written) // 已经有了对应的/clickhouse/task_queue/ddl/query-0000000123/finished/host1:port，task的状态却只是 was_executed
                {
                    /// TODO We cannot guarantee that query was actually executed synchronously if connection was lost.
                    /// Let's simple create synced/ node for now, but it would be better to pass UNFINISHED status to initiator
                    /// or wait for query to actually finish (requires https://github.com/ClickHouse/ClickHouse/issues/23513)
                    task->createSyncedNodeIfNeed(zookeeper); // 只有ReplicatedDatabaseDDLTask才需要进行sync
                    task->completely_processed = true; // 完全处理完成
                }
                else // task->was_executed为true，但是却没有写 `/clickhouse/task_queue/ddl/query-0000000123/finished/host1:port` 节点
                {
                    processTask(*task, zookeeper); // 继续处理任务(有可能处理了一半)
                }
                ++task_it; // 下一个任务
            }
            else // task的状态连was_executed都没有到
            {
                /// We didn't even executed a query, so let's just remove it.
                /// We will try to read the task again and execute it from the beginning.
                /// NOTE: We can safely compare entry names as Strings, because they are padded.
                /// Entry name always starts with "query-" and contain exactly 10 decimal digits
                /// of log entry number (with leading zeros).
                if (!first_failed_task_name || task->entry_name < *first_failed_task_name)
                    first_failed_task_name = task->entry_name; // 找到一个序号更小的失败任务

                task_it = current_tasks.erase(task_it);
            }
        }
    }
    // 执行到这里，内存中的current_task已经清空了，开始从zookeeper的队列中领取新的节点并生成新的任务
    // using Strings = std::vector<String>;
    Strings queue_nodes = zookeeper->getChildren(queue_dir, &queue_node_stat, queue_updated_event);
    size_t size_before_filtering = queue_nodes.size();
    filterAndSortQueueNodes(queue_nodes); // 对节点进行排序
    /// The following message is too verbose, but it can be useful to debug mysterious test failures in CI
    LOG_TRACE(log, "scheduleTasks: initialized={}, size_before_filtering={}, queue_size={}, "
                   "entries={}..{}, "
                   "first_failed_task_name={}, current_tasks_size={}, "
                   "last_current_task={}, "
                   "last_skipped_entry_name={}",
                   initialized, size_before_filtering, queue_nodes.size(),
                   queue_nodes.empty() ? "none" : queue_nodes.front(), queue_nodes.empty() ? "none" : queue_nodes.back(),
                   first_failed_task_name ? *first_failed_task_name : "none", current_tasks.size(),
                   current_tasks.empty() ? "none" : current_tasks.back()->entry_name,
                   last_skipped_entry_name ? *last_skipped_entry_name : "none");

    if (max_tasks_in_queue < queue_nodes.size())
        cleanup_event->set();

    // 此时从keeper中获取的所有任务节点，必须和当前内存中正在执行的任务的list进行边界的确定，放置重复或者遗漏
    /// Detect queue start, using:
    /// - skipped tasks
    /// - in memory tasks (that are currently active or were finished recently)
    /// - failed tasks (that should be processed again)
    auto begin_node = queue_nodes.begin();
    if (first_failed_task_name)
    {   // 如果有一个或者多个失败的任务，那么应该从失败的任务开始执行
        /// If we had failed tasks, then we should start from the first failed task.
        chassert(reinitialized);
        // 设置重新执行的开始位置
        begin_node = std::lower_bound(queue_nodes.begin(), queue_nodes.end(), first_failed_task_name);
    }
    else
    {  // 没有失败任务，那么开始执行的起点需要看看当前内存中已经取出的任务
        /// We had no failed tasks. Let's just choose the maximum entry we have previously seen.
        String last_task_name;
        if (!current_tasks.empty())
            last_task_name = current_tasks.back()->entry_name; // 当前内存中的最后一个任务
        // 如果存在最后被skip掉的entry，并且，current_tasks的最后一个task对应的skip掉的entry的后面(即，last_skipped_entry_name的id比当前内存中的最后一个task的id更大)，
        // 那么，最后一个需要执行的task就是last_skipped_entry_name
        if (last_skipped_entry_name && last_task_name < *last_skipped_entry_name)
            last_task_name = *last_skipped_entry_name;
        // 已经在内存中的任务，不需要再从queue中放入内存
        // last_skipped_entry_name和以前的任务，都不需要再重新放入内存
        begin_node = std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_task_name);
    }

    if (begin_node == queue_nodes.end())
        LOG_DEBUG(log, "No tasks to schedule");
    else
        LOG_DEBUG(log, "Will schedule {} tasks starting from {}", std::distance(begin_node, queue_nodes.end()), *begin_node);

    /// Let's ensure that it's exactly the first task we should process.
    /// Maybe such asserts are too paranoid and excessive,
    /// but it's easy enough to break DDLWorker in a very unobvious way by making some minor change in code.
    [[maybe_unused]] bool have_no_tasks_info = !first_failed_task_name && current_tasks.empty() && !last_skipped_entry_name;
    // String DDLWorker::enqueueQuery中可以看到，这里的entry_name 是前缀`query-`加上keeper的序号自动生成的
    // queue_nodes.end == std::find_if(...)代表没有找到任何一个可以执行的entry
    // 如果 have_no_tasks_info 为false，那么:
    // have_no_tasks_info = false 意味着以下情况之一：
    //  first_failed_task_name.has_value() ||    // 有失败的任务
    //  !current_tasks.empty() ||                // 有正在处理的任务
    //  last_skipped_entry_name.has_value()      // 有跳过的任务
    // 这时候， queue_nodes.end() == std::find_if()必须返回true，即，std::find_if()必须在所有的节点上返回false，assert才能最终成立
    assert(have_no_tasks_info || queue_nodes.end() == std::find_if(queue_nodes.begin(), queue_nodes.end(), [&](const String & entry_name)
    {
        // 执行到这个function，意味着这个funciton必须对每个entry返回false，才能让assert成立。
        // 一旦有一个entry返回了true，意味着
        /// We should return true if some invariants are violated.
        String reason;
        // 将对应的entry转换成task，这里，如果这个Entry中的hosts不包含自己，那么会跳过
        // 当这个entry不需要在当前机器执行，返回nullptr，否则返回对应的task
        auto task = initAndCheckTask(entry_name, reason, zookeeper);
        bool maybe_currently_processing = current_tasks.end() != std::find_if(current_tasks.begin(), current_tasks.end(), [&](const auto & t)
        {
            return t->entry_name == entry_name; // 这个取出来的entry居然已经在current_tasks中了
        });
        /// begin_node is something like a log pointer
        // 如果这个entry是在刚刚计算好的起始节点的前面，那么，这个节点肯定是不应该处理的，不然就说明bigin_node 的设置有问题
        if (begin_node == queue_nodes.end() || entry_name < *begin_node)
        {
            /// Return true if entry should be scheduled.
            /// There is a minor race condition: initAndCheckTask(...) may return not null
            /// if someone is deleting outdated entry right now (including finished/ nodes), so we also check active/ status dir.
            bool maybe_concurrently_deleting = task && !zookeeper->exists(fs::path(task->entry_path) / "active");
            // 返回true(会导致断言失败)，代表这个在begin_node前面的居然是一个需要被调度的entry，说明begin_node的设置有问题
            return task && !maybe_concurrently_deleting && !maybe_currently_processing;
        }
        // 对于在begin_node后面的节点
        else if (last_skipped_entry_name.has_value() && !queue_fully_loaded_after_initialization_debug_helper)
        {
            /// If connection was lost during queue loading
            /// we may start processing from finished task (because we don't know yet that it's finished) and it's ok.
            // 没问题，这个节点不成立
            return false;
        }
        else
        {
            /// Return true if entry should not be scheduled.
            // initAndCheckTask返回了null并且OUT_REASON是TASK_PROCESSED
            bool processed = !task && reason == TASK_PROCESSED_OUT_REASON;
            // 如果发现这是一个已经被处理或者正在被处理的节点，或者当前正在被处理的节点，那么也违反了有序性
            return processed || maybe_currently_processing;
        }
    }));
    // 通过检查，没问题，开始从begin_node开始执行任务
    for (auto it = begin_node; it != queue_nodes.end() && !stop_flag; ++it)
    {
        String entry_name = *it;
        LOG_TRACE(log, "Checking task {}", entry_name);

        String reason;
        // // 将对应的entry转换成task，这里，如果这个Entry中的hosts不包含自己，那么会跳过
        // 返回null代表这个entry不应该这个节点执行，比如，这个task的host list中不包含自己
        auto task = initAndCheckTask(entry_name, reason, zookeeper);
        if (task) // 成功地将Entry转换成了task，否则返回null
        {
            queue_fully_loaded_after_initialization_debug_helper = true;
        }
        else
        {
            // 没有成功地将这个Entry转换成task
            LOG_DEBUG(log, "Will not execute task {}: {}", entry_name, reason);
            updateMaxDDLEntryID(entry_name);
            last_skipped_entry_name.emplace(entry_name);
            continue;
        }
        // 通过校验，把这个task放到内存中，下面将会处理，通过std::move转移所有权
        auto & saved_task = saveTask(std::move(task));

        if (worker_pool)
        {
            worker_pool->scheduleOrThrowOnError([this, &saved_task, zookeeper]()
            {
                // 在这里进行异步调用，提交即返回，不用等执行结果
                setThreadName("DDLWorkerExec");
                // 开始处理这个task，这里会进一步调用taskShouldBeExecutedOnLeader，因此也有可能不执行
                processTask(saved_task, zookeeper);
            });
        }
        else
        {
            // 直接在当前线程中同步执行
            processTask(saved_task, zookeeper);
        }
    }
}

DDLTaskBase & DDLWorker::saveTask(DDLTaskPtr && task)
{
    current_tasks.remove_if([](const DDLTaskPtr & t) { return t->completely_processed.load(); });

    /// Tasks are scheduled and executed in main thread <==> Parallel execution is disabled
    assert((worker_pool != nullptr) == (1 < pool_size));

    /// Parallel execution is disabled ==> All previous tasks are failed to start or finished,
    /// so current tasks list must be empty when we are ready to process new one.
    assert(worker_pool || current_tasks.empty());

    /// Parallel execution is enabled ==> Not more than pool_size tasks are currently executing.
    /// Note: If current_tasks.size() == pool_size, then all worker threads are busy,
    /// so we will wait on worker_pool->scheduleOrThrowOnError(...)
    assert(!worker_pool || current_tasks.size() <= pool_size);

    current_tasks.emplace_back(std::move(task));

    if (first_failed_task_name && *first_failed_task_name == current_tasks.back()->entry_name)
        first_failed_task_name.reset();

    return *current_tasks.back();
}

/**
 * 非virtual方法，因此 DatabaseReplicatedDDLWorker 没有重写这个方法
 * 区别于 tryExecuteQueryOnLeaderReplica
 * 这个方法是无协调的对Query的执行，tryExecuteQueryOnLeaderReplica实际上是经过协调以后执行 tryExecuteQuery
 * @param task
 * @param zookeeper
 * @return
 */
bool DDLWorker::tryExecuteQuery(DDLTaskBase & task, const ZooKeeperPtr & zookeeper)
{
    /// Add special comment at the start of query to easily identify DDL-produced queries in query_log
    String query_prefix = "/* ddl_entry=" + task.entry_name + " */ ";
    String query_to_execute = query_prefix + task.query_str;
    String query_to_show_in_logs = query_prefix + task.query_for_logging;

    ReadBufferFromString istr(query_to_execute);
    String dummy_string;
    WriteBufferFromString ostr(dummy_string);
    std::optional<CurrentThread::QueryScope> query_scope;

    try
    {
        auto query_context = task.makeQueryContext(context, zookeeper);

        chassert(!query_context->getCurrentTransaction());
        if (query_context->getSettingsRef().implicit_transaction)
        {
            if (query_context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot begin an implicit transaction inside distributed DDL query");
            query_context->setSetting("implicit_transaction", Field{0});
        }

        query_context->setInitialQueryId(task.entry.initial_query_id);

        if (!task.is_initial_query)
            query_scope.emplace(query_context);

        executeQuery(istr, ostr, !task.is_initial_query, query_context, {}, QueryFlags{ .internal = false, .distributed_backup_restore = task.entry.is_backup_restore });

        if (auto txn = query_context->getZooKeeperMetadataTransaction())
        {
            /// Most queries commit changes to ZooKeeper right before applying local changes,
            /// but some queries does not support it, so we have to do it here.
            if (!txn->isExecuted())
                txn->commit();
        }
    }
    catch (const DB::Exception & e)
    {
        if (task.is_initial_query)
            throw;

        task.execution_status = ExecutionStatus::fromCurrentException();
        tryLogCurrentException(log, "Query " + query_to_show_in_logs + " wasn't finished successfully");

        /// We use return value of tryExecuteQuery(...) in tryExecuteQueryOnLeaderReplica(...) to determine
        /// if replica has stopped being leader and we should retry query.
        /// However, for the majority of exceptions there is no sense to retry, because most likely we will just
        /// get the same exception again. So we return false only for several special exception codes,
        /// and consider query as executed with status "failed" and return true in other cases.
        bool no_sense_to_retry = e.code() != ErrorCodes::KEEPER_EXCEPTION &&
                                 e.code() != ErrorCodes::UNFINISHED &&
                                 e.code() != ErrorCodes::NOT_A_LEADER &&
                                 e.code() != ErrorCodes::TABLE_IS_READ_ONLY &&
                                 e.code() != ErrorCodes::CANNOT_ASSIGN_ALTER &&
                                 e.code() != ErrorCodes::CANNOT_ALLOCATE_MEMORY &&
                                 e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED;
        return no_sense_to_retry;
    }
    catch (...)
    {
        if (task.is_initial_query)
            throw;

        task.execution_status = ExecutionStatus::fromCurrentException();
        tryLogCurrentException(log, "Query " + query_to_show_in_logs + " wasn't finished successfully");

        /// We don't know what exactly happened, but maybe it's Poco::NetException or std::bad_alloc,
        /// so we consider unknown exception as retryable error.
        return false;
    }

    task.execution_status = ExecutionStatus(0);
    LOG_DEBUG(log, "Executed query: {}", query_to_show_in_logs);

    return true;
}

/**
 * 不是virtual方法，因此子类DatabaseReplicated没有重写该方法
 * 线程安全地更新这个DDLWorker的max_id值，即ClickHouse Server内存中的max_id值
 * @param entry_name
 */
void DDLWorker::updateMaxDDLEntryID(const String & entry_name)
{
    // 从 ZooKeeper 的 entry 名称中提取出数值型的 ID，例如从 "query-0000001234" 提取出 1234。
    UInt32 id = DDLTaskBase::getLogEntryNumber(entry_name);
    auto prev_id = max_id.load(std::memory_order_relaxed);
    while (prev_id < id) // 只要当前内存的值小于这个entry的序号
    {
        // 原子操作，只有当 prev_id == max_id 时才会成功把 max_id 改成 id；
        // 如果有别的线程也在更新，它会失败，并自动把最新的 max_id 放到 prev_id 中，进入下一轮比较
        if (max_id.compare_exchange_weak(prev_id, id))
        {
            if (max_entry_metric)
                CurrentMetrics::set(*max_entry_metric, id);
            break;
        }
    }
}

/**
 * 非virtual方法，静态绑定，因此 DatabaseReplicatedWorker 也是使用这个方法来执行
 * 在分布式环境下安全、幂等地执行一个 DDL 任务，并将其状态（active、finished、synced）记录在 ZooKeeper 中
 * 在这里，执行以前，会进一步判断这个Task是否只应该在Leader上执行
*/
void DDLWorker::processTask(DDLTaskBase & task, const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Processing task {} (query: {}, backup restore: {})", task.entry_name, task.query_for_logging, task.entry.is_backup_restore);
    chassert(!task.completely_processed);

    /// Setup tracing context on current thread for current DDL
    OpenTelemetry::TracingContextHolder tracing_ctx_holder(__PRETTY_FUNCTION__,
        task.entry.tracing_context,
        this->context->getOpenTelemetrySpanLog());
    tracing_ctx_holder.root_span.kind = OpenTelemetry::SpanKind::CONSUMER;
    // 获取keeper上对应的active node和finish_node，node以这个机器的host:port为最终节点
    // fs::path(entry_path) / "active" / host_id_str;
    String active_node_path = task.getActiveNodePath();
    // fs::path(entry_path) / "finished" / host_id_str;
    String finished_node_path = task.getFinishedNodePath();

    /// Step 1: Create ephemeral node in active/ status dir.
    /// It allows other hosts to understand that task is currently executing (useful for system.distributed_ddl_queue)
    /// and protects from concurrent deletion or the task.

    /// It will tryRemove(...) on exception
    auto active_node = zkutil::EphemeralNodeHolder::existing(active_node_path, *zookeeper);

    /// Try fast path
    // 获取当前ClickHouse Server的UUID
    const String canary_value = Field(ServerUUID::get()).dump();
    // 在active目录下面为当前的host创建一个对应的临时节点
    /**
     * 每个任务有一个 active 节点，代表“当前有一个 ClickHouse 实例正在执行此任务”。
     * 使用 ephemeral 节点，断线或 crash 自动消失。
     * canary_value 是当前服务器 UUID，方便比对。
     *
     * `/clickhouse/task_queue/ddl/query-0000000123/active/host1:port`
     */
    auto create_active_res = zookeeper->tryCreate(active_node_path, canary_value, zkutil::CreateMode::Ephemeral);
    if (create_active_res != Coordination::Error::ZOK)
    {
        // 没有成功创建active节点，开始分析原因
        // 如果既不是ZNONODE 也不是 ZNODEEXISTS，那么不在具体分析原因，直接抛出异常，无法处理
        if (create_active_res != Coordination::Error::ZNONODE && create_active_res != Coordination::Error::ZNODEEXISTS)
        {
            chassert(Coordination::isHardwareError(create_active_res));
            throw Coordination::Exception::fromPath(create_active_res, active_node_path);
        }
        // 如果create_active_res的状态是节点存在或者节点不存在，那么都需要重新创建节点

        /// Status dirs were not created in enqueueQuery(...) or someone is removing entry
        // 正常情况下，状态目录应该在enqueueQuery(...)已经建好，但是现在不存在，因此，先创建
        if (create_active_res == Coordination::Error::ZNONODE) // 说明 entry 被删了，跳过。
        {
            chassert(dynamic_cast<DatabaseReplicatedTask *>(&task) == nullptr);
            if (task.was_executed)
            {
                // task已经被标记为已执行，说明status目录之前肯定创建成功过，但是现在status目录却不存在，说明task被删除了。我们
                // 在这种情况下不再执行这个任务
                /// Special case:
                /// Task was executed (and we are trying to write status after connection loss) ==> Status dirs were previously created.
                /// (Status dirs were previously created AND active/ does not exist) ==> Task was removed.
                /// We cannot write status, but it's not required anymore, because no one will try to execute it again.
                /// So we consider task as completely processed.
                LOG_WARNING(log, "Task {} is executed, but looks like entry {} was deleted, cannot write status", task.entry_name, task.entry_path);
                task.completely_processed = true;
                return;
            }
            // 先把active和finished状态目录创建好，然后再在下面开始创建对应节点的标记节点
            createStatusDirs(task.entry_path, zookeeper);
        }
        // 这个节点已经存在了，说明是重试场景，尝试删除旧的 ephemeral 节点。
        if (create_active_res == Coordination::Error::ZNODEEXISTS)
        {
            /// Connection has been lost and now we are retrying,
            /// but our previous ephemeral node still exists.
            zookeeper->deleteEphemeralNodeIfContentMatches(active_node_path, canary_value);
        }
        // 再次尝试创建一个代表当前任务是active状态的临时节点
        zookeeper->create(active_node_path, canary_value, zkutil::CreateMode::Ephemeral);
    }

    /// We must hold the lock until task execution status is committed to ZooKeeper,
    /// otherwise another replica may try to execute query again.
    std::unique_ptr<zkutil::ZooKeeperLock> execute_on_leader_lock;
    // 执行到这里，说明active节点的状态已经没问题了，因此开始执行
    /// Step 2: Execute query from the task.
    if (!task.was_executed) // 开始执行任务
    {
        /// If table and database engine supports it, they will execute task.ops by their own in a single transaction
        /// with other zk operations (such as appending something to ReplicatedMergeTree log, or
        /// updating metadata in Replicated database), so we make create request for finished_node_path with status "0",
        /// which means that query executed successfully.
        /**
         * 会将任务的 ops 填充为：删除 active 节点, 创建 finished 节点(持久化节点，而非临时节点)，写入执行状态（成功/失败）。
         */
        task.ops.emplace_back(zkutil::makeRemoveRequest(active_node_path, -1));
        // 在finished节点中写入状态码0，代表执行成功
        task.ops.emplace_back(zkutil::makeCreateRequest(finished_node_path, ExecutionStatus(0).serializeText(), zkutil::CreateMode::Persistent));

        try
        {
            LOG_DEBUG(log, "Executing query: {}", task.query_for_logging);

            StoragePtr storage;
            if (auto * query_with_table = dynamic_cast<ASTQueryWithTableAndOutput *>(task.query.get()); query_with_table)
            {
                if (query_with_table->table)
                {
                    /// It's not CREATE DATABASE
                    auto table_id = context->tryResolveStorageID(*query_with_table, Context::ResolveOrdinary);
                    storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
                }
                // 判断这个task是否只需要在shard的某一个replica上执行，而不是所有的replica上执行
                task.execute_on_leader = storage && taskShouldBeExecutedOnLeader(task.query, storage) && !task.is_circular_replicated;
            }

            // 如果这是一个只能在leader上执行的task，那么，就需要通过tryExecuteQueryOnLeaderReplica方法，保证
            if (task.execute_on_leader)
            {
                // leader执行，其实对于ReplicatedMergeTree，所有replica都是leader，所以，tryExecuteQueryOnLeaderReplica()
                // 方法并不是让这个task只在leader上执行，而是只在shard上的一个replica上执行
                tryExecuteQueryOnLeaderReplica(task, storage, task.entry_path, zookeeper, execute_on_leader_lock);
            }
            else
            {
                // 普通执行，所有的replica上都执行
                storage.reset();
                tryExecuteQuery(task, zookeeper);
            }
        }
        catch (const Coordination::Exception &)
        {
            throw;
        }
        catch (...)
        {
            if (task.is_initial_query)
                throw;
            tryLogCurrentException(log, "An error occurred before execution of DDL task: ");
            task.execution_status = ExecutionStatus::fromCurrentException("An error occurred before execution");
        }

        if (task.execution_status.code != 0) // 任务的执行状态码非0
        {
            bool status_written_by_table_or_db = task.ops.empty();
            bool is_replicated_database_task = dynamic_cast<DatabaseReplicatedTask *>(&task);
            if (status_written_by_table_or_db || is_replicated_database_task)
            {   // 如果是replicated_database_task，那么直接返回失败
                throw Exception(ErrorCodes::UNFINISHED, "Unexpected error: {}", task.execution_status.message);
            }
            else
            {
                /// task.ops where not executed by table or database engine, so DDLWorker is responsible for
                /// writing query execution status into ZooKeeper.
                // 如果不是replicated database task，那么这个DDLWorker有义务将执行的状态码写入到finished_node_path中
                task.ops.emplace_back(zkutil::makeSetRequest(finished_node_path, task.execution_status.serializeText(), -1));
            }
        }
        // task执行完成(有可能成功，有可能失败)，但是还没有开始更新zookeeper上节点状态
        /// We need to distinguish ZK errors occurred before and after query executing
        task.was_executed = true;
    }

    /// Step 3: Create node in finished/ status dir and write execution status.
    /// FIXME: if server fails right here, the task will be executed twice. We need WAL here.
    /// NOTE: If ZooKeeper connection is lost here, we will try again to write query status.
    /// NOTE: If both table and database are replicated, task is executed in single ZK transaction.

    bool status_written = task.ops.empty();
    if (!status_written) // 需要写入keeper ops
    {
        /**
         * 执行剩余所有 ZooKeeper 操作（原子提交）：
         * 删除 active 节点。
         * 创建 finished 节点。
         * 写入执行状态。
         */

        // 一次性执行task中的所有的ops
        zookeeper->multi(task.ops);
        task.ops.clear(); //清空所有的ops
    }

    /// Active node was removed in multi ops
    active_node->setAlreadyRemoved();
    /**
     * 普通的DDLTask没有实现该方法，即什么都不做
     * 但是DatabaseReplicatedTask::createSyncedNodeIfNeed重写了该方法
     */
    task.createSyncedNodeIfNeed(zookeeper);
    updateMaxDDLEntryID(task.entry_name); // 更新max_id的值
    task.completely_processed = true; // 完全完成，任务执行完成，同时完成了Keeper上的任务维护
    subsequent_errors_count = 0;
}

/**
 * 某个 DDL 查询是否应该只在主副本（leader replica）上执行一次，
 * 然后由 Replicated 引擎通过 ZooKeeper 同步到其他副本，而不是每个副本都单独执行一遍。
 * ast_ddl：抽象语法树（AST），表示当前的 DDL 语句。
 * storage：目标表的 Storage 引擎，用于判断是否支持复制。
 * 某些类型的 DDL 查询，比如 ALTER/OPTIMIZE 等，如果每个副本都执行一遍，可能导致不一致或冲突，因此需要通过 leader 副本统一执行一次。
 */
bool DDLWorker::taskShouldBeExecutedOnLeader(const ASTPtr & ast_ddl, const StoragePtr storage)
{
    /// Pure DROP queries have to be executed on each node separately
    /**
     * 对于普通的 DROP TABLE / DROP DATABASE：
     * 每个副本都要单独执行，因为表/库的存在本地依赖。
     * 例外是 TRUNCATE，因为它不删除元数据，只清空数据，可统一由 leader 执行。
     * 从ASTDropQuery::Kind可以看到，DROP, TRUNCATE和DETACH在ClickHouse中都是一种类型的DropQuery
     */
    if (auto * query = ast_ddl->as<ASTDropQuery>(); query && query->kind != ASTDropQuery::Kind::Truncate)
        return false;
    /**
     * 以下几种查询类型才考虑只在 leader 执行：
     * ALTER TABLE
     * OPTIMIZE TABLE
     * DROP TABLE（Truncate）
     * CREATE INDEX / DROP INDEX（ClickHouse 中不常见）
     */
    if (!ast_ddl->as<ASTAlterQuery>() &&
        !ast_ddl->as<ASTOptimizeQuery>() &&
        !ast_ddl->as<ASTDropQuery>() &&
        !ast_ddl->as<ASTCreateIndexQuery>() &&
        !ast_ddl->as<ASTDropIndexQuery>())
        return false; // 在所有replica上执行

    /**
     * 对于 ALTER 操作，还要排除掉这些子类型，它们应在每个副本单独执行：
     *  修改表设置（ALTER ... MODIFY SETTING）
     *  冻结（ALTER ... FREEZE）
     *  移动分区到磁盘/卷（ALTER ... MOVE PARTITION）
     *  注释（ALTER ... COMMENT）
     * */
    if (auto * alter = ast_ddl->as<ASTAlterQuery>())
    {
        // Setting alters should be executed on all replicas
        if (alter->isSettingsAlter() ||
            alter->isFreezeAlter() ||
            alter->isMovePartitionToDiskOrVolumeAlter() ||
            alter->isCommentAlter())
            return false; // 上面的alter类型应该在所有节点上执行
    }
    // 其他ALTER（如增加列，修改列类型，增加索引等结构变更），返回true，表示只在Leader上执行
    // 如果这个表是 ReplicatedMergeTree 或其他支持复制的引擎，才允许只在 leader 上执行。
    // 目前只有 StorageReplicatedMergeTree 支持 replication
    return storage->supportsReplication();
}

/**
 * 新版设计中，所有副本都可以被认为是“leader”——这是为了避免因为 leader 选举导致单点瓶颈，提升可用性和并行度。
 * 但是为了分布式 DDL 操作时防止冲突，ClickHouse 又引入了基于 ZooKeeper 的分布式锁机制来“虚拟”选出唯一的执行者。
 * 所以，这个方法的命名似乎是legacy的，准确的理解是：只在Shard的某一个Replica上执行，而不是在Shard的所有replica上执行
 */
bool DDLWorker::tryExecuteQueryOnLeaderReplica(
    DDLTaskBase & task,
    StoragePtr storage, // 只有StorageReplicatedMergeTree 才能执行分布式ddl
    const String & /*node_path*/,
    const ZooKeeperPtr & zookeeper,
    std::unique_ptr<zkutil::ZooKeeperLock> & execute_on_leader_lock)
{
    StorageReplicatedMergeTree * replicated_storage = dynamic_cast<StorageReplicatedMergeTree *>(storage.get());

    /// If we will develop new replicated storage
    if (!replicated_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage type '{}' is not supported by distributed DDL", storage->getName());
    // fs::path(entry_path) / "shards" / getShardID()， 这里的shardId并不是replica id， 必须区分开
    String shard_path = task.getShardNodePath();
    // 这个task是否已经被执行，如果存在这个节点，节点中存放了执行者replica
    // 注意，这个路径是创建在 shard_path下面的
    String is_executed_path = fs::path(shard_path) / "executed";
    // 开始尝试执行，创建这个节点的时候，其中存放了执行的attempt次数
    String tries_to_execute_path = fs::path(shard_path) / "tries_to_execute";
    assert(shard_path.starts_with(String(fs::path(task.entry_path) / "shards" / "")));
    zookeeper->createIfNotExists(fs::path(task.entry_path) / "shards", "");
    zookeeper->createIfNotExists(shard_path, "");

    /// Leader replica creates is_executed_path node on successful query execution.
    /// We will remove create_shard_flag from zk operations list, if current replica is just waiting for leader to execute the query.
    auto create_shard_flag = zkutil::makeCreateRequest(is_executed_path, task.host_id_str, zkutil::CreateMode::Persistent);

    /// Node exists, or we will create or we will get an exception
    // 这个节点或者存在，如果不存在，则初始化为0，后续执行者每次尝试执行的时候都不断递增
    zookeeper->tryCreate(tries_to_execute_path, "0", zkutil::CreateMode::Persistent);

    static constexpr int MAX_TRIES_TO_EXECUTE = 3;
    static constexpr int MAX_EXECUTION_TIMEOUT_SEC = 3600;

    String executed_by;

    zkutil::EventPtr event = std::make_shared<Poco::Event>();
    /// We must use exists request instead of get, because zookeeper will not setup event
    /// for non existing node after get request
    // 在 is_executed_path上进行监听，这样，当task执行完成(可能是自己执行完成的，但是无所谓)，就会收到通知
    // 很可能这个节点当前还不存在，这时候通过exists()也照样能创建监听，而用get()的话，假如节点不存在，则无法创建监听
    if (zookeeper->exists(is_executed_path, nullptr, event))
    {
        // is_executed_path已经存在，说明任务已经被其他的replica执行了
        LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, zookeeper->get(is_executed_path));
        if (auto op = task.getOpToUpdateLogPointer())
            task.ops.push_back(op);
        return true;
    }

    pcg64 rng(randomSeed());
    // 创建一个基于Zookeeper的分布式锁，创建成功以后，写入自己的id，即host_id_str
    execute_on_leader_lock = createSimpleZooKeeperLock(zookeeper, shard_path, "lock", task.host_id_str);

    Stopwatch stopwatch;

    bool executed_by_us = false;
    bool executed_by_other_leader = false;

    bool extra_attempt_for_replicated_database = false;

    /// Defensive programming. One hour is more than enough to execute almost all DDL queries.
    /// If it will be very long query like ALTER DELETE for a huge table it's still will be executed,
    /// but DDL worker can continue processing other queries.
    while (stopwatch.elapsedSeconds() <= MAX_EXECUTION_TIMEOUT_SEC)
    {
        ReplicatedTableStatus status;
        // Has to get with zk fields to get active replicas field
        // 根据当前的 StorageReplicatedMergeTree，设置ReplicatedTableStatus
        // 这里，只要当前的 Replica是leader，那么status.is_leader=true，
        // 并且根据新版本的ClickHouse的设置，所有的replica在正常情况下都是leader,
        // 对于tryExecuteQueryOnLeaderReplica，
        replicated_storage->getStatus(status, true);

        // Should return as soon as possible if the table is dropped or detached, so we will release StoragePtr
        bool replica_dropped = storage->is_dropped;
        bool all_replicas_likely_detached = status.active_replicas == 0 && !DatabaseCatalog::instance().isTableExist(storage->getStorageID(), context);
        if (replica_dropped || all_replicas_likely_detached)
        {
            /// We have to exit (and release StoragePtr) if the replica is being restarted,
            /// but we can retry in this case, so don't write execution status
            if (storage->is_being_restarted)
                throw Exception(ErrorCodes::UNFINISHED, "Cannot execute replicated DDL query, table is dropped or detached permanently");
            LOG_WARNING(log, ", task {} will not be executed.", task.entry_name);
            task.execution_status = ExecutionStatus(ErrorCodes::UNFINISHED, "Cannot execute replicated DDL query, table is dropped or detached permanently");
            return false;
        }

        if (task.is_initial_query && !status.is_leader)
            throw Exception(ErrorCodes::NOT_A_LEADER, "Cannot execute initial query on non-leader replica");

        /// Any replica which is leader tries to take lock
        // 这里的is_leader并不代表已经被选为即将执行这个SQL的节点，只是说当前的replica是leader replica(ClickHouse允许多leader)
        // 只有是leader 的replica，才有可能参与后续的执行权的竞争
        // 这里的execute_on_leader_lock是基于zookeeper的分布式锁
        if (status.is_leader && execute_on_leader_lock->tryLock())
        {
            /// In replicated merge tree we can have multiple leaders. So we can
            /// be "leader" and took lock, but another "leader" replica may have
            /// already executed this task.
            if (zookeeper->tryGet(is_executed_path, executed_by))
            {
                LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, executed_by);
                executed_by_other_leader = true;
                if (auto op = task.getOpToUpdateLogPointer())
                    task.ops.push_back(op);
                break;
            }

            /// Checking and incrementing counter exclusively.
            size_t counter = parse<int>(zookeeper->get(tries_to_execute_path));
            if (counter > MAX_TRIES_TO_EXECUTE) // 执行次数超过了 MAX_TRIES_TO_EXECUTE
            {
                /// Replicated databases have their own retries, limiting retries here would break outer retries
                bool is_replicated_database_task = dynamic_cast<DatabaseReplicatedTask *>(&task);
                if (is_replicated_database_task) // 如果这是一个DatabaseReplicatedTask，不是普通的ReplicatedTask
                    extra_attempt_for_replicated_database = true;
                else
                    break;
            }
            // tries_to_execute_path 路径上的计数器+1
            zookeeper->set(tries_to_execute_path, toString(counter + 1));

            task.ops.push_back(create_shard_flag); // 既然自己准备执行，那么就将对应的is_executed_path创建请求放入ops中
            SCOPE_EXIT_MEMORY({ if (!executed_by_us && !task.ops.empty()) task.ops.pop_back(); });

            /// If the leader will unexpectedly changed this method will return false
            /// and on the next iteration new leader will take lock
            if (tryExecuteQuery(task, zookeeper)) // 执行
            {
                executed_by_us = true; // 执行成功，退出
                break;
            }
            else if (extra_attempt_for_replicated_database)
                break; //  如果这是一个DatabaseReplicatedTask，不是普通的ReplicatedTask那么跳出循环，因为DatabaseReplicated自己会有重试机制
        }
        // 如果是自己拿到锁并执行成功了，代码不会走到这里，而是直接break了
        /// Waiting for someone who will execute query and change is_executed_path node
        if (event->tryWait(std::uniform_int_distribution<int>(0, 1000)(rng)))
        {
            // 任务已经被执行成功了，因为已经拿到了 is_executed_path 节点的event
            LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, zookeeper->get(is_executed_path));
            executed_by_other_leader = true;
            if (auto op = task.getOpToUpdateLogPointer())
                task.ops.push_back(op);
            break;
        }
        else
        {
            // 等了好久，还是没有等到is_executed_path节点出现，说明还是没执行成功
            String tries_count;
            zookeeper->tryGet(tries_to_execute_path, tries_count);
            if (parse<int>(tries_count) > MAX_TRIES_TO_EXECUTE)
            {
                /// Nobody will try to execute query again
                LOG_WARNING(log, "Maximum retries count for task {} exceeded, cannot execute replicated DDL query", task.entry_name);
                break; // 不再尝试，其它节点拿到的事相同的tries_count，也不会再尝试
            }
            else
            {
                /// Will try to wait or execute
                LOG_TRACE(log, "Task {} still not executed, will try to wait for it or execute ourselves, tries count {}", task.entry_name, tries_count);
            }
        }
    }
    // 执行到这里，说明经过重试，task依然不是一个正确的、完成执行的状态
    //  executed_by_us 和 executed_by_other_leader不可能同时为true
    chassert(!(executed_by_us && executed_by_other_leader));

    /// Not executed by leader so was not executed at all
    if (!executed_by_us && !executed_by_other_leader)
    {
        // 如果既没有被自己也没有被别的leader执行
        /// If we failed with timeout
        if (stopwatch.elapsedSeconds() >= MAX_EXECUTION_TIMEOUT_SEC)
        {
            LOG_WARNING(log, "Task {} was not executed by anyone, maximum timeout {} seconds exceeded", task.entry_name, MAX_EXECUTION_TIMEOUT_SEC);
            task.execution_status = ExecutionStatus(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot execute replicated DDL query, timeout exceeded");
        }
        else /// If we exceeded amount of tries
        {
            LOG_WARNING(log, "Task {} was not executed by anyone, maximum number of retries exceeded", task.entry_name);
            bool keep_original_error = extra_attempt_for_replicated_database && task.execution_status.code;
            if (!keep_original_error)
                task.execution_status = ExecutionStatus(ErrorCodes::UNFINISHED, "Cannot execute replicated DDL query, maximum retries exceeded");
        }
        return false;
    }

    if (executed_by_us) // 自己执行的
        LOG_DEBUG(log, "Task {} executed by current replica", task.entry_name);
    else // if (executed_by_other_leader) // 别人执行的
        LOG_DEBUG(log, "Task {} has already been executed by replica ({}) of the same shard.", task.entry_name, zookeeper->get(is_executed_path));

    return true;
}


void DDLWorker::cleanupQueue(Int64, const ZooKeeperPtr & zookeeper)
{
    LOG_DEBUG(log, "Cleaning queue");

    Strings queue_nodes = zookeeper->getChildren(queue_dir);
    filterAndSortQueueNodes(queue_nodes);

    for (auto it = queue_nodes.cbegin(); it < queue_nodes.cend(); ++it)
    {
        if (stop_flag)
            return;

        String node_name = *it;
        String node_path = fs::path(queue_dir) / node_name;

        Coordination::Stat stat;

        try
        {
            /// Already deleted
            if (!zookeeper->exists(node_path, &stat))
                continue;

            if (!canRemoveQueueEntry(node_name, stat))
                continue;

            /// At first we remove entry/active node to prevent staled hosts from executing entry concurrently
            auto rm_active_res = zookeeper->tryRemove(fs::path(node_path) / "active");
            if (rm_active_res != Coordination::Error::ZOK && rm_active_res != Coordination::Error::ZNONODE)
            {
                if (rm_active_res == Coordination::Error::ZNOTEMPTY)
                    LOG_DEBUG(log, "Task {} should be deleted, but there are active workers. Skipping it.", node_name);
                else
                    LOG_WARNING(log, "Unexpected status code {} on attempt to remove {}/active", rm_active_res, node_name);
                continue;
            }

            /// Now we can safely delete entry
            LOG_INFO(log, "Task {} is outdated, deleting it", node_name);

            /// We recursively delete all nodes except node_path/finished to prevent staled hosts from
            /// creating node_path/active node (see createStatusDirs(...))
            zookeeper->tryRemoveChildrenRecursive(node_path, /* probably_flat */ false, zkutil::RemoveException{"finished"});

            /// And then we remove node_path and node_path/finished in a single transaction
            Coordination::Requests ops;
            Coordination::Responses res;
            ops.emplace_back(zkutil::makeCheckRequest(node_path, -1));  /// See a comment below
            ops.emplace_back(zkutil::makeRemoveRequest(fs::path(node_path) / "finished", -1));
            ops.emplace_back(zkutil::makeRemoveRequest(node_path, -1));
            auto rm_entry_res = zookeeper->tryMulti(ops, res);

            if (rm_entry_res == Coordination::Error::ZNONODE)
            {
                /// Most likely both node_path/finished and node_path were removed concurrently.
                bool entry_removed_concurrently = res[0]->error == Coordination::Error::ZNONODE;
                if (entry_removed_concurrently)
                    continue;

                /// Possible rare case: initiator node has lost connection after enqueueing entry and failed to create status dirs.
                /// No one has started to process the entry, so node_path/active and node_path/finished nodes were never created, node_path has no children.
                /// Entry became outdated, but we cannot remove remove it in a transaction with node_path/finished.
                chassert(res[0]->error == Coordination::Error::ZOK && res[1]->error == Coordination::Error::ZNONODE);
                rm_entry_res = zookeeper->tryRemove(node_path);
                chassert(rm_entry_res != Coordination::Error::ZNOTEMPTY);
                continue;
            }
            zkutil::KeeperMultiException::check(rm_entry_res, ops, res);
            entries_to_skip.remove(node_name);
        }
        catch (...)
        {
            LOG_INFO(log, "An error occurred while checking and cleaning task {} from queue: {}", node_name, getCurrentExceptionMessage(false));
        }
    }
}

bool DDLWorker::canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat)
{
    /// Delete node if its lifetime is expired (according to task_max_lifetime parameter)
    constexpr UInt64 zookeeper_time_resolution = 1000;
    Int64 zookeeper_time_seconds = stat.ctime / zookeeper_time_resolution;
    bool node_lifetime_is_expired = zookeeper_time_seconds + task_max_lifetime < Poco::Timestamp().epochTime();

    /// If too many nodes in task queue (> max_tasks_in_queue), delete oldest one
    UInt32 entry_number = DDLTaskBase::getLogEntryNumber(entry_name);
    bool node_is_outside_max_window = entry_number + max_tasks_in_queue < max_id.load(std::memory_order_relaxed);

    return node_lifetime_is_expired || node_is_outside_max_window;
}

/// Try to create nonexisting "status" dirs for a node
void DDLWorker::createStatusDirs(const std::string & node_path, const ZooKeeperPtr & zookeeper)
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(fs::path(node_path) / "active", {}, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(fs::path(node_path) / "finished", {}, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    bool both_created = code == Coordination::Error::ZOK;

    /// Failed on attempt to create node_path/active because it exists, so node_path/finished must exist too
    bool both_already_exists = responses.size() == 2 && responses[0]->error == Coordination::Error::ZNODEEXISTS
                                                     && responses[1]->error == Coordination::Error::ZRUNTIMEINCONSISTENCY;
    assert(!both_already_exists || (zookeeper->exists(fs::path(node_path) / "active") && zookeeper->exists(fs::path(node_path) / "finished")));

    /// Failed on attempt to create node_path/finished, but node_path/active does not exist
    bool is_currently_deleting = responses.size() == 2 && responses[0]->error == Coordination::Error::ZOK
                                                       && responses[1]->error == Coordination::Error::ZNODEEXISTS;
    if (both_created || both_already_exists)
        return;

    if (is_currently_deleting)
    {
        cleanup_event->set();
        throw Exception(ErrorCodes::UNFINISHED, "Cannot create status dirs for {}, "
                        "most likely because someone is deleting it concurrently", node_path);
    }

    /// Connection lost or entry was removed
    assert(Coordination::isHardwareError(code) || code == Coordination::Error::ZNONODE);
    zkutil::KeeperMultiException::check(code, ops, responses);
}

/**
 * 这是virtual方法，在DatabaseReplicatedDDLWorker::enqueueQuery重载了该方法
 * @param entry
 * @return
 */
String DDLWorker::enqueueQuery(DDLLogEntry & entry)
{
    if (entry.hosts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty host list in a distributed DDL task");

    auto zookeeper = getAndSetZooKeeper();

    String query_path_prefix = fs::path(queue_dir) / "query-";
    zookeeper->createAncestors(query_path_prefix);

    // 这里的entry的文件名字是根据 zkutil::CreateMode::PersistentSequential自动生成的
    String node_path = zookeeper->create(query_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential);
    if (max_pushed_entry_metric)
    {
        String str_buf = node_path.substr(query_path_prefix.length());
        DB::ReadBufferFromString in(str_buf);
        CurrentMetrics::Value pushed_entry;
        readText(pushed_entry, in);
        pushed_entry = std::max(CurrentMetrics::get(*max_pushed_entry_metric), pushed_entry);
        CurrentMetrics::set(*max_pushed_entry_metric, pushed_entry);
    }

    /// We cannot create status dirs in a single transaction with previous request,
    /// because we don't know node_path until previous request is executed.
    /// Se we try to create status dirs here or later when we will execute entry.
    try
    {
        createStatusDirs(node_path, zookeeper);
    }
    catch (...)
    {
        LOG_INFO(log, "An error occurred while creating auxiliary ZooKeeper directories in {} . They will be created later. Error : {}", node_path, getCurrentExceptionMessage(true));
    }

    return node_path;
}


bool DDLWorker::initializeMainThread()
{
    chassert(!initialized);
    setThreadName("DDLWorker");
    LOG_DEBUG(log, "Initializing DDLWorker thread");

    while (!stop_flag)
    {
        try
        {
            auto zookeeper = getAndSetZooKeeper();
            zookeeper->createAncestors(fs::path(queue_dir) / "");
            initialized = true; // 成功完成了初始化，有可能是第一次初始化，有可能是连接断开以后的初始化
            return true; // 初始化成功
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
            {
                /// A logical error.
                LOG_ERROR(log, "ZooKeeper error: {}. Failed to start DDLWorker.", getCurrentExceptionMessage(true));
                chassert(false);  /// Catch such failures in tests with debug build
            }

            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot initialize DDL queue.");
        }

        /// Avoid busy loop when ZooKeeper is not available.
        sleepForSeconds(5); //失败以后返回重试
    }

    return false;
}

void DDLWorker::runMainThread()
{
    // 重置状态的回调函数，严重错误下的完全重置
    auto reset_state = [&]()
    {
        initialized = false;
        /// It will wait for all threads in pool to finish and will not rethrow exceptions (if any).
        /// We create new thread pool to forget previous exceptions.
        if (1 < pool_size) // 重新创建Thread Pool
            worker_pool = std::make_unique<ThreadPool>(CurrentMetrics::DDLWorkerThreads, CurrentMetrics::DDLWorkerThreadsActive, CurrentMetrics::DDLWorkerThreadsScheduled, pool_size);
        /// Clear other in-memory state, like server just started.
        current_tasks.clear(); // 所有的内存中的任务清空
        last_skipped_entry_name.reset();
        max_id = 0; // 最大id置位0
        LOG_INFO(log, "Cleaned DDLWorker state");
    };

    setThreadName("DDLWorker");
    LOG_DEBUG(log, "Starting DDLWorker thread");

    while (!stop_flag) // 只有没有收到停止信号，则反复执行
    {
        try
        {
            /**
             * 如果 initialized == false ⇒ 表示刚刚经历过重启或 ZooKeeper 断连重连，因此还处于一个没有完全初始化完成的稳定状态
             * 此时， reinitialized == true ⇒ 这轮调度是一次重启恢复之后的调度，我们要特别小心处理“未完成的任务”。
            */
            bool reinitialized = !initialized;

            /// Reinitialize DDLWorker state (including ZooKeeper connection) if required
            // 只要initialized为false，表示现在需要进行zookeeper的重新初始化才能开始任务的收集和执行
            // 如果initialized=false，则initializeMainThread()会不断反复重试直到成功
            if (!initialized) // 如果刚刚经历过reset，还没有完成初始化
            {
                /// Stopped
                // 尝试进行重新的初始化，返回false，表示重新初始化失败，或者收到了stop_flag
                // 如果返回true，代表初始化成功，因此在initializeMainThread()中会把initialized置为true，代表已经进行了成功初始化
                if (!initializeMainThread())
                    break;
                LOG_DEBUG(log, "Initialized DDLWorker thread");
            }
            //  如果initialized=false，则initializeMainThread()会不断反复重试直到成功，
            // 因此代码执行到这里，一定是已经重试成功了，即如果初始化不成功，就无法执行scheduleTasks
            // reinitialized的含义是：刚刚是否从一次失败中刚刚恢复过来，如果的确是从一次失败中刚刚恢复过来，那么需要对这些task进行一些特殊处理
            cleanup_event->set();
            scheduleTasks(reinitialized); // 进行任务的收集和调度，这里的reinitialized的含义是： 是否刚刚完成了一次新的initialize操作
            subsequent_errors_count = 0;

            LOG_DEBUG(log, "Waiting for queue updates");
            queue_updated_event->wait(); //等待新的任务，而不是反复循环重试
        }
        catch (const Coordination::Exception & e)
        {
            subsequent_errors_count = 0;
            // 如果是硬件类 ZooKeeper 错误（如 TCP 断连、session expired）， 那么无需reset_state，而是重建线程池，然后在下一次进行尝试继续
            if (Coordination::isHardwareError(e.code))
            {
                initialized = false; // 将initialized置为false，表示需要再次进行初始化，但是这个初始化不会重置(清空)任务
                /// Wait for pending async tasks
                if (1 < pool_size)
                    worker_pool = std::make_unique<ThreadPool>(CurrentMetrics::DDLWorkerThreads, CurrentMetrics::DDLWorkerThreadsActive, CurrentMetrics::DDLWorkerThreadsScheduled, pool_size);
                LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}", getCurrentExceptionMessage(true));
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error, will try to restart main thread: {}", getCurrentExceptionMessage(true));
                reset_state(); // 重置整个系统状态，说明是严重的失联
            }
            sleepForSeconds(1);
        }
        catch (...)
        {
            String message = getCurrentExceptionMessage(/*with_stacktrace*/ true);
            if (subsequent_errors_count)
            {
                if (last_unexpected_error == message)
                {
                    ++subsequent_errors_count;
                }
                else
                {
                    subsequent_errors_count = 1;
                    last_unexpected_error = message;
                }
            }
            else
            {
                subsequent_errors_count = 1;
                last_unexpected_error = message;
            }

            LOG_ERROR(log, "Unexpected error ({} times in a row), will try to restart main thread: {}", subsequent_errors_count, message);

            /// Sleep before retrying
            sleepForSeconds(5);
            /// Reset state after sleeping, so DatabaseReplicated::canExecuteReplicatedMetadataAlter()
            /// will have a chance even when the database got stuck in infinite retries
            reset_state();
        }
    }
}


void DDLWorker::runCleanupThread()
{
    setThreadName("DDLWorkerClnr");
    LOG_DEBUG(log, "Started DDLWorker cleanup thread");

    Int64 last_cleanup_time_seconds = 0;
    while (!stop_flag)
    {
        try
        {
            cleanup_event->wait();
            if (stop_flag)
                break;

            Int64 current_time_seconds = Poco::Timestamp().epochTime();
            if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_delay_period)
            {
                LOG_TRACE(log, "Too early to clean queue, will do it later.");
                continue;
            }

            /// ZooKeeper connection is recovered by main thread. We will wait for it on cleanup_event.
            auto zookeeper = tryGetZooKeeper();
            if (zookeeper->expired())
                continue;

            cleanupQueue(current_time_seconds, zookeeper);
            last_cleanup_time_seconds = current_time_seconds;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

}
