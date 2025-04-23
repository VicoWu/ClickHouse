#include <Processors/Transforms/buildPushingToViewsChain.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageValues.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <base/defines.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>


namespace ProfileEvents
{
    extern const Event SelectedBytes;
    extern const Event SelectedRows;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool deduplicate_blocks_in_dependent_materialized_views;
    extern const SettingsBool ignore_materialized_views_with_dropped_target_table;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool log_queries;
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsBool log_query_views;
    extern const SettingsBool materialized_views_ignore_errors;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_bytes_for_materialized_views;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_rows_for_materialized_views;
    extern const SettingsBool parallel_view_processing;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool use_async_executor_for_materialized_views;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

ThreadStatusesHolder::~ThreadStatusesHolder()
{
    auto * original_thread = current_thread;
    SCOPE_EXIT({ current_thread = original_thread; });

    while (!thread_statuses.empty())
    {
        current_thread = thread_statuses.front().get();
        thread_statuses.pop_front();
    }
}

/**
 * ClickHouse 中 buildPushingToViewsChain 所依赖的一个核心数据结构 —— ViewsData。
 * 它主要用来封装多个 Materialized View 写入过程中的上下文信息和状态。
 */
struct ViewsData
{
    /// A separate holder for thread statuses, needed for proper destruction order.
    /**
     * 持有所有子线程的 ThreadStatus 对象，目的是为了确保 在所有线程销毁后再销毁 pipeline 相关资源。
     * 每个 view 可能在自己的线程中执行，因此需要统一的 holder 管理这些状态。
     */
    ThreadStatusesHolderPtr thread_status_holder;
    /// Separate information for every view.
    /**
     * 一个列表，保存了每个 View 的运行时信息。
     * view 的名字
     * 目标表的 sink 信息
     * 执行时统计数据
     * pipeline input/output 端口等
     */
    std::list<ViewRuntimeData> views;
    /// Some common info about source storage.
    ContextPtr context;
    // 最初执行 insert 的那张表，通常是 Kafka 表。
    StorageID source_storage_id;
    StorageMetadataPtr source_metadata_snapshot;
    // Kafka 表的 Storage 对象，用于处理插入时的某些行为。
    StoragePtr source_storage;
    size_t max_threads = 1;

    /// In case of exception happened while inserting into main table, it is pushed to pipeline.
    /// Remember the first one, we should keep them after view processing.
    std::atomic_bool has_exception = false;
    std::exception_ptr first_exception;

    ViewsData(ThreadStatusesHolderPtr thread_status_holder_, ContextPtr context_, StorageID source_storage_id_, StorageMetadataPtr source_metadata_snapshot_, StoragePtr source_storage_)
        : thread_status_holder(std::move(thread_status_holder_))
        , context(std::move(context_))
        , source_storage_id(std::move(source_storage_id_))
        , source_metadata_snapshot(std::move(source_metadata_snapshot_))
        , source_storage(std::move(source_storage_))
    {
    }
};

using ViewsDataPtr = std::shared_ptr<ViewsData>;

/// Copies data inserted into table for every dependent table.
class CopyingDataToViewsTransform final : public IProcessor
{
public:
    CopyingDataToViewsTransform(const Block & header, ViewsDataPtr data, size_t view_level_);

    String getName() const override { return "CopyingDataToViewsTransform"; }
    Status prepare() override;
    InputPort & getInputPort() { return input; }

private:
    InputPort & input;
    ViewsDataPtr views_data;
    size_t view_level;
};

/// For source chunk, execute view query over it.
template <typename Executor>
class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(const Block & header, ViewRuntimeData & view_, ViewsDataPtr views_data_, bool disable_deduplication_for_children_);

    String getName() const override { return "ExecutingInnerQueryFromView"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

private:
    ViewsDataPtr views_data;
    ViewRuntimeData & view;
    bool disable_deduplication_for_children;

    struct State
    {
        QueryPipeline pipeline;
        Executor executor;

        explicit State(QueryPipeline pipeline_)
            : pipeline(std::move(pipeline_))
            , executor(pipeline)
        {
        }
    };

    std::optional<State> state;
};

/// Insert into LiveView.
class PushingToLiveViewSink final : public SinkToStorage
{
public:
    PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, StoragePtr storage_holder_, ContextPtr context_);
    String getName() const override { return "PushingToLiveViewSink"; }
    void consume(Chunk & chunk) override;

private:
    StorageLiveView & live_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

/// Insert into WindowView.
class PushingToWindowViewSink final : public SinkToStorage
{
public:
    PushingToWindowViewSink(const Block & header, StorageWindowView & window_view_, StoragePtr storage_holder_, ContextPtr context_);
    String getName() const override { return "PushingToWindowViewSink"; }
    void consume(Chunk & chunk) override;

private:
    StorageWindowView & window_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

/// For every view, collect exception.
/// Has single output with empty header.
/// If any exception happen before view processing, pass it.
/// Othervise return any exception from any view.
class FinalizingViewsTransform final : public IProcessor
{
    struct ExceptionStatus
    {
        std::exception_ptr exception;
        bool is_first = false;
    };

    static InputPorts initPorts(std::vector<Block> headers);

public:
    FinalizingViewsTransform(std::vector<Block> headers, ViewsDataPtr data);

    String getName() const override { return "FinalizingViewsTransform"; }
    Status prepare() override;
    void work() override;

private:
    OutputPort & output;
    ViewsDataPtr views_data;
    std::vector<ExceptionStatus> statuses;
    std::exception_ptr any_exception;
};

/**
 * 为某个物化视图 view_id 构造一个处理链 Chain，这个链最终会把插入的数据送入物化视图背后的表。
 * 调用者是 Chain buildPushingToViewsChain(
 */
/// Generates one chain part for every view in buildPushingToViewsChain
std::optional<Chain> generateViewChain(
    ContextPtr context,
    size_t view_level,
    const StorageID & view_id,
    ThreadGroupPtr running_group,
    Chain & result_chain,
    ViewsDataPtr views_data,
    ThreadStatusesHolderPtr thread_status_holder,
    bool async_insert,
    const Block & storage_header,
    bool disable_deduplication_for_children)
{
    auto view = DatabaseCatalog::instance().tryGetTable(view_id, context);
    if (view == nullptr)
    {
        LOG_WARNING(
            getLogger("PushingToViews"), "Trying to access table {} but it doesn't exist", view_id.getFullTableName());
        return std::nullopt;
    }
    // 从DatabaseCatalog::instance()中获取到的View信息
    auto view_metadata_snapshot = view->getInMemoryMetadataPtr();
    auto select_context = view_metadata_snapshot->getSQLSecurityOverriddenContext(context);
    select_context->setQueryAccessInfo(context->getQueryAccessInfoPtr());

    auto insert_context = Context::createCopy(select_context);

    const auto & insert_settings = insert_context->getSettingsRef();

    if (disable_deduplication_for_children)
    {
        insert_context->setSetting("insert_deduplicate", Field{false});
    }

    // Processing of blocks for MVs is done block by block, and there will
    // be no parallel reading after (plus it is not a costless operation)
    select_context->setSetting("parallelize_output_from_storages", Field{false});

    // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
    if (insert_settings[Setting::min_insert_block_size_rows_for_materialized_views])
        insert_context->setSetting("min_insert_block_size_rows", insert_settings[Setting::min_insert_block_size_rows_for_materialized_views].value);
    if (insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views])
        insert_context->setSetting(
            "min_insert_block_size_bytes", insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views].value);

    ASTPtr query;
    Chain out;

    /// We are creating a ThreadStatus per view to store its metrics individually
    /// Since calling ThreadStatus() changes current_thread we save it and restore it after the calls
    /// Later on, before doing any task related to a view, we'll switch to its ThreadStatus, do the work,
    /// and switch back to the original thread_status.
    auto * original_thread = current_thread;
    SCOPE_EXIT({ current_thread = original_thread; });
    current_thread = nullptr;
    // 每个视图有自己专属的 ThreadStatus，用于记录自己的性能和异常
    std::unique_ptr<ThreadStatus> view_thread_status_ptr = std::make_unique<ThreadStatus>(/*check_current_thread_on_destruction=*/ false);
    /// Copy of a ThreadStatus should be internal.
    view_thread_status_ptr->setInternalThread();
    // 视图会加入到主线程组中（父插入语句）
    view_thread_status_ptr->attachToGroup(running_group);

    auto * view_thread_status = view_thread_status_ptr.get();
    views_data->thread_status_holder->thread_statuses.push_front(std::move(view_thread_status_ptr));

    auto runtime_stats = std::make_unique<QueryViewsLogElement::ViewRuntimeStats>();
    runtime_stats->target_name = view_id.getFullTableName();
    runtime_stats->thread_status = view_thread_status;
    runtime_stats->event_time = std::chrono::system_clock::now();
    runtime_stats->event_status = QueryViewsLogElement::ViewStatus::EXCEPTION_BEFORE_START;

    auto & type = runtime_stats->type;
    auto & target_name = runtime_stats->target_name;
    auto * view_counter_ms = &runtime_stats->elapsed_ms;
    // 如果是Kafka -> mv -> MergeTree的这种 StorageMaterializedView
    if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(view.get()))
    {
        LOG_DEBUG(getLogger("PushingToViews"), "Running generateViewChain. "
                                               "Current type is StorageMaterializedView. View Name {}, target Table {}",
                  materialized_view->getName(), materialized_view->getTargetTable()->getName());
        // 对物化视图本身加读锁
        auto lock = materialized_view->tryLockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);

        if (lock == nullptr)
        {
            // In case the materialized view is dropped/detached at this point, we register a warning and ignore it
            assert(materialized_view->is_dropped || materialized_view->is_detached);
            LOG_WARNING(
                getLogger("PushingToViews"), "Trying to access table {} but it doesn't exist", view_id.getFullTableName());
            return std::nullopt;
        }

        type = QueryViewsLogElement::ViewType::MATERIALIZED;
        result_chain.addTableLock(lock);
        // 获取视图背后的物理表（一般是 MergeTree 表，也有可能还是MV表等）
        StoragePtr inner_table = materialized_view->tryGetTargetTable();
        /// If target table was dropped, ignore this materialized view.
        if (!inner_table)
        {
            if (context->getSettingsRef()[Setting::ignore_materialized_views_with_dropped_target_table])
                return std::nullopt;

            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Target table '{}' of view '{}' doesn't exists. To ignore this view use setting "
                "ignore_materialized_views_with_dropped_target_table",
                materialized_view->getTargetTableId().getFullTableName(),
                view_id.getFullTableName());
        }

        auto inner_table_id = inner_table->getStorageID();
        auto inner_metadata_snapshot = inner_table->getInMemoryMetadataPtr();
        // 从DatabaseCatalog::instance()中获取到的View信息保存在select_query中
        const auto & select_query = view_metadata_snapshot->getSelectQuery();
        LOG_DEBUG(getLogger("PushingToViews"), "inner_table_id = {}, inner table name {} "
                                               "select_query.select_table_id {}, "
                                               "source storage name {}, source storage id {},"
                                               " view_data select query is {}, "
                                               "view_data inner query is {},",
                  inner_table_id, inner_table_id.getFullTableName(),
                  select_query.select_table_id, views_data->source_storage->getName(),
                  views_data->source_storage->getStorageID(), select_query.select_query->dumpTree(),
                  select_query.inner_query->dumpTree());
        // 防止视图逻辑已经被改写，不再以当前表为来源。例如用户 ALTER 了视图。
        if (select_query.select_table_id != views_data->source_storage_id)
        {
            // 从DatabaseCatalog::instance()中获取到的View信息已经过时，目前最新的source table信息是 views_data->source_storage_id
            /// It may happen if materialize view query was changed and it doesn't depend on this source table anymore.
            /// See setting `allow_experimental_alter_materialized_view_structure`
            LOG_DEBUG(
                getLogger("PushingToViews"), "Table '{}' is not a source for view '{}' anymore, current source is '{}'",
                select_query.select_table_id.getFullTableName(), view_id.getFullTableName(), views_data->source_storage_id);
            return std::nullopt;
        }
        // 查询(来自Kafka表)
        query = select_query.inner_query;
        // 目标表(来自MergeTreeTable)
        target_name = inner_table_id.getFullTableName();

        Block header;

        /// Get list of columns we get from select query.
        if (select_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            header = InterpreterSelectQueryAnalyzer::getSampleBlock(query, select_context);
        else
            header = InterpreterSelectQuery(query, select_context, SelectQueryOptions()).getSampleBlock();

        /// Insert only columns returned by select.
        Names insert_columns;
        const auto & inner_table_columns = inner_metadata_snapshot->getColumns();
        for (const auto & column : header)
        {
            /// But skip columns which storage doesn't have.
            if (inner_table_columns.hasNotAlias(column.name))
                insert_columns.emplace_back(column.name);
        }

        InterpreterInsertQuery interpreter(
            nullptr,
            insert_context,
            /* allow_materialized */ false,
            /* no_squash */ false, // 需要进行squash
            /* no_destination */ false, //  有dest
            /* async_isnert */ false); //  不是 async_insert

        /// TODO: remove sql_security_type check after we turn `ignore_empty_sql_security_in_create_view_query=false`
        bool check_access = !materialized_view->hasInnerTable() && materialized_view->getInMemoryMetadataPtr()->sql_security_type;
        // 查看方法 InterpreterInsertQuery::buildChain，返回一个chain
        // 这里的inner_table是当前的mv表的目标表，它可能是一个local表，也可能还是一个mv表，如果是一个mv表，那么显然会继续递归调用
        out = interpreter.buildChain(inner_table, view_level + 1, inner_metadata_snapshot, insert_columns, thread_status_holder, view_counter_ms, check_access);
        LOG_DEBUG(getLogger("PushingToViews"), "Already built chain. PushingToViews, interpreter.shouldAddSquashingForStorage(inner_table) : {}",
                  interpreter.shouldAddSquashingForStorage(inner_table));
        if (interpreter.shouldAddSquashingForStorage(inner_table))
        {
            bool table_prefers_large_blocks = inner_table->prefersLargeBlocks();
            const auto & settings = insert_context->getSettingsRef();
            // 将这个Processor添加到这个Chain的头部，作为这个Chain的第一个Processor
            out.addSource(std::make_shared<SquashingTransform>(
                out.getInputHeader(),
                table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL));
        }

#ifdef DEBUG_OR_SANITIZER_BUILD
        out.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Before squashing", out.getInputHeader()));
#endif

        auto counting = std::make_shared<CountingTransform>(out.getInputHeader(), current_thread, insert_context->getQuota());
        counting->setProcessListElement(insert_context->getProcessListElement());
        counting->setProgressCallback(insert_context->getProgressCallback());
        out.addSource(std::move(counting));

        out.addStorageHolder(view);
        out.addStorageHolder(inner_table);
    }
    else if (auto * live_view = dynamic_cast<StorageLiveView *>(view.get())) // 如果是StorageLiveView
    {
        runtime_stats->type = QueryViewsLogElement::ViewType::LIVE;
        query = live_view->getInnerQuery();
        out = buildPushingToViewsChain(
            view, view_metadata_snapshot, insert_context, ASTPtr(),
            view_level + 1,
            /* no_destination= */ true,
            thread_status_holder, running_group, view_counter_ms, async_insert, storage_header);
    }
    else if (auto * window_view = dynamic_cast<StorageWindowView *>(view.get())) // 如果是StorageWindowView
    {
        runtime_stats->type = QueryViewsLogElement::ViewType::WINDOW;
        query = window_view->getMergeableQuery();
        out = buildPushingToViewsChain(
            view, view_metadata_snapshot, insert_context, ASTPtr(),
            view_level + 1,
            /* no_destination= */ true,
            thread_status_holder, running_group, view_counter_ms, async_insert);
    }
    else
        out = buildPushingToViewsChain(
            view, view_metadata_snapshot, insert_context, ASTPtr(),
            view_level + 1,
            /* no_destination= */ false,
            thread_status_holder, running_group, view_counter_ms, async_insert);

    // 向ViewData的views中插入ViewRuntimeData
    views_data->views.emplace_back(ViewRuntimeData{
        std::move(query),
        out.getInputHeader(),
        view_id,
        nullptr,
        std::move(runtime_stats),
        insert_context});

    if (type == QueryViewsLogElement::ViewType::MATERIALIZED)
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        out.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right after Inner query", out.getInputHeader()));
#endif

        if (context->getSettingsRef()[Setting::use_async_executor_for_materialized_views])
        {
             auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingAsyncPipelineExecutor>>(
                 storage_header, views_data->views.back(), views_data, disable_deduplication_for_children);
             executing_inner_query->setRuntimeData(view_thread_status, view_counter_ms);

             out.addSource(std::move(executing_inner_query));
        }
        else
        {

             auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingPipelineExecutor>>(
                 storage_header, views_data->views.back(), views_data, disable_deduplication_for_children);
             executing_inner_query->setRuntimeData(view_thread_status, view_counter_ms);

             out.addSource(std::move(executing_inner_query));

        }

#ifdef DEBUG_OR_SANITIZER_BUILD
        out.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right before Inner query", out.getInputHeader()));
#endif
    }


    return out;
}

/**
 * 调用者是 Chain InterpreterInsertQuery::buildSink(
 * 把 Kafka 表“虚拟 insert”操作，通过中间的 materialized views 转化为写入目标表的执行链。
 */
Chain buildPushingToViewsChain(
    const StoragePtr & storage, // 对应的kafka表
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const ASTPtr & query_ptr,
    size_t view_level, // 在 Chain InterpreterInsertQuery::buildSink(中调用的时候，view_level是0
    bool no_destination,
    ThreadStatusesHolderPtr thread_status_holder,
    ThreadGroupPtr running_group,
    std::atomic_uint64_t * elapsed_counter_ms,
    bool async_insert,
    const Block & live_view_header
)
{
    checkStackSize();
    Chain result_chain;
    // 用于后续追踪和记录执行线程的状态和资源
    ThreadStatus * thread_status = current_thread;

    if (!thread_status_holder)
    {
        thread_status_holder = std::make_shared<ThreadStatusesHolder>();
        thread_status = nullptr;
    }

    /// If we don't write directly to the destination
    /// then expect that we're inserting with precalculated virtual columns
    auto storage_header = no_destination ? metadata_snapshot->getSampleBlockWithVirtuals(storage->getVirtualsList())
                                         : metadata_snapshot->getSampleBlock();

    /** TODO This is a very important line. At any insertion into the table one of chains should own lock.
      * Although now any insertion into the table is done via PushingToViews chain,
      *  but it's clear that here is not the best place for this functionality.
      */
    result_chain.addTableLock(storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]));

    bool disable_deduplication_for_children = !context->getSettingsRef()[Setting::deduplicate_blocks_in_dependent_materialized_views];
    // 获取对应的kafka表的table id
    auto table_id = storage->getStorageID();
    // 或者一张Kafka表所依赖的所有的view，可能有多个
    auto views = DatabaseCatalog::instance().getDependentViews(table_id);

    auto log = getLogger("buildPushingToViewsChain");
    LOG_TEST(log, "Views: {}", views.size());

    if (no_destination && views.empty())
        LOG_WARNING(log, "No views attached and no_destination = 1");
    ViewsDataPtr views_data;
    if (!views.empty())
    {
        auto process_context = Context::createCopy(context);  /// This context will be used in `process` function
        views_data = std::make_shared<ViewsData>(thread_status_holder, process_context, table_id, metadata_snapshot, storage);
    }
    LOG_DEBUG(log, "Chain buildPushingToViewsChain, table_id {}, views size {}, no_destination {}, async_insert {}",
              storage->getStorageID(), views.size(), no_destination, async_insert);
    // 为每一个View构造一个独立的Chain，形成一个vector<Chain> chains
    std::vector<Chain> chains;
    for (const auto & view_id : views)
    {
        try
        {
            /**
             * 每个 View 会构造一个独立的 Chain： 每个 Chain 包含：filter -> projection -> sink 等变换步骤
               最终 sink 通常是写入 ReplicatedMergeTree
               执行过程中会往ViewsData中插入对应View的ViewRuntimeData
             */
            auto out = generateViewChain(
                context, view_level, view_id, running_group, result_chain,
                views_data, thread_status_holder, async_insert, storage_header, disable_deduplication_for_children);

            LOG_DEBUG(log, "Generated chain with view_level {}, view_id {}, "
                           "source_storage name is {}, source-storage full name {}, "
                           "storage id {}, "
                           "storage full table name {}, no_destination {}, async_insert {}",
                      view_level, view_id, views_data->source_storage->getName(),
                      views_data->source_storage->getStorageID().getFullTableName(),
                      storage->getName(), storage->getStorageID().getFullTableName(), no_destination, async_insert);
            if (!out.has_value())
                continue;

            chains.emplace_back(std::move(*out));

            /// Add the view to the query access info so it can appear in system.query_log
            /// hasQueryContext - for materialized tables with background replication process query context is not added
            if (!no_destination && context->hasQueryContext())
            {
                context->getQueryContext()->addQueryAccessInfo(
                    backQuoteIfNeed(view_id.getDatabaseName()),
                    views_data->views.back().runtime_stats->target_name,
                    /*column_names=*/ {});

                context->getQueryContext()->addViewAccessInfo(view_id.getFullTableName());
            }
        }
        catch (const Poco::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("PushingToViews"), "Failed to push block to view {}, {}", view_id, e.message());
            if (!context->getSettingsRef()[Setting::materialized_views_ignore_errors])
                throw;
        }
    }


    /**
     * +---------------------+
       |                     |
       | CopyingDataTransform|  <- 把数据复制给多个视图的入口
       |                     |
       +---------------------+
                 |
       +---------------+-----------------+-------------+
       |               |                 |             |
   +---------+     +---------+       +---------+     +---------+
   | View A  |     | View B  |  ...  | View N  |     (多个视图，每个一个 Chain)
   +---------+     +---------+       +---------+
       |               |                 |
       +---------------+-----------------+-------------+
                 |
       +----------------------+
       | FinalizingViewsTransf|  <- 收集每个视图的运行状态/错误
       +----------------------+

     */
    // 只要有一个view存在
    if (views_data && !views_data->views.empty())
    {
        // view的数量
        size_t num_views = views_data->views.size();
        const Settings & settings = context->getSettingsRef();
        if (settings[Setting::parallel_view_processing])
            views_data->max_threads = settings[Setting::max_threads] ? std::min(static_cast<size_t>(settings[Setting::max_threads]), num_views) : num_views;

        std::vector<Block> headers;
        headers.reserve(num_views);
        for (const auto & chain : chains)
            headers.push_back(chain.getOutputHeader());
        /**
         * 这一步把所有子视图链收敛到一起，形成一个总的 chain。
         * copying_data放在最前面，finalizing_views放在最后面，每一个View对应的chain放在中间
         */
        auto copying_data = std::make_shared<CopyingDataToViewsTransform>(storage_header, views_data, view_level);
        auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(headers), views_data);
        auto out = copying_data->getOutputs().begin();// copying_data的output会重复输出给所有的views_data
        auto in = finalizing_views->getInputs().begin(); // 所有views_data的view的output会集中给finalizing_views的每一个view，由finalizing_views进行汇总

        size_t max_parallel_streams = 0;

        std::list<ProcessorPtr> processors;
        // 遍历每一个view对应的chain
        for (auto & chain : chains)
        {
            max_parallel_streams += std::max<size_t>(chain.getNumThreads(), 1);
            result_chain.attachResources(chain.detachResources());
            connect(*out, chain.getInputPort());
            connect(chain.getOutputPort(), *in);
            ++in; // finalizing_views的下一个input，切换到下一个view
            ++out; // copying_data的下一个output，切换到下一个view
            // 把每个 chain 中的所有 Processor，从它内部拿出来，然后拼接到我们总的 processors 列表的末尾。
            processors.splice(processors.end(), Chain::getProcessors(std::move(chain)));
        }

        processors.emplace_front(std::move(copying_data));
        processors.emplace_back(std::move(finalizing_views));
        result_chain = Chain(std::move(processors)); // 遍历完了所有的processors，把这些Processor组成一个最终的Chain
        result_chain.setNumThreads(std::min(views_data->max_threads, max_parallel_streams));
        result_chain.setConcurrencyControl(settings[Setting::use_concurrency_control]);
    }

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        LOG_DEBUG(log, "Generated StorageLiveView chain with view_level {}, "
                       "source_storage name is {}, source-storage full name {}, "
                       "storage id {}, "
                       "storage full table name {}, no_destination {}, async_insert {}",
                  view_level, views_data->source_storage->getName(),
                  views_data->source_storage->getStorageID().getFullTableName(),
                  storage->getName(), storage->getStorageID().getFullTableName(), no_destination, async_insert);

        auto sink = std::make_shared<PushingToLiveViewSink>(live_view_header, *live_view, storage, context);
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));

        result_chain.addSource(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(result_chain.getInputHeader()));
    }
    else if (auto * window_view = dynamic_cast<StorageWindowView *>(storage.get()))
    {
        LOG_DEBUG(log, "Generated StorageWindowView chain with view_level {}, "
                       "source_storage name is {}, source-storage full name {}, "
                       "storage id {}, "
                       "storage full table name {},  no_destination {}, async_insert {}",
                  view_level, views_data->source_storage->getName(),
                  views_data->source_storage->getStorageID().getFullTableName(),
                  storage->getName(), storage->getStorageID().getFullTableName(), no_destination, async_insert);
        auto sink = std::make_shared<PushingToWindowViewSink>(window_view->getInputHeader(), *window_view, storage, context);
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));

        result_chain.addSource(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(result_chain.getInputHeader()));
    }
    else if (dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        LOG_DEBUG(log, "Generated StorageMaterializedView chain with view_level {}, "
                       "source_storage name is {}, source-storage full name {}, "
                       "storage id {}, "
                       "storage full table name {}, no_destination {}, async_insert {}",
                  view_level, views_data->source_storage->getName(),
                  views_data->source_storage->getStorageID().getFullTableName(),
                  storage->getName(), storage->getStorageID().getFullTableName(), no_destination, async_insert);
        // 只有当这个 storage 真正指向的是一个 StorageMaterializedView 实例时，转换才会成功。
        auto sink = storage->write(query_ptr, metadata_snapshot, context, async_insert);
        metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));

        result_chain.addSource(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(result_chain.getInputHeader()));
    }
    /// Do not push to destination table if the flag is set
    else if (!no_destination) // 如果  no_destination = false
    {
        LOG_DEBUG(log, "Not StorageMaterializedView and StorageWindowView and StorageLiveView. "
                       "no_destination {} chain with view_level {}, "
                       "source_storage name is {}, source-storage full name {}, "
                       "storage id {}, "
                       "storage full table name {}, no_destination {}, async_insert {}",
                  no_destination,
                  view_level, views_data->source_storage->getName(),
                  views_data->source_storage->getStorageID().getFullTableName(),
                  storage->getName(), storage->getStorageID().getFullTableName(), no_destination, async_insert);
        auto sink = storage->write(query_ptr, metadata_snapshot, context, async_insert);
        metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        sink->setRuntimeData(thread_status, elapsed_counter_ms);

        result_chain.addSource(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(sink->getHeader()));

        result_chain.addSource(std::move(sink));
    }
    else
    { // no_destination = true
        result_chain.addSource(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(storage_header));
    }

    if (result_chain.empty())
        result_chain.addSink(std::make_shared<NullSinkToStorage>(storage_header));

    if (result_chain.getOutputHeader().columns() != 0)
    {
        /// Convert result header to empty block.
        auto dag = ActionsDAG::makeConvertingActions(result_chain.getOutputHeader().getColumnsWithTypeAndName(), {}, ActionsDAG::MatchColumnsMode::Name);
        auto actions = std::make_shared<ExpressionActions>(std::move(dag));
        result_chain.addSink(std::make_shared<ConvertingTransform>(result_chain.getOutputHeader(), std::move(actions)));
    }

    return result_chain;
}

static QueryPipeline process(Block block, ViewRuntimeData & view, const ViewsData & views_data, Chunk::ChunkInfoCollection && chunk_infos, bool disable_deduplication_for_children)
{
    const auto & context = view.context;

    /// We create a table with the same name as original table and the same alias columns,
    ///  but it will contain single block (that is INSERT-ed into main table).
    /// InterpreterSelectQuery will do processing of alias columns.
    auto local_context = Context::createCopy(context);
    local_context->addViewSource(std::make_shared<StorageValues>(
        views_data.source_storage_id,
        views_data.source_metadata_snapshot->getColumns(),
        std::move(block),
        *views_data.source_storage->getVirtualsPtr()));

    QueryPipelineBuilder pipeline;

    if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(view.query, local_context, local_context->getViewSource(), SelectQueryOptions().ignoreAccessCheck());
        pipeline = interpreter.buildQueryPipeline();
    }
    else
    {
        InterpreterSelectQuery interpreter(view.query, local_context, SelectQueryOptions().ignoreAccessCheck());
        pipeline = interpreter.buildQueryPipeline();
    }

    pipeline.resize(1);
    pipeline.dropTotalsAndExtremes();

    /// Squashing is needed here because the materialized view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    pipeline.addTransform(std::make_shared<SquashingTransform>(
        pipeline.getHeader(),
        context->getSettingsRef()[Setting::min_insert_block_size_rows],
        context->getSettingsRef()[Setting::min_insert_block_size_bytes]));

    auto converting = ActionsDAG::makeConvertingActions(
        pipeline.getHeader().getColumnsWithTypeAndName(),
        view.sample_block.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    pipeline.addTransform(std::make_shared<ExpressionTransform>(
        pipeline.getHeader(),
        std::make_shared<ExpressionActions>(std::move(converting))));

    pipeline.addTransform(std::make_shared<RestoreChunkInfosTransform>(std::move(chunk_infos), pipeline.getHeader()));

    if (!disable_deduplication_for_children)
    {
        String materialize_view_id = view.table_id.hasUUID() ? toString(view.table_id.uuid) : view.table_id.getFullNameNotQuoted();
        pipeline.addTransform(std::make_shared<DeduplicationToken::SetViewIDTransform>(std::move(materialize_view_id), pipeline.getHeader()));
        pipeline.addTransform(std::make_shared<DeduplicationToken::SetViewBlockNumberTransform>(pipeline.getHeader()));
    }
    else
    {
        pipeline.addTransform(std::make_shared<DeduplicationToken::ResetTokenTransform>(pipeline.getHeader()));
    }

    return QueryPipelineBuilder::getPipeline(std::move(pipeline));
}

static void logQueryViews(std::list<ViewRuntimeData> & views, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    const UInt64 min_query_duration = settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds();
    const QueryViewsLogElement::ViewStatus min_status = settings[Setting::log_queries_min_type];
    if (views.empty() || !settings[Setting::log_queries] || !settings[Setting::log_query_views])
        return;

    for (auto & view : views)
    {
        const auto & stats = *view.runtime_stats;
        if ((min_query_duration && stats.elapsed_ms <= min_query_duration) || (stats.event_status < min_status))
            continue;

        try
        {
            if (stats.thread_status)
                stats.thread_status->logToQueryViewsLog(view);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


CopyingDataToViewsTransform::CopyingDataToViewsTransform(const Block & header, ViewsDataPtr data, size_t view_level_)
    : IProcessor({header}, OutputPorts(data->views.size(), header))
    , input(inputs.front())
    , views_data(std::move(data))
    , view_level(view_level_)
{
    if (views_data->views.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CopyingDataToViewsTransform cannot have zero outputs");
}

IProcessor::Status CopyingDataToViewsTransform::prepare()
{
    bool all_can_push = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push data to view because output port is finished");

        if (!output.canPush())
            all_can_push = false;
    }

    if (!all_can_push)
        return Status::PortFull;

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto data = input.pullData();
    if (data.exception)
    {
        // If view_level == 0 than the exception comes from the source table.
        // There is no case when we could tolerate exceptions from the source table.
        // Do not tolerate incoming exception and do not pass it to the following processors.
        if (view_level == 0)
            std::rethrow_exception(data.exception);

        if (!views_data->has_exception)
        {
            views_data->first_exception = data.exception;
            views_data->has_exception = true;
        }

        for (auto & output : outputs)
            output.pushException(data.exception);
    }
    else
    {
        for (auto & output : outputs)
            output.push(data.chunk.clone());
    }

    return Status::PortFull;
}


template <typename Executor>
ExecutingInnerQueryFromViewTransform<Executor>::ExecutingInnerQueryFromViewTransform(
    const Block & header,
    ViewRuntimeData & view_,
    std::shared_ptr<ViewsData> views_data_,
    bool disable_deduplication_for_children_)
    : ExceptionKeepingTransform(header, view_.sample_block)
    , views_data(std::move(views_data_))
    , view(view_)
    , disable_deduplication_for_children(disable_deduplication_for_children_)
{
}

template <typename Executor>
void ExecutingInnerQueryFromViewTransform<Executor>::onConsume(Chunk chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    state.emplace(process(std::move(block), view, *views_data, std::move(chunk.getChunkInfos()), disable_deduplication_for_children));
}


template <typename Executor>
ExecutingInnerQueryFromViewTransform<Executor>::GenerateResult ExecutingInnerQueryFromViewTransform<Executor>::onGenerate()
{
    GenerateResult res;
    if (!state.has_value())
        return res;

    res.is_done = false;
    while (!res.is_done)
    {
        res.is_done = !state->executor.pull(res.chunk);
        if (res.chunk)
            break;
    }

    if (res.is_done)
        state.reset();

    return res;
}

PushingToLiveViewSink::PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, StoragePtr storage_holder_, ContextPtr context_)
    : SinkToStorage(header)
    , live_view(live_view_)
    , storage_holder(std::move(storage_holder_))
    , context(std::move(context_))
{
}

void PushingToLiveViewSink::consume(Chunk & chunk)
{
    Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
    live_view.writeBlock(live_view, getHeader().cloneWithColumns(chunk.getColumns()), std::move(chunk.getChunkInfos()), context);

    if (auto process = context->getProcessListElement())
        process->updateProgressIn(local_progress);

    ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
}


PushingToWindowViewSink::PushingToWindowViewSink(
    const Block & header, StorageWindowView & window_view_,
    StoragePtr storage_holder_, ContextPtr context_)
    : SinkToStorage(header)
    , window_view(window_view_)
    , storage_holder(std::move(storage_holder_))
    , context(std::move(context_))
{
}

void PushingToWindowViewSink::consume(Chunk & chunk)
{
    Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
    StorageWindowView::writeIntoWindowView(
        window_view, getHeader().cloneWithColumns(chunk.getColumns()), std::move(chunk.getChunkInfos()), context);

    if (auto process = context->getProcessListElement())
        process->updateProgressIn(local_progress);

    ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
}


FinalizingViewsTransform::FinalizingViewsTransform(std::vector<Block> headers, ViewsDataPtr data)
    : IProcessor(initPorts(std::move(headers)), {Block()})
    , output(outputs.front())
    , views_data(std::move(data))
{
    statuses.resize(views_data->views.size());
}

InputPorts FinalizingViewsTransform::initPorts(std::vector<Block> headers)
{
    InputPorts res;
    for (auto & header : headers)
        res.emplace_back(std::move(header));

    return res;
}

IProcessor::Status FinalizingViewsTransform::prepare()
{
    if (output.isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finalize views because output port is finished");

    if (!output.canPush())
        return Status::PortFull;

    bool ignore_errors = views_data->context->getSettingsRef()[Setting::materialized_views_ignore_errors];
    size_t num_finished = 0;
    size_t pos = 0;
    for (auto & input : inputs)
    {
        auto i = pos;
        ++pos;

        if (input.isFinished())
        {
            ++num_finished;
            continue;
        }

        input.setNeeded();
        if (input.hasData())
        {
            auto data = input.pullData();
            //std::cerr << "********** FinalizingViewsTransform got input " << i << " has exc " << bool(data.exception) << std::endl;
            if (data.exception)
            {
                if (views_data->has_exception && views_data->first_exception == data.exception)
                    statuses[i].is_first = true;
                else
                    statuses[i].exception = data.exception;

                if (i == 0 && statuses[0].is_first && !ignore_errors)
                {
                    output.pushData(std::move(data));
                    return Status::PortFull;
                }
            }

            if (input.isFinished())
                ++num_finished;
        }
    }

    if (num_finished == inputs.size())
    {
        if (!statuses.empty())
            return Status::Ready;

        if (any_exception && !ignore_errors)
            output.pushException(any_exception);

        output.finish();
        return Status::Finished;
    }
    return Status::NeedData;
}

static std::exception_ptr addStorageToException(std::exception_ptr ptr, const StorageID & storage)
{
    try
    {
        std::rethrow_exception(ptr);
    }
    catch (DB::Exception & exception)
    {
        exception.addMessage("while pushing to view {}", storage.getNameForLogs());
        return std::current_exception();
    }
    catch (...)
    {
        return std::current_exception();
    }
}

void FinalizingViewsTransform::work()
{
    size_t i = 0;
    for (auto & view : views_data->views)
    {
        auto & status = statuses[i];
        ++i;

        if (status.exception)
        {
            if (!any_exception)
                any_exception = status.exception;

            view.setException(addStorageToException(status.exception, view.table_id));

            /// Exception will be ignored, it is saved here for the system.query_views_log
            if (views_data->context->getSettingsRef()[Setting::materialized_views_ignore_errors])
                tryLogException(view.exception, getLogger("PushingToViews"), "Cannot push to the storage, ignoring the error");
        }
        else
        {
            view.runtime_stats->setStatus(QueryViewsLogElement::ViewStatus::QUERY_FINISH);

            LOG_TRACE(
                getLogger("PushingToViews"),
                "Pushing from {} to {} took {} ms.",
                views_data->source_storage_id.getNameForLogs(),
                view.table_id.getNameForLogs(),
                view.runtime_stats->elapsed_ms);
        }
    }

    logQueryViews(views_data->views, views_data->context);

    statuses.clear();
}
}
