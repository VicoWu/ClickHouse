#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/logger_useful.h>
#include <Processors/Merges/Algorithms/MergeTreePartLevelInfo.h>
#include <Storages/MergeTree/checkDataPart.h>

namespace DB
{

/// Lightweight (in terms of logic) stream for reading single part from
/// MergeTree, used for merges and mutations.
///
/// NOTE:
///  It doesn't filter out rows that are deleted with lightweight deletes.
///  Use createMergeTreeSequentialSource filter out those rows.
class MergeTreeSequentialSource : public ISource
{
public:
    MergeTreeSequentialSource(
        MergeTreeSequentialSourceType type,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        Names columns_to_read_,
        std::optional<MarkRanges> mark_ranges_,
        bool apply_deleted_mask,
        bool read_with_direct_io_,
        bool prefetch);

    ~MergeTreeSequentialSource() override;

    String getName() const override { return "MergeTreeSequentialSource"; }

    size_t getCurrentMark() const { return current_mark; }

    size_t getCurrentRow() const { return current_row; }

protected:
    Chunk generate() override;

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    /// Should read using direct IO
    bool read_with_direct_io;

    LoggerPtr log = getLogger("MergeTreeSequentialSource");

    std::optional<MarkRanges> mark_ranges;

    std::shared_ptr<MarkCache> mark_cache;
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    MergeTreeReaderPtr reader;

    /// current mark at which we stop reading
    size_t current_mark = 0;

    /// current row at which we stop reading
    size_t current_row = 0;

    /// Closes readers and unlock part locks
    void finish();
};

MergeTreeSequentialSource::MergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    Names columns_to_read_,
    std::optional<MarkRanges> mark_ranges_,
    bool apply_deleted_mask,
    bool read_with_direct_io_,
    bool prefetch)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_ranges(std::move(mark_ranges_))
    , mark_cache(storage.getContext()->getMarkCache())
{
    /// Print column name but don't pollute logs in case of many columns.
    if (columns_to_read.size() == 1)
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
            data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front());
    else
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
            data_part->getMarksCount(), data_part->name, data_part->rows_count);

    auto alter_conversions = storage.getAlterConversionsForPart(data_part);

    /// Note, that we don't check setting collaborate_with_coordinator presence, because this source
    /// is only used in background merges.
    addTotalRowsApprox(data_part->rows_count);

    /**
     * 搜索 NameSet injectRequiredColumns(
     */
    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions),
        storage_snapshot,
        storage.supportsSubcolumns(),
        columns_to_read);

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withExtendedObjects()
        .withVirtuals()
        .withSubcolumns(storage.supportsSubcolumns());

    auto columns_for_reader = storage_snapshot->getColumnsByNames(options, columns_to_read);

    const auto & context = storage.getContext();
    ReadSettings read_settings = context->getReadSettings();
    read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = !storage.getSettings()->force_read_through_cache_for_merges;

    /// It does not make sense to use pthread_threadpool for background merges/mutations
    /// And also to preserve backward compatibility
    read_settings.local_fs_method = LocalFSReadMethod::pread;
    if (read_with_direct_io)
        read_settings.direct_io_threshold = 1;

    /// Configure throttling
    switch (type)
    {
        case Mutation:
            read_settings.local_throttler = context->getMutationsThrottler();
            break;
        case Merge:
            read_settings.local_throttler = context->getMergesThrottler();
            break;
    }
    read_settings.remote_throttler = read_settings.local_throttler;

    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
        .apply_deleted_mask = apply_deleted_mask,
        .can_read_part_without_marks = true,
    };

    if (!mark_ranges)
        mark_ranges.emplace(MarkRanges{MarkRange(0, data_part->getMarksCount())});

    reader = data_part->getReader(
        columns_for_reader,
        storage_snapshot,
        *mark_ranges,
        /*virtual_fields=*/ {},
        /*uncompressed_cache=*/ {},
        mark_cache.get(),
        alter_conversions,
        reader_settings,
        /*avg_value_size_hints=*/ {},
        /*profile_callback=*/ {});

    if (prefetch)
        reader->prefetchBeginOfRange(Priority{});
}

static void fillBlockNumberColumns(
    Columns & res_columns,
    const NamesAndTypesList & columns_list,
    UInt64 block_number,
    UInt64 block_offset,
    UInt64 num_rows)
{
    chassert(res_columns.size() == columns_list.size());

    auto it = columns_list.begin();
    for (size_t i = 0; i < res_columns.size(); ++i, ++it)
    {
        if (res_columns[i])
            continue;

        if (it->name == BlockNumberColumn::name)
        {
            res_columns[i] = BlockNumberColumn::type->createColumnConst(num_rows, block_number)->convertToFullColumnIfConst();
        }
        else if (it->name == BlockOffsetColumn::name)
        {
            auto column = BlockOffsetColumn::type->createColumn();
            auto & block_offset_data = assert_cast<ColumnUInt64 &>(*column).getData();

            block_offset_data.resize(num_rows);
            std::iota(block_offset_data.begin(), block_offset_data.end(), block_offset);

            res_columns[i] = std::move(column);
        }
    }
}

Chunk MergeTreeSequentialSource::generate()
try
{
    const auto & header = getPort().getHeader();
    /// Part level is useful for next step for merging non-merge tree table
    bool add_part_level = storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    // 当前 part（data_part）总共有多少 “mark”（稀疏索引点），用于 readRows 控制读取尾部
    size_t num_marks_in_part = data_part->getMarksCount();
    // 如果当前读到的行数小于这个part的总行数，那么意味着还没有读完，继续读取
    if (!isCancelled() && current_row < data_part->rows_count)
    {
        // 获取当前Mark对应的可读的数据行数
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
        // 首个 mark 不需要“延续”，之后的 mark 需告知 Reader “接着读”，即如果current_mark=0，不存在接着读，而如果current_mark!=0，需要从前面读的位置接着读
        bool continue_reading = (current_mark != 0);
        // IMergeTreeReader 给出的 header（NamesAndTypesList）；我的 case 中 sample 对应 [host String, tagGroup1.values Array(LowCardinality(String))]
        const auto & sample = reader->getColumns();
        Columns columns(sample.size()); // 构造需要读取的Column的Vector，即为每个请求的列分配槽位（初始全 nullptr)，  std::vector<ColumnPtr>;
        // 使用IMergeTreeReader从指定的part读取指定行数的数据, 从 part 读取真实数据，填进 columns。
        // 由于host在part中不存在，这一步只把 tagGroup1.values 读出来，host 保持 nullptr；返回值 rows_read 等于实际读到的行数
        size_t rows_read = reader->readRows(current_mark, num_marks_in_part, continue_reading, rows_to_read, columns);

        if (rows_read) // 如果的确读到了数据
        {
            // 填充诸如 _part, _part_index 的虚拟列（查 storage_snapshot->virtual_columns）。
            fillBlockNumberColumns(columns, sample, data_part->info.min_block, current_row, rows_read);
            reader->fillVirtualColumns(columns, rows_read);
            // 更新 current_row、current_mark：推进游标，下一轮知道从哪继续。
            current_row += rows_read; // 更新读取的总行数
            current_mark += (rows_to_read == rows_read);

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read);

            reader->performRequiredConversions(columns);

            if (should_evaluate_missing_defaults)
                reader->evaluateMissingDefaults({}, columns); // 调用的时候，columns包含了两列，host和tagGroup1.values

            /// Reorder columns and fill result block.
            size_t num_columns = sample.size();
            Columns res_columns;
            res_columns.reserve(num_columns);

            auto it = sample.begin();
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (header.has(it->name))
                {
                    columns[i]->assumeMutableRef().shrinkToFit();
                    res_columns.emplace_back(std::move(columns[i]));
                }

                ++it;
            }

            auto result = Chunk(std::move(res_columns), rows_read);
            if (add_part_level)
                result.getChunkInfos().add(std::make_shared<MergeTreePartLevelInfo>(data_part->info.level));
            return result;
        }
    }
    else
    {
        finish();
    }

    return {};
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (!isRetryableException(std::current_exception()))
        storage.reportBrokenPart(data_part);
    throw;
}

void MergeTreeSequentialSource::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    reader.reset();
    data_part.reset();
}

MergeTreeSequentialSource::~MergeTreeSequentialSource() = default;


Pipe createMergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part, // 需要读取的part
    Names columns_to_read, // 需要读取的列，可以看到，在Vertical Merge中，这个列是merging_columns，而不是gathering_columns
    std::optional<MarkRanges> mark_ranges,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    bool read_with_direct_io,
    bool prefetch)
{

    /// The part might have some rows masked by lightweight deletes
    const bool need_to_filter_deleted_rows = apply_deleted_mask && data_part->hasLightweightDelete();
    const bool has_filter_column = std::ranges::find(columns_to_read, RowExistsColumn::name) != columns_to_read.end();

    if (need_to_filter_deleted_rows && !has_filter_column)
        columns_to_read.emplace_back(RowExistsColumn::name);

    auto column_part_source = std::make_shared<MergeTreeSequentialSource>(type,
        storage, storage_snapshot, data_part, columns_to_read, std::move(mark_ranges),
        /*apply_deleted_mask=*/ false, read_with_direct_io, prefetch);

    Pipe pipe(std::move(column_part_source));

    /// Add filtering step that discards deleted rows
    if (need_to_filter_deleted_rows)
    {
        pipe.addSimpleTransform([filtered_rows_count, has_filter_column](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header, nullptr, RowExistsColumn::name, !has_filter_column, false, filtered_rows_count);
        });
    }

    return pipe;
}

/// A Query Plan step to read from a single Merge Tree part
/// using Merge Tree Sequential Source (which reads strictly sequentially in a single thread).
/// This step is used for mutations because the usual reading is too tricky.
/// Previously, sequential reading was achieved by changing some settings like max_threads,
/// however, this approach lead to data corruption after some new settings were introduced.
class ReadFromPart final : public ISourceStep
{
public:
    ReadFromPart(
        MergeTreeSequentialSourceType type_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        Names columns_to_read_,
        bool apply_deleted_mask_,
        std::optional<ActionsDAG> filter_,
        ContextPtr context_,
        LoggerPtr log_)
        : ISourceStep(DataStream{.header = storage_snapshot_->getSampleBlockForColumns(columns_to_read_)})
        , type(type_)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , data_part(std::move(data_part_))
        , columns_to_read(std::move(columns_to_read_))
        , apply_deleted_mask(apply_deleted_mask_)
        , filter(std::move(filter_))
        , context(std::move(context_))
        , log(log_)
    {
    }

    String getName() const override { return fmt::format("ReadFromPart({})", data_part->name); }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        std::optional<MarkRanges> mark_ranges;

        const auto & metadata_snapshot = storage_snapshot->metadata;
        if (filter && metadata_snapshot->hasPrimaryKey())
        {
            const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
            const Names & primary_key_column_names = primary_key.column_names;
            KeyCondition key_condition(&*filter, context, primary_key_column_names, primary_key.expression);
            LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

            if (!key_condition.alwaysFalse())
                mark_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                    data_part,
                    metadata_snapshot,
                    key_condition,
                    /*part_offset_condition=*/{},
                    /*exact_ranges=*/nullptr,
                    context->getSettingsRef(),
                    log);

            if (mark_ranges && mark_ranges->empty())
            {
                pipeline.init(Pipe(std::make_unique<NullSource>(output_stream->header)));
                return;
            }
        }

        auto source = createMergeTreeSequentialSource(type,
            storage,
            storage_snapshot,
            data_part,
            columns_to_read,
            std::move(mark_ranges),
            /*filtered_rows_count=*/ nullptr,
            apply_deleted_mask,
            /*read_with_direct_io=*/ false,
            /*prefetch=*/ false);

        pipeline.init(Pipe(std::move(source)));
    }

private:
    MergeTreeSequentialSourceType type;
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeData::DataPartPtr data_part;
    Names columns_to_read;
    bool apply_deleted_mask;
    std::optional<ActionsDAG> filter;
    ContextPtr context;
    LoggerPtr log;
};

void createReadFromPartStep(
    MergeTreeSequentialSourceType type,
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    Names columns_to_read,
    bool apply_deleted_mask,
    std::optional<ActionsDAG> filter,
    ContextPtr context,
    LoggerPtr log)
{
    auto reading = std::make_unique<ReadFromPart>(type,
        storage, storage_snapshot, std::move(data_part),
        std::move(columns_to_read), apply_deleted_mask,
        std::move(filter), std::move(context), log);

    plan.addStep(std::move(reading));
}

}
