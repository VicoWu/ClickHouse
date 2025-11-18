#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IMergeTreeReader::IMergeTreeReader(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    const NamesAndTypesList & columns_, // 这次merge需要读取的Column，显然，对于Vertical Merge的VerticalMergeStage阶段，这个columns就是gathering_columns 中的某一个Column
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    const MarkRanges & all_mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    const ValueSizeMap & avg_value_size_hints_)
    : data_part_info_for_read(data_part_info_for_read_)
    , avg_value_size_hints(avg_value_size_hints_)
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , settings(settings_)
    , storage_snapshot(storage_snapshot_)
    , all_mark_ranges(all_mark_ranges_)
    , alter_conversions(data_part_info_for_read->getAlterConversions())
    /// For wide parts convert plain arrays of Nested to subcolumns
    /// to allow to use shared offset column from cache.
    , original_requested_columns(columns_) // 原始的请求的Column，一个 NamesAndTypesList
    , requested_columns(data_part_info_for_read->isWidePart() // 如果是Wide Part，那么如果是复合列，还需要对复合列进行拆分，拆分以后放到requested_columns中
        ? Nested::convertToSubcolumns(columns_) // 对columns_中的列进行规范化处理，返回 NamesAndTypesList
        : columns_)
    , part_columns(data_part_info_for_read->isWidePart() // 设置这个part的column信息，如果是wide part，那么就收集包含Nested Column的
        ? data_part_info_for_read->getColumnsDescriptionWithCollectedNested() // 对这个Part中的所有列进行规范化处理，返回ColumnDescription，ColumnDescription其实就是封装了规范化以后的NamesAndTypesList
        : data_part_info_for_read->getColumnsDescription())
    , virtual_fields(virtual_fields_)
{
    columns_to_read.reserve(requested_columns.size());
    serializations.reserve(requested_columns.size());

    for (const auto & column : requested_columns)
    {
        columns_to_read.emplace_back(getColumnInPart(column));
        serializations.emplace_back(getSerializationInPart(column)); // 每一个Column的序列化实现类ISerialization存放在serializations中
    }
}

const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}

void IMergeTreeReader::fillVirtualColumns(Columns & columns, size_t rows) const
{
    chassert(columns.size() == requested_columns.size());

    const auto * loaded_part_info = typeid_cast<const LoadedMergeTreeDataPartInfoForReader *>(data_part_info_for_read.get());
    if (!loaded_part_info)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Filling of virtual columns is supported only for LoadedMergeTreeDataPartInfoForReader");

    const auto & data_part = loaded_part_info->getDataPart();
    const auto & storage_columns = storage_snapshot->metadata->getColumns();
    const auto & virtual_columns = storage_snapshot->virtual_columns;

    auto it = requested_columns.begin();
    for (size_t pos = 0; pos < columns.size(); ++pos, ++it)
    {
        if (columns[pos] || storage_columns.has(it->name))
            continue;

        auto virtual_column = virtual_columns->tryGet(it->name);
        if (!virtual_column)
            continue;

        if (!it->type->equals(*virtual_column->type))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Data type for virtual column {} mismatched. Requested type: {}, virtual column type: {}",
                it->name, it->type->getName(), virtual_column->type->getName());
        }

        if (MergeTreeRangeReader::virtuals_to_fill.contains(it->name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Virtual column {} must be filled by range reader", it->name);

        Field field;
        if (auto field_it = virtual_fields.find(it->name); field_it != virtual_fields.end())
            field = field_it->second;
        else
            field = getFieldForConstVirtualColumn(it->name, *data_part);

        columns[pos] = virtual_column->type->createColumnConst(rows, field)->convertToFullColumnIfConst();
    }
}

void IMergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows) const
{
    try
    {
        NamesAndTypesList available_columns(columns_to_read.begin(), columns_to_read.end());
        DB::fillMissingColumns(
            res_columns, // 带出返回值
            num_rows, // 行号
            Nested::convertToSubcolumns(requested_columns), // 请求的列
            Nested::convertToSubcolumns(available_columns), // part中实际的column
            partially_read_columns,
            storage_snapshot->metadata);
        // 只要res_columns中有任何一个Column是nullptr，那么 should_evaluate_missing_defaults = true
        should_evaluate_missing_defaults = std::any_of(
            res_columns.begin(), res_columns.end(), [](const auto & column) { return column == nullptr; });
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

/**
 * 这里的res_columns就是请求的列，比如，当前的VerticalMergeStage正在请求的例
 * Columns & res_columns 与 original_requested_columns(构造IMergeTreeReader的时候传入的) 对齐，
 * 代表调用方本次请求的列集：有可能是基列，也可能是子列。
 * 它和 original_requested_columns 一一对应，位置相同；已读出的列是非空指针，缺失的列为 nullptr，后续默认值计算会据此填满。
 *
 * 在我们的例子中，
 * 这个列是Map<LowCardinality<String>,String>，而不是缺失的那5个非LowCardinality列，但是方法evaluateMissingDefaults
 * 的触发确实是有那5列缺失触发的
 * @param additional_columns
 * @param res_columns
 */
void IMergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const
{
    try
    {
        size_t num_columns = original_requested_columns.size();

        // 在构造 IMergeTreeReader 的时候传入的列一定是当前处理的列
        if (res_columns.size() != num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected {}, got {}", num_columns, res_columns.size());

        NameSet full_requested_columns_set;
        NamesAndTypesList full_requested_columns;

        /// Convert columns list to block. And convert subcolumns to full columns.
        /// Defaults should be executed on full columns to get correct values for subcolumns.
        /// TODO: rewrite with columns interface. It will be possible after changes in ExpressionActions.

        auto it = original_requested_columns.begin();
        // 遍历构造IMergeTreeReader的时候传入的参数列 original_requested_columns
        for (size_t pos = 0; pos < num_columns; ++pos, ++it)
        {
            // 找到对应的存储列。如果是子列，则映射到对应的表结构的基列
            auto name_in_storage = it->getNameInStorage();

            // 构建 full_requested_columns， 把对应的子列提升为对应的存储列(基列)级别
            // 如果 full_requested_columns_set中不存在name_in_storage， 则执行插入
            // NameSet full_requested_columns_set; 和 NamesAndTypesList full_requested_columns;。前者用来去重存储列名，后者存对应列名+类型
            if (full_requested_columns_set.emplace(name_in_storage).second)
                full_requested_columns.emplace_back(name_in_storage, it->getTypeInStorage());

            // res_columns 是与original_requested_columns同样位置对齐的 Columns 数组，
            // Columns & res_columns 与 original_requested_columns 对齐，代表调用方本次请求的列集：有可能是基列，也可能是子列。
            // 它和 original_requested_columns 一一对应，位置相同；已读出的列是非空指针，缺失的列为 nullptr，后续默认值计算会据此填满。
            // res_columns里面有的指针已经被上一步读取填好，有的缺失仍是 nullptr。
            if (res_columns[pos]) // 空指针就代表缺失列，不往下插，因为缺的列要靠后面的默认表达式去补，
                // 所以，additional_columns代表的是不缺的列，即如果当前列已有数据，就把它塞进临时的 Block additional_columns
                additional_columns.insert({res_columns[pos], it->type, it->name});
        }

        // 构造评估默认值的DAG, 这里根据表元数据（包含 DEFAULT/MATERIALIZED）和已存在的列，生成一棵表达式 DAG，指明哪些缺失列要怎么计算。
        auto dag = DB::evaluateMissingDefaults(
            additional_columns, full_requested_columns,
            storage_snapshot->metadata->getColumns(),
            data_part_info_for_read->getContext());

        if (dag)
        {
            dag->addMaterializingOutputActions();
            auto actions = std::make_shared<ExpressionActions>(
                std::move(*dag),
                ExpressionActionsSettings::fromSettings(data_part_info_for_read->getContext()->getSettingsRef()));
            /**
             * actions->execute(additional_columns); 会“把计算结果放进这个 Block”，既包括：
             *  对原本缺失的列：按照默认表达式新建出一列，加到 additional_columns 里（原来没有这一列，现在有了）。
             *  对已有的列：如果表达式图里定义了对它的计算（比如 MATERIALIZED/DEFAULT 依赖），也会在同一个 Block 里覆盖/更新对应列的数据。
             */

            actions->execute(additional_columns);
        }

        /// Move columns from block.
        // 再次遍历原始的请求列，
        // 这里的循环负责把默认值计算后的结果从 additional_columns 拿出来，按调用方请求的粒度（可能是基列，也可能是子列）填回 res_columns
        it = original_requested_columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++it)
        {
            // 如果请求的是子列，这里取的是它所属的存储列名；如果请求的是基列，就是自身列名。
            auto name_in_storage = it->getNameInStorage(); // 获取对应基列的列名
            // 先拿到对应存储列在 additional_columns 里的完整列数据指针，放到结果槽位。
            // 此时 res_columns[pos] 持有的是“基列”的完整数据。
            res_columns[pos] = additional_columns.getByName(name_in_storage).column;

            // 若原请求是子列而不是基列，就还要从刚取出的完整基列里切出子列
            if (it->isSubcolumn()) // 如果当前的这个请求列是子列
            {
                // 拿到基列的数据类型对象，用它来解析子列
                const auto & type_in_storage = it->getTypeInStorage(); // 获取对应的基列的列类型
                // 在这里报错了
                // 这里，从完整列(res_columns[pos])中提取出对应的子列数据，替换掉 res_columns[pos]。
                // 这样最终返回给上层的就是用户请求的那一列形状，而不是整个基列
                res_columns[pos] = type_in_storage->getSubcolumn(it->getSubcolumnName(), res_columns[pos]);
            }
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

bool IMergeTreeReader::isSubcolumnOffsetsOfNested(const String & name_in_storage, const String & subcolumn_name) const
{
    /// We cannot read separate subcolumn with offsets from compact parts.
    if (!data_part_info_for_read->isWidePart() || subcolumn_name != "size0")
        return false;

    auto split = Nested::splitName(name_in_storage);
    if (split.second.empty())
        return false;

    auto nested_column = part_columns.tryGetColumn(GetColumnsOptions::All, split.first);
    return nested_column && isNested(nested_column->type);
}

String IMergeTreeReader::getColumnNameInPart(const NameAndTypePair & required_column) const
{
    auto name_in_storage = required_column.getNameInStorage();
    auto subcolumn_name = required_column.getSubcolumnName();

    // 恢复到alter以前的column名字
    if (alter_conversions->isColumnRenamed(name_in_storage))
        name_in_storage = alter_conversions->getColumnOldName(name_in_storage);

    /// A special case when we read subcolumn of shared offsets of Nested.
    /// E.g. instead of requested column "n.arr1.size0" we must read column "n.size0" from disk.
    if (isSubcolumnOffsetsOfNested(name_in_storage, subcolumn_name))
        name_in_storage = Nested::splitName(name_in_storage).first;

    return Nested::concatenateName(name_in_storage, subcolumn_name);
}

NameAndTypePair IMergeTreeReader::getColumnInPart(const NameAndTypePair & required_column) const
{
    auto name_in_part = getColumnNameInPart(required_column);
    auto column_in_part = part_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name_in_part);
    if (column_in_part)
        return *column_in_part;

    return required_column;
}

SerializationPtr IMergeTreeReader::getSerializationInPart(const NameAndTypePair & required_column) const
{
    auto name_in_part = getColumnNameInPart(required_column);
    auto column_in_part = part_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name_in_part);
    if (!column_in_part)
        return IDataType::getSerialization(required_column);

    const auto & infos = data_part_info_for_read->getSerializationInfos();
    if (auto it = infos.find(column_in_part->getNameInStorage()); it != infos.end())
        return IDataType::getSerialization(*column_in_part, *it->second);

    return IDataType::getSerialization(*column_in_part);
}

void IMergeTreeReader::performRequiredConversions(Columns & res_columns) const
{
    try
    {
        size_t num_columns = requested_columns.size();

        if (res_columns.size() != num_columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Invalid number of columns passed to MergeTreeReader::performRequiredConversions. "
                            "Expected {}, got {}", num_columns, res_columns.size());
        }

        Block copy_block;
        auto name_and_type = requested_columns.begin();

        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            copy_block.insert({res_columns[pos], getColumnInPart(*name_and_type).type, name_and_type->name});
        }

        DB::performRequiredConversions(copy_block, requested_columns, data_part_info_for_read->getContext());

        /// Move columns from block.
        name_and_type = requested_columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            if (copy_block.has(name_and_type->name))
                res_columns[pos] = std::move(copy_block.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

IMergeTreeReader::ColumnNameLevel IMergeTreeReader::findColumnForOffsets(const NameAndTypePair & required_column) const
{
    auto get_offsets_streams = [](const auto & serialization, const auto & name_in_storage)
    {
        std::vector<std::pair<String, size_t>> offsets_streams;
        serialization->enumerateStreams([&](const auto & subpath)
        {
            if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                return;

            auto subname = ISerialization::getSubcolumnNameForStream(subpath);
            auto full_name = Nested::concatenateName(name_in_storage, subname);
            offsets_streams.emplace_back(full_name, ISerialization::getArrayLevel(subpath));
        });

        return offsets_streams;
    };

    auto required_name_in_storage = Nested::extractTableName(required_column.getNameInStorage());
    auto required_offsets_streams = get_offsets_streams(getSerializationInPart(required_column), required_name_in_storage);

    size_t max_matched_streams = 0;
    ColumnNameLevel name_level;

    /// Find column that has maximal number of matching
    /// offsets columns with required_column.
    for (const auto & part_column : Nested::convertToSubcolumns(data_part_info_for_read->getColumns()))
    {
        auto name_in_storage = Nested::extractTableName(part_column.name);
        if (name_in_storage != required_name_in_storage)
            continue;

        auto offsets_streams = get_offsets_streams(data_part_info_for_read->getSerialization(part_column), name_in_storage);
        NameToIndexMap offsets_streams_map(offsets_streams.begin(), offsets_streams.end());

        size_t i = 0;
        auto it = offsets_streams_map.end();
        for (; i < required_offsets_streams.size(); ++i)
        {
            auto current_it = offsets_streams_map.find(required_offsets_streams[i].first);
            if (current_it == offsets_streams_map.end())
                break;
            it = current_it;
        }

        if (i && (!name_level || i > max_matched_streams))
        {
            max_matched_streams = i;
            name_level.emplace(part_column.name, it->second);
        }
    }

    return name_level;
}

void IMergeTreeReader::checkNumberOfColumns(size_t num_columns_to_read) const
{
    if (num_columns_to_read != requested_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid number of columns passed to MergeTreeReader::readRows. "
                        "Expected {}, got {}", requested_columns.size(), num_columns_to_read);
}

String IMergeTreeReader::getMessageForDiagnosticOfBrokenPart(size_t from_mark, size_t max_rows_to_read) const
{
    const auto & data_part_storage = data_part_info_for_read->getDataPartStorage();
    return fmt::format(
        "(while reading from part {} in table {} located on disk {} of type {}, from mark {} with max_rows_to_read = {})",
        data_part_storage->getFullPath(),
        data_part_info_for_read->getTableName(),
        data_part_storage->getDiskName(),
        data_part_storage->getDiskType(),
        from_mark,
        max_rows_to_read);
}

}
