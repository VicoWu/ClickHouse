#include "inplaceBlockConversions.h"

#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <utility>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Common/checkStackSize.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

namespace ErrorCode
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Add all required expressions for missing columns calculation
void addDefaultRequiredExpressionsRecursively(
    const Block & block,
    const String & required_column_name,
    DataTypePtr required_column_type,
    const ColumnsDescription & columns,
    ASTPtr default_expr_list_accum,
    NameSet & added_columns,
    bool null_as_default)
{
    checkStackSize();

    bool is_column_in_query = block.has(required_column_name);
    bool convert_null_to_default = false;

    if (is_column_in_query)
        convert_null_to_default = null_as_default && isNullableOrLowCardinalityNullable(block.findByName(required_column_name)->type) && !isNullableOrLowCardinalityNullable(required_column_type);

    if ((is_column_in_query && !convert_null_to_default) || added_columns.contains(required_column_name))
        return;

    auto column_default = columns.getDefault(required_column_name);

    if (column_default)
    {
        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        auto column_default_expr = column_default->expression->clone();

        /// Our default may depend on columns with default expr which not present in block
        /// we have to add them to block too
        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(column_default_expr);
        NameSet required_columns_names = columns_context.requiredColumns();
        auto required_type = std::make_shared<ASTLiteral>(columns.get(required_column_name).type->getName());

        auto expr = makeASTFunction("_CAST", column_default_expr, required_type);

        if (is_column_in_query && convert_null_to_default)
        {
            expr = makeASTFunction("ifNull", std::make_shared<ASTIdentifier>(required_column_name), std::move(expr));
            /// ifNull does not respect LowCardinality.
            /// It may be fixed later or re-implemented properly for identical types.
            expr = makeASTFunction("_CAST", std::move(expr), required_type);
        }
        default_expr_list_accum->children.emplace_back(setAlias(expr, required_column_name));

        added_columns.emplace(required_column_name);

        for (const auto & next_required_column_name : required_columns_names)
        {
            /// Required columns of the default expression should not be converted to NULL,
            /// since this map value to default and MATERIALIZED values will not work.
            ///
            /// Consider the following structure:
            /// - A Nullable(Int64)
            /// - X Int64 materialized coalesce(A, -1)
            ///
            /// With recursive_null_as_default=true you will get:
            ///
            ///     _CAST(coalesce(A, -1), 'Int64') AS X, NULL AS A
            ///
            /// And this will ignore default expression.
            bool recursive_null_as_default = false;
            addDefaultRequiredExpressionsRecursively(block,
                next_required_column_name, required_column_type,
                columns, default_expr_list_accum, added_columns,
                recursive_null_as_default);
        }
    }
    else if (columns.has(required_column_name))
    {
        /// In case of dictGet function we allow to use it with identifier dictGet(identifier, 'column_name', key_expression)
        /// and this identifier will be in required columns. If such column is not in ColumnsDescription we ignore it.

        /// This column is required, but doesn't have default expression, so lets use "default default"
        const auto & column = columns.get(required_column_name);
        auto default_value = column.type->getDefault();
        ASTPtr expr = std::make_shared<ASTLiteral>(default_value);
        if (is_column_in_query && convert_null_to_default)
        {
            /// We should CAST default value to required type, otherwise the result of ifNull function can be different type.
            auto cast_expr = makeASTFunction("_CAST", std::move(expr), std::make_shared<ASTLiteral>(columns.get(required_column_name).type->getName()));
            expr = makeASTFunction("ifNull", std::make_shared<ASTIdentifier>(required_column_name), std::move(cast_expr));
        }
        default_expr_list_accum->children.emplace_back(setAlias(expr, required_column_name));
        added_columns.emplace(required_column_name);
    }
}

ASTPtr defaultRequiredExpressions(const Block & block, const NamesAndTypesList & required_columns, const ColumnsDescription & columns, bool null_as_default)
{
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    NameSet added_columns;
    for (const auto & column : required_columns)
        addDefaultRequiredExpressionsRecursively(block, column.name, column.type, columns, default_expr_list, added_columns, null_as_default);

    if (default_expr_list->children.empty())
        return nullptr;

    return default_expr_list;
}

ASTPtr convertRequiredExpressions(Block & block, const NamesAndTypesList & required_columns)
{
    ASTPtr conversion_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & required_column : required_columns)
    {
        if (!block.has(required_column.name))
            continue;

        auto column_in_block = block.getByName(required_column.name);
        if (column_in_block.type->equals(*required_column.type))
            continue;

        auto cast_func = makeASTFunction(
            "_CAST", std::make_shared<ASTIdentifier>(required_column.name), std::make_shared<ASTLiteral>(required_column.type->getName()));

        conversion_expr_list->children.emplace_back(setAlias(cast_func, required_column.name));

    }
    return conversion_expr_list;
}

std::optional<ActionsDAG> createExpressions(
    const Block & header,
    ASTPtr expr_list,
    bool save_unneeded_columns,
    ContextPtr context)
{
    if (!expr_list)
        return {};

    auto syntax_result = TreeRewriter(context).analyze(expr_list, header.getNamesAndTypesList());
    auto expression_analyzer = ExpressionAnalyzer{expr_list, syntax_result, context};
    ActionsDAG dag(header.getNamesAndTypesList());
    auto actions = expression_analyzer.getActionsDAG(true, !save_unneeded_columns);
    return ActionsDAG::merge(std::move(dag), std::move(actions));
}

}

void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context)
{
    ASTPtr conversion_expr_list = convertRequiredExpressions(block, required_columns);
    if (conversion_expr_list->children.empty())
        return;

    if (auto dag = createExpressions(block, conversion_expr_list, true, context))
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(*dag), ExpressionActionsSettings::fromContext(context));
        expression->execute(block);
    }
}

bool needConvertAnyNullToDefault(const Block & header, const NamesAndTypesList & required_columns, const ColumnsDescription & columns)
{
    for (const auto & required_column : required_columns)
    {
        if (columns.has(required_column.name) && isNullableOrLowCardinalityNullable(header.findByName(required_column.name)->type) && !isNullableOrLowCardinalityNullable(required_column.type))
            return true;
    }
    return false;
}

std::optional<ActionsDAG> evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns,
    bool null_as_default)
{
    if (!columns.hasDefaults() && (!null_as_default || !needConvertAnyNullToDefault(header, required_columns, columns)))
        return {};

    ASTPtr expr_list = defaultRequiredExpressions(header, required_columns, columns, null_as_default);
    return createExpressions(header, expr_list, save_unneeded_columns, context);
}

static std::unordered_map<String, ColumnPtr> collectOffsetsColumns(
    const NamesAndTypesList & available_columns, const Columns & res_columns)
{
    std::unordered_map<String, ColumnPtr> offsets_columns;

    auto available_column = available_columns.begin();
    for (size_t i = 0; i < available_columns.size(); ++i, ++available_column)
    {
        if (res_columns[i] == nullptr || isColumnConst(*res_columns[i]))
            continue;

        auto serialization = IDataType::getSerialization(*available_column);
        serialization->enumerateStreams([&](const auto & subpath)
        {
            if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                return;

            auto stream_name = ISerialization::getFileNameForStream(*available_column, subpath);
            const auto & current_offsets_column = subpath.back().data.column;

            /// If for some reason multiple offsets columns are present
            /// for the same nested data structure, choose the one that is not empty.
            if (current_offsets_column && !current_offsets_column->empty())
            {
                auto & offsets_column = offsets_columns[stream_name];
                if (!offsets_column)
                {
                    offsets_column = current_offsets_column;
                }
                else
                {
                    /// If we are inside Variant element, it may happen that
                    /// offsets are different, because when we read Variant
                    /// element as a subcolumn, we expand this column according
                    /// to the discriminators, so, offsets column can be changed.
                    /// In this case we should select the original offsets column
                    /// of this stream, which is the smallest one.
                    bool inside_variant_element = false;
                    for (const auto & elem : subpath)
                        inside_variant_element |= elem.type == ISerialization::Substream::VariantElement;

                    if (offsets_column->size() != current_offsets_column->size() && inside_variant_element)
                        offsets_column = offsets_column->size() < current_offsets_column->size() ? offsets_column : current_offsets_column;
#ifndef NDEBUG
                    else
                    {
                        const auto & offsets_data = assert_cast<const ColumnUInt64 &>(*offsets_column).getData();
                        const auto & current_offsets_data = assert_cast<const ColumnUInt64 &>(*current_offsets_column).getData();

                        if (offsets_data != current_offsets_data)
                            throw Exception(ErrorCodes::LOGICAL_ERROR,
                                            "Found non-equal columns with offsets (sizes: {} and {}) for stream {}",
                                            offsets_data.size(), current_offsets_data.size(), stream_name);
                    }
#endif
                }
            }
        }, available_column->type, res_columns[i]);
    }

    return offsets_columns;
}

static ColumnPtr createColumnWithDefaultValue(const IDataType & data_type, const String & subcolumn_name, size_t num_rows)
{
    auto column = data_type.createColumnConstWithDefaultValue(num_rows);

    /// We must turn a constant column into a full column because the interpreter could infer
    /// that it is constant everywhere but in some blocks (from other parts) it can be a full column.

    if (subcolumn_name.empty())
        return column->convertToFullColumnIfConst();

    /// Firstly get subcolumn from const column and then replicate.
    column = assert_cast<const ColumnConst &>(*column).getDataColumnPtr();
    column = data_type.getSubcolumn(subcolumn_name, column);

    return ColumnConst::create(std::move(column), num_rows)->convertToFullColumnIfConst();
}

static bool hasDefault(const StorageMetadataPtr & metadata_snapshot, const NameAndTypePair & column)
{
    if (!metadata_snapshot)
        return false;

    const auto & columns = metadata_snapshot->getColumns();
    if (columns.has(column.name))
        return columns.hasDefault(column.name);

    auto name_in_storage = column.getNameInStorage();
    return columns.hasDefault(name_in_storage);
}

static String removeTupleElementsFromSubcolumn(String subcolumn_name, const Names & tuple_elements)
{
    /// Add a dot to the end of name for convenience.
    subcolumn_name += ".";
    for (const auto & elem : tuple_elements)
    {
        auto pos = subcolumn_name.find(elem + ".");
        if (pos != std::string::npos)
            subcolumn_name.erase(pos, elem.size() + 1);
    }

    if (subcolumn_name.ends_with("."))
        subcolumn_name.pop_back();

    return subcolumn_name;
}

void fillMissingColumns(
    Columns & res_columns, // res_columns = [nullptr, ColumnArray(...)] ,包含了host和tagGroup1.valuel两列
    size_t num_rows, // fillMissingColumns 用它来在需要时创建“默认列”的行数, 比如，一列没有默认值，并且也在part中缺失，那么需要按照行数去构造对应的占位符，并设置“类型默认值”
    const NamesAndTypesList & requested_columns, // [host String, tagGroup1.values MapValueSubcolumn]，顺序和 res_columns 一一对应
    const NamesAndTypesList & available_columns, // columns_to_read 由构造 IMergeTreeReader 时的 requested_columns 直接转换而来, 包含两项：host（虽然 part 里没有物理流，但仍按请求列记在 columns_to_read）以及 tagGroup1.values（真实从 part 读取的 Map value 子列）
    const NameSet & partially_read_columns, // 在MergeTreeReaderWide::addStreams中被设置，主要是看这一列是否缺少stream
    StorageMetadataPtr metadata_snapshot)
{
    size_t num_columns = requested_columns.size();
    if (num_columns != res_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to fillMissingColumns. Expected {}, got {}",
            num_columns, res_columns.size());

    /// For a missing column of a nested data structure
    /// we must create not a column of empty arrays,
    /// but a column of arrays of correct length.

    /// First, collect offset columns for all arrays in the block.
    // 构造对应的stream_name和IColumn的映射关系
    auto offsets_columns = collectOffsetsColumns(available_columns, res_columns);

    /// Insert default values only for columns without default expressions.
    auto requested_column = requested_columns.begin(); // 对于vertical merge，这里的requested_columns其实仅仅是当前正在进行merge的列，比如，当前的host和tagGroup1.values
    for (size_t i = 0; i < num_columns; ++i, ++requested_column)
    {
        if (res_columns[i] && partially_read_columns.contains(requested_column->name))
            res_columns[i] = nullptr; // 只要该列不为空，并且有 missing stream，那么就先把res_columns[i]置为nullptr，但是这不是最终设置，后面会尝试进行类型默认值的推断

        /// Nothing to fill or default should be filled in evaluateMissingDefaults
        /**
         * 如果 hasDefault(metadata_snapshot, requested_column) 为 true,
         *  直接 continue，不在 fillMissingColumns 内生成任何数据，保持 res_columns[i] 为 nullptr
         *  目的：让 evaluateMissingDefaults 去按 DEFAULT 表达式计算，可能依赖其它列
         */
        if (res_columns[i] || hasDefault(metadata_snapshot, *requested_column))
            continue; // 如果 res_columns[i] 不为空(这一列在part中存在)，或者，虽然不存在，但是有default值，那么直接返回
        // 执行到这里，说明 res_columns[i] 为空，并且目前没有default值，那么在方法内部“就地”补上类型默认值
        // 所以，tagGroup1不会执行到这里（因为它对应的res_columns[i]不是空），host也不在这里(因为它有默认值，会在evaluateMissingDefault中进行默认值评估)
        std::vector<ColumnPtr> current_offsets;
        size_t num_dimensions = 0;

        const auto * array_type = typeid_cast<const DataTypeArray *>(requested_column->type.get());
        if (array_type && !offsets_columns.empty())
        {
            //  获取数组的维度，即，这是几维数组，比如 一维数组的num_dimensions=1,二维数组的num_dimensions=2
            num_dimensions = getNumberOfDimensions(*array_type);
            // current_offsets 的大小设置成这个array的dimension的大小
            current_offsets.resize(num_dimensions);

            auto serialization = IDataType::getSerialization(*requested_column);
            /**
             *  path 是 ISerialization::SubstreamPath ——一个描述“逻辑子流层级”的向量，比如 [{ArraySizes, level=0}, {TupleElement, name='values'}]，表示我们当前处理的是哪个嵌套/子列/维度。
             *  stream_name 则是把 path 和列名拼接成实际的物理流名，也就是落盘的文件前缀（例如 tagGroup1.values.size0）。
             *  换句话说，path 描述结构，stream_name 是最终用来查 .bin/.mrk 的字符串。
             */
            serialization->enumerateStreams([&](const auto & subpath)
            {
                // 遍历这个数组序列化下所有子流，筛选出只要 ArraySizes（即 *.size0/size1...）的流
                if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                    return;

                size_t level = ISerialization::getArrayLevel(subpath);
                /// It can happen if element of Array is Map.
                if (level >= num_dimensions)
                    return;
                // // 拼出真实的 stream 名
                auto stream_name = ISerialization::getFileNameForStream(*requested_column, subpath);
                auto it = offsets_columns.find(stream_name);
                // 如果找到了，就把对应的 ColumnUInt64 指针放到 current_offsets[level]，level 由 ISerialization::getArrayLevel 给出。
                if (it != offsets_columns.end())
                    current_offsets[level] = it->second; // 第level维的值设置为对应的ColumnPtr
            });

            // 对于数组的每一个维度
            for (size_t j = 0; j < num_dimensions; ++j)
            {
                //  如果多维数组里有某一层 offset 缺失，就把 current_offsets 缩短到那一层之前，避免使用不完整的 offsets。
                if (!current_offsets[j])
                {
                    current_offsets.resize(j);
                    break;
                }
            }
        }

        if (!current_offsets.empty())
        {
            Names tuple_elements;
            auto serialization = IDataType::getSerialization(*requested_column);

            /// For Nested columns collect names of tuple elements and skip them while getting the base type of array.
            IDataType::forEachSubcolumn([&](const auto & path, const auto &, const auto &)
            {
                if (path.back().type == ISerialization::Substream::TupleElement)
                    tuple_elements.push_back(path.back().name_of_substream);
            }, ISerialization::SubstreamData(serialization));

            /// The number of dimensions that belongs to the array itself but not shared in Nested column.
            /// For example for column "n Nested(a UInt64, b Array(UInt64))" this value is 0 for `n.a` and 1 for `n.b`.
            size_t num_empty_dimensions = num_dimensions - current_offsets.size();

            auto base_type = getBaseTypeOfArray(requested_column->getTypeInStorage(), tuple_elements);
            auto scalar_type = createArrayOfType(base_type, num_empty_dimensions);
            size_t data_size = assert_cast<const ColumnUInt64 &>(*current_offsets.back()).getData().back();

            /// Remove names of tuple elements because they are already processed by 'getBaseTypeOfArray'.
            auto subcolumn_name = removeTupleElementsFromSubcolumn(requested_column->getSubcolumnName(), tuple_elements);
            res_columns[i] = createColumnWithDefaultValue(*scalar_type, subcolumn_name, data_size);

            for (auto it = current_offsets.rbegin(); it != current_offsets.rend(); ++it)
                res_columns[i] = ColumnArray::create(res_columns[i], *it);
        }
        else
        {
            res_columns[i] = createColumnWithDefaultValue(*requested_column->getTypeInStorage(), requested_column->getSubcolumnName(), num_rows);
        }
    }
}

}
