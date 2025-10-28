#include <cstring>
#include <memory>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/StringUtils.h>
#include "Columns/IColumn.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>

#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>

#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace Nested
{

std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name)
{
    if (nested_table_name.empty())
        return nested_field_name;

    if (nested_field_name.empty())
        return nested_table_name;

    return nested_table_name + "." + nested_field_name;
}


/** Name can be treated as compound if it contains dot (.) in the middle.
 *
功能: 把带点号的“复合名”拆成两段。若没有可用的点，返回原名和空后缀。
规则:
选择分隔点:
reverse=false: 用第一个点的位置（find_first_of('.'))
reverse=true: 用最后一个点的位置（find_last_of('.'))
边界检查: 没点/点在开头/点在末尾，都返回 {name, ""}。
否则返回 {name[0:idx), name[idx+1:]}。
示例:
splitName("n.a", false) → {"n", "a"}
splitName("n.a.b", false) → {"n", "a.b"}
splitName("n.a.b", true) → {"n.a", "b"}
splitName(".a") 或 splitName("a.") 或 splitName("a", any) → {"原串", ""}
  */
std::pair<std::string, std::string> splitName(const std::string & name, bool reverse)
{
    auto idx = (reverse ? name.find_last_of('.') : name.find_first_of('.'));
    if (idx == std::string::npos || idx == 0 || idx + 1 == name.size())
        return {name, {}};

    return {name.substr(0, idx), name.substr(idx + 1)};
}

std::pair<std::string_view, std::string_view> splitName(std::string_view name, bool reverse)
{
    auto idx = (reverse ? name.find_last_of('.') : name.find_first_of('.'));
    if (idx == std::string::npos || idx == 0 || idx + 1 == name.size())
        return {name, {}};

    return {name.substr(0, idx), name.substr(idx + 1)};
}


std::string extractTableName(const std::string & nested_name)
{
    auto split = splitName(nested_name);
    return split.first;
}


static Block flattenImpl(const Block & block, bool flatten_named_tuple)
{
    Block res;

    for (const auto & elem : block)
    {
        if (isNested(elem.type))
        {
            const DataTypeArray * type_arr = assert_cast<const DataTypeArray *>(elem.type.get());
            const DataTypeTuple * type_tuple = assert_cast<const DataTypeTuple *>(type_arr->getNestedType().get());
            if (type_tuple->haveExplicitNames())
            {
                const DataTypes & element_types = type_tuple->getElements();
                const Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = isColumnConst(*elem.column);
                const ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const ColumnArray *>(&assert_cast<const ColumnConst &>(*elem.column).getDataColumn());
                else
                    column_array = typeid_cast<const ColumnArray *>(elem.column.get());

                const ColumnPtr & column_offsets = column_array->getOffsetsPtr();

                const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(column_array->getData());
                const auto & element_columns = column_tuple.getColumns();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = concatenateName(elem.name, names[i]);
                    ColumnPtr column_array_of_element = ColumnArray::create(element_columns[i], column_offsets);

                    res.insert(ColumnWithTypeAndName(
                        is_const
                            ? ColumnConst::create(std::move(column_array_of_element), block.rows())
                            : column_array_of_element,
                        std::make_shared<DataTypeArray>(element_types[i]),
                        nested_name));
                }
            }
            else
                res.insert(elem);
        }
        else if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(elem.type.get()); type_tuple && flatten_named_tuple)
        {
            if (type_tuple->haveExplicitNames())
            {
                const DataTypes & element_types = type_tuple->getElements();
                const Strings & names = type_tuple->getElementNames();
                const ColumnTuple * column_tuple;
                if (isColumnConst(*elem.column))
                    column_tuple = typeid_cast<const ColumnTuple *>(&assert_cast<const ColumnConst &>(*elem.column).getDataColumn());
                else
                    column_tuple = typeid_cast<const ColumnTuple *>(elem.column.get());
                size_t tuple_size = column_tuple->tupleSize();
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    const auto & element_column = column_tuple->getColumn(i);
                    String nested_name = concatenateName(elem.name, names[i]);
                    res.insert(ColumnWithTypeAndName(element_column.getPtr(), element_types[i], nested_name));
                }
            }
            else
                res.insert(elem);
        }
        else
            res.insert(elem);
    }

    return res;
}

Block flatten(const Block & block)
{
    return flattenImpl(block, true);
}


Block flattenNested(const Block & block)
{
    return flattenImpl(block, false);
}

namespace
{

using NameToDataType = std::map<String, DataTypePtr>;

/**
 * 传入普通的NamesAndTypesList，筛选出对应的Nested列，并以 unordered_map<String, NamesAndTypesList>返回
 * @param names_and_types
 * @return unordered_map<String, NamesAndTypesList>，这个key是对应的Nested的基列的名字，value是多个子列的类型信息
 */
NameToDataType getSubcolumnsOfNested(const NamesAndTypesList & names_and_types)
{
    // nested是一个map，map的key是父列的基名，NamesAndTypesList是对应的子列的列表
    std::unordered_map<String, NamesAndTypesList> nested;
    for (const auto & name_type : names_and_types)
    {
        const auto * type_arr = typeid_cast<const DataTypeArray *>(name_type.type.get());

        /// Ignore true Nested type, but try to unite flatten arrays to Nested type.
        if (!isNested(name_type.type) && type_arr)
        {
            // 对复合子列进行拆分，比如, n.a 拆分成("n", "a")，
            // 这样，split.first = "n"(基列名称), split.second = "a"(子列名称)
            auto split = splitName(name_type.name);
            if (!split.second.empty()) // 如果有子列， 则保存到 unordered_map<String, NamesAndTypesList> nested  中去
                nested[split.first].emplace_back(split.second, type_arr->getNestedType());
        }
    }
    // 构造nested，key是基列的名称，value是createNested(...)的返回值，
    // 即类似一个DataTypeNestedCustomName的封装，描述了每一个Array列的列名和类型
    for (const auto & [name, elems] : nested)
        nested_types.emplace(name, createNested(elems.getTypes(), elems.getNames()));

    return nested_types;
}

}

NamesAndTypesList collect(const NamesAndTypesList & names_and_types)
{
    NamesAndTypesList res;
    // 按基名聚合出每个 Nested 父列的 DataTypeNested(子字段列表) 映射：n -> Nested(a T, b U)。
    auto nested_types = getSubcolumnsOfNested(names_and_types);
    // 对每个 name_type
    for (const auto & name_type : names_and_types)
    {
        auto split = splitName(name_type.name);
        // 如果不是 Array，或没有子名，或基名不在 nested_types，则保留该列到 res。
        // 否则（这是 Nested 的子列，如 n.a Array(T)），跳过，不放进 res。
        if (!isArray(name_type.type) || split.second.empty() || !nested_types.contains(split.first))
            res.push_back(name_type);  // 普通列，直接放到res中
    }

    /**
     * 把 nested_types 里收集到的每个父列（n, DataTypeNested(...)) 追加进 res
     * name_type.first:  Nested 父列的基名（例如 "n"）
     * name_type.second: 对应的 DataTypeNested 类型（包含该 Nested 的子字段定义，如 a T, b U）。
     */

    for (const auto & name_type : nested_types)
        res.emplace_back(name_type.first, name_type.second);

    /**
     * 非 Nested 的列保持原样
     * 属于 Nested 的子列不再单独出现，转而以一个父列 n: Nested(a T, b U) 出现在列表中。
     * 常用于 Wide part 的“逻辑视图”重建（配合 GetColumnsOptions::All），便于识别 Nested 父列、共享 offsets 等。
     */
    return res;
}

/**
 * class NamesAndTypesList : public std::list<NameAndTypePair>
 * 对 Nested 来说，它的子字段是以 Array(T) 形式出现（Nested 物理上拆成多列的 Array），convertToSubcolumns方法 利用这一点把像 "n.a: Array(T)"，"n.b: Array(T)"
 * 规范化为“子列表示”（基名 n + 子列 a），以便共享 offsets、精确到子列读取。
 * 它不会把任意复合类型“转成 Array”；只在检测到 type 是 Array 且名字形如 base.child（Nested 场景）时做规范化。
 * @param names_and_types std::list<NameAndTypePair>，这里的列是没有拆解的列，即加入是复合列，那么这里还没有拆解
 * @return
 */
NamesAndTypesList convertToSubcolumns(const NamesAndTypesList & names_and_types)
{
    // using NameToDataType = std::map<String, DataTypePtr>;
    // 在names_and_types中 收集每个 Nested 基名对应的 Nested 类型，形成一个map，如 n -> DataTypeNested(n).
    auto nested_types = getSubcolumnsOfNested(names_and_types);
    auto res = names_and_types;

    for (auto & name_type : res) // 对于参数中的列(基列，是对Nested已经拆解成多个Array以后的列)
    {
        // 若该项类型不是 Array，跳过（Nested 的每个字段物理上是 Array(...)）。
        if (!isArray(name_type.type))
            continue;
        // 把名字拆成 (基名, 子列名)，如 "n.a" -> ("n","a")
        auto split = splitName(name_type.name);
        // 已是子列表示(isSubcolumn())或没有子列名(split.second 为空)，跳过
        if (name_type.isSubcolumn() || split.second.empty())
            continue;
        // 查找基列名称，找到对应的子列
        auto it = nested_types.find(split.first);
        if (it != nested_types.end()) // 找到了这个基名信息
            // 把当前的NameAndTypePair替换成一个新的NameAndTypePair，包含了这个子列的等价规范的信息
            name_type = NameAndTypePair{split.first,  // 基名 n
                                        split.second,  // 子列名 b
                                        it->second,   // 这个基列对应的 DataTypeNested，DataTypeNested中是包含了这个基列的所有子列信息
                                        it->second->getSubcolumnType(split.second)  // 这个基名对应的子列名的类型
            };
    }

    return res;
}


void validateArraySizes(const Block & block)
{
    /// Nested prefix -> position of first column in block.
    std::map<std::string, size_t> nested;

    for (size_t i = 0, size = block.columns(); i < size; ++i)
    {
        const auto & elem = block.getByPosition(i);

        if (isArray(elem.type))
        {
            if (!typeid_cast<const ColumnArray *>(elem.column.get()))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Column with Array type is not represented by ColumnArray column: {}",
                                elem.column->dumpStructure());

            auto split = splitName(elem.name);

            /// Is it really a column of Nested data structure.
            if (!split.second.empty())
            {
                auto [it, inserted] = nested.emplace(split.first, i);

                /// It's not the first column of Nested data structure.
                if (!inserted)
                {
                    const ColumnArray & first_array_column = assert_cast<const ColumnArray &>(*block.getByPosition(it->second).column);
                    const ColumnArray & another_array_column = assert_cast<const ColumnArray &>(*elem.column);

                    if (!first_array_column.hasEqualOffsets(another_array_column))
                        throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                                        "Elements '{}' and '{}' "
                                        "of Nested data structure '{}' (Array columns) have different array sizes.",
                                        block.getByPosition(it->second).name, elem.name, split.first);
                }
            }
        }
    }
}


std::unordered_set<String> getAllTableNames(const Block & block, bool to_lower_case)
{
    std::unordered_set<String> nested_table_names;
    for (const auto & name : block.getNames())
    {
        auto nested_table_name = Nested::extractTableName(name);
        if (to_lower_case)
            boost::to_lower(nested_table_name);

        if (!nested_table_name.empty())
            nested_table_names.insert(std::move(nested_table_name));
    }
    return nested_table_names;
}

Names getAllNestedColumnsForTable(const Block & block, const std::string & table_name)
{
    Names names;
    for (const auto & name: block.getNames())
    {
        if (extractTableName(name) == table_name)
            names.push_back(name);
    }
    return names;
}

bool isSubcolumnOfNested(const String & column_name, const ColumnsDescription & columns)
{
    auto nested_subcolumn = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
    return nested_subcolumn && isNested(nested_subcolumn->getTypeInStorage()) && nested_subcolumn->isSubcolumn() && isArray(nested_subcolumn->type);
}

}

NestedColumnExtractHelper::NestedColumnExtractHelper(const Block & block_, bool case_insentive_)
    : block(block_)
    , case_insentive(case_insentive_)
{}

std::optional<ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(const String & column_name)
{
    if (block.has(column_name, case_insentive))
        return {block.getByName(column_name, case_insentive)};

    auto nested_names = Nested::splitName(column_name);
    if (case_insentive)
    {
        boost::to_lower(nested_names.first);
        boost::to_lower(nested_names.second);
    }
    if (!block.has(nested_names.first, case_insentive))
        return {};

    if (!nested_tables.contains(nested_names.first))
    {
        ColumnsWithTypeAndName columns = {block.getByName(nested_names.first, case_insentive)};
        nested_tables[nested_names.first] = std::make_shared<Block>(Nested::flatten(columns));
    }

    return extractColumn(column_name, nested_names.first, nested_names.second);
}

std::optional<ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(
    const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix)
{
    auto table_iter = nested_tables.find(column_name_prefix);
    if (table_iter == nested_tables.end())
    {
        return {};
    }

    auto & nested_table = table_iter->second;
    auto nested_names = Nested::splitName(column_name_suffix);
    auto new_column_name_prefix = Nested::concatenateName(column_name_prefix, nested_names.first);
    if (nested_names.second.empty())
    {
        if (auto * column_ref = nested_table->findByName(new_column_name_prefix, case_insentive))
        {
            ColumnWithTypeAndName column = *column_ref;
            if (case_insentive)
                column.name = original_column_name;
            return {std::move(column)};
        }
        else
        {
            return {};
        }
    }

    if (!nested_table->has(new_column_name_prefix, case_insentive))
    {
        return {};
    }

    ColumnsWithTypeAndName columns = {nested_table->getByName(new_column_name_prefix, case_insentive)};
    Block sub_block(columns);
    nested_tables[new_column_name_prefix] = std::make_shared<Block>(Nested::flatten(sub_block));
    return extractColumn(original_column_name, new_column_name_prefix, nested_names.second);
}
}
