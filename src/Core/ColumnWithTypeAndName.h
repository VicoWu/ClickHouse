#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class WriteBuffer;


/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */
struct ColumnWithTypeAndName
{
    /**
     * ColumnPtr column 指向这列的实际数据（一个 IColumn 派生对象）。它可以有两种状态：
            非空：持有这一列的具体数据块。
            空指针：只表示“列头”（有类型和名字，用于描述/占位），还没有装载数据。
       type 说明数据类型，name 是列名或临时名。
     */
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() = default;
    ColumnWithTypeAndName(const ColumnPtr & column_, const DataTypePtr & type_, const String & name_)
        : column(column_), type(type_), name(name_) {}

    /// Uses type->createColumn() to create column
    ColumnWithTypeAndName(const DataTypePtr & type_, const String & name_)
        : column(type_->createColumn()), type(type_), name(name_) {}

    ColumnWithTypeAndName cloneEmpty() const;
    bool operator==(const ColumnWithTypeAndName & other) const;

    void dumpNameAndType(WriteBuffer & out) const;
    void dumpStructure(WriteBuffer & out) const;
    String dumpStructure() const;
};

}
