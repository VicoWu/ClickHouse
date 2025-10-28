#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <unordered_map>


namespace DB
{

enum class ColumnDefaultKind : uint8_t
{
    Default,  // 普通默认值。INSERT 未提供该列时，用表达式计算补齐；值可在读/合并时动态补齐，不一定提前落盘。
    Materialized, // 物化列。值在写入时由表达式计算并“物理写入”该列；INSERT 不能显式赋值；上游列变更不会自动重算（需 MATERIALIZE COLUMN）
    Alias, // 别名列。不落盘，读时用表达式即时计算；不能 INSERT/UPDATE；等价于对其他列/表达式的只读视图
    Ephemeral // 短暂/内部列（临时默认）。仅在执行过程中按需生成，不持久化，用户一般不会在表定义中直接使用，主要服务于内部流程（例如读取/合并阶段需要的临时派生列）。
};


ColumnDefaultKind columnDefaultKindFromString(const std::string & str);
std::string toString(ColumnDefaultKind kind);


struct ColumnDefault
{
    ColumnDefault() = default;
    ColumnDefault(const ColumnDefault & other) { *this = other; }
    ColumnDefault & operator=(const ColumnDefault & other);
    ColumnDefault(ColumnDefault && other) noexcept { *this = std::move(other); }
    ColumnDefault & operator=(ColumnDefault && other) noexcept;

    ColumnDefaultKind kind = ColumnDefaultKind::Default;
    ASTPtr expression;
    bool ephemeral_default = false;
};

bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);

using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

}
