#pragma once

#include <functional>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class ProcessListEntry;

/**
 * BlockIO 是 ClickHouse 中处理数据查询流的一个类，涉及查询的输入输出流（in, out）以及查询的执行过程和状态。
 * 它涉及多个重要的成员和方法，通常用于表示一个查询的执行状态、管道（pipeline）、和查询过程中相关的资源。
 * 它还负责在查询结束后清理资源，并提供必要的回调机制来处理查询的完成、异常和取消等事件。
 * 在 InterpreterSelectQuery::execute()中，构造了一个BlockIO
 */
struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs) noexcept;
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<ProcessListEntry> process_list_entry;
    // 在 InterpreterSelectQuery::execute()中被赋值，代表真个Query的QueryPipeline
    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(QueryPipeline &)> finish_callback;
    std::function<void(bool)> exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;
    // 搜索 auto finish_or_cancel = [this]() 查看调用位置
    void onFinish();
    void onException();
    void onCancelOrConnectionLoss();

    /// Set is_all_data_sent in system.processes for this query.
    void setAllDataSent() const;

private:
    void reset();
};

}
