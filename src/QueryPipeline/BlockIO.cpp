#include <QueryPipeline/BlockIO.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

/**
 * 在BlockIO的析构函数BlockIO::~BlockIO()被调用的时候会调用这个方法
 * TCPHandler对executeQuery返回的BlockIO对象的生命周期的管理决定了BlockIO的析构函数什么时候被调用
 */
void BlockIO::reset()
{
    /** process_list_entry should be destroyed after in, after out and after pipeline,
      *  since in, out and pipeline contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      *
      *  However, QueryStatus inside process_list_entry holds shared pointers to streams for some reason.
      *  Streams must be destroyed before storage locks, storages and contexts inside pipeline,
      *  so releaseQueryStreams() is required.
      */
    /// TODO simplify it all

    pipeline.reset();
    // reset() 使 shared_ptr 不再指向当前对象。如果 shared_ptr 有多个实例指向同一对象，
    // reset() 会减少该对象的引用计数，直到最后一个 shared_ptr 销毁对象。
    // 参考 ProcessListEntry::~ProcessListEntry() 的销毁过程
    process_list_entry.reset();

    /// TODO Do we need also reset callbacks? In which order?
}

BlockIO & BlockIO::operator= (BlockIO && rhs) noexcept
{
    if (this == &rhs)
        return *this;

    /// Explicitly reset fields, so everything is destructed in right order
    reset();

    process_list_entry      = std::move(rhs.process_list_entry);
    pipeline                = std::move(rhs.pipeline);

    finish_callback         = std::move(rhs.finish_callback);
    exception_callback      = std::move(rhs.exception_callback);

    null_format             = rhs.null_format;

    return *this;
}

/**
 * BlockIO最后在 TCPHandler 中被管理，因此可以搜索 void TCPHandler::runImpl()，看到这个方法里通过调用
 * executeQuery 返回的BlockIO对象,所以可以查看TCPHandler对executeQuery返回的BlockIO对象的生命周期的管理
 */
BlockIO::~BlockIO()
{
    reset(); // 完全销毁这个BlockIO
}

/**
 * 搜索 auto finish_or_cancel = [this]() 查看调用位置
 */
void BlockIO::onFinish()
{
    if (finish_callback)
        finish_callback(pipeline);

    pipeline.reset();
}

void BlockIO::onException()
{
    if (exception_callback)
        exception_callback(/* log_error */ true);

    pipeline.reset();
}

void BlockIO::onCancelOrConnectionLoss()
{
    /// Query was not finished gracefully, so we should call exception_callback
    /// But we don't have a real exception
    try
    {
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled or a client has unexpectedly dropped the connection");
    }
    catch (...)
    {
        if (exception_callback)
        {
            exception_callback(/* log_error */ false);
        }

        /// destroy pipeline and write buffers with an exception context
        pipeline.reset();
    }

}

void BlockIO::setAllDataSent() const
{
    /// The following queries does not have process_list_entry:
    /// - internal
    /// - SHOW PROCESSLIST
    if (process_list_entry)
        process_list_entry->getQueryStatus()->setAllDataSent();
}


}
