#include <QueryPipeline/ReadProgressCallback.h>
#include <Interpreters/ProcessList.h>
#include <Access/EnabledQuota.h>


namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}

// std::shared_ptr<QueryStatus>;
void ReadProgressCallback::setProcessListElement(QueryStatusPtr elem)
{
    process_list_elem = elem;
    if (!elem)
        return;

    /// Update total_rows_approx as soon as possible.
    ///
    /// It is important to do this, since you will not get correct
    /// total_rows_approx until the query will start reading all parts (in case
    /// of query needs to read from multiple parts), and this is especially a
    /// problem in case of max_threads=1.
    ///
    /// NOTE: This can be done only if progress callback already set, since
    /// otherwise total_rows_approx will lost.
    size_t rows_approx = 0;
    if (progress_callback && (rows_approx = total_rows_approx.exchange(0)) != 0)
    {
        Progress total_rows_progress = {0, 0, rows_approx};

        progress_callback(total_rows_progress);
        process_list_elem->updateProgressIn(total_rows_progress);
    }
}

/**
 * using StorageLimitsList = std::list<StorageLimits>

 */
bool ReadProgressCallback::onProgress(uint64_t read_rows, uint64_t read_bytes, const StorageLimitsList & storage_limits)
{
    for (const auto & limits : storage_limits)
    {
        if (!limits.local_limits.speed_limits.checkTimeLimit(total_stopwatch, limits.local_limits.timeout_overflow_mode))
            return false;
    }

    size_t rows_approx = 0;
    // total_rows_approx 是一个 std::atomic_size_t 类型的对象，表示一个原子变量,
    // exchange(0) 的意思是将 total_rows_approx 的当前值与 0 交换。即：把 total_rows_approx 的当前值设置为 0，并返回交换前的原始值。
    // 从这个清零的动作可以看到，每次调用ReadProgressCallback::onProgress的时候，rows_approx，bytes都被清零，因此，构造出来的Progress对象
    // 是一个增量的Progress，即两次调用之间发生的Progress，而不是当前的状态Progress
    if ((rows_approx = total_rows_approx.exchange(0)) != 0) // 如果 total_rows_approx 中之前存放的值不是0，那么设置为0，并且执行下面的逻辑
    {
        // 查看构造函数 Progress(UInt64 read_rows_, UInt64 read_bytes_, UInt64 total_rows_to_read_ = 0, UInt64 total_bytes_to_read_ = 0)
        Progress total_rows_progress = {0, 0, rows_approx};
        // 如果定义了progress_callback这个function，那么就调用这个function
        // ReadProgressCallback 对象是在 QueryPipeline::getReadProgressCallback()中每次被调用的时候构造的。
        // 构造 ReadProgressCallback的时候，传入了progress_callback这个function，这个function是通过QueryPipeline::setProgressCallback设置的
        if (progress_callback) // using ProgressCallback = std::function<void(const Progress & progress)>;
            progress_callback(total_rows_progress); // 调用progress_callback这个function

        // 如果这个ReadProgressCallback对象中注册了对应的Query的QueryStatus对象，那么会调用QueryStatus::updateProgressIn
        if (process_list_elem) // process_list_elem其实就是这个Query对应的QueryStatus， 更新QueryStatus的Input Status
            process_list_elem->updateProgressIn(total_rows_progress);
    }

    size_t bytes = 0;
    if ((bytes = total_bytes.exchange(0)) != 0)  // 如果 total_bytes 中之前存放的值不是0，那么设置为0，并且执行下面的逻辑
    {
        // 查看构造函数 Progress(UInt64 read_rows_, UInt64 read_bytes_, UInt64 total_rows_to_read_ = 0, UInt64 total_bytes_to_read_ = 0)
        Progress total_bytes_progress = {0, 0, 0, bytes};

        if (progress_callback)
            progress_callback(total_bytes_progress);// 调用progress_callback这个function

        if (process_list_elem) // 更新QueryStatus的Input Status
            process_list_elem->updateProgressIn(total_bytes_progress);
    }

    Progress value {read_rows, read_bytes};

    if (progress_callback)
        progress_callback(value); // 调用progress_callback这个function

    if (process_list_elem)
    {
        if (!process_list_elem->updateProgressIn(value))
            return false;

        /// The total amount of data processed or intended for processing in all sources, possibly on remote servers.

        ProgressValues progress = process_list_elem->getProgressIn();

        for (const auto & limits : storage_limits)
        {
            /// If the mode is "throw" and estimate of total rows is known, then throw early if an estimate is too high.
            /// If the mode is "break", then allow to read before limit even if estimate is very high.

            size_t rows_to_check_limit = progress.read_rows;
            if (limits.local_limits.size_limits.overflow_mode == OverflowMode::THROW && progress.total_rows_to_read > progress.read_rows)
                rows_to_check_limit = progress.total_rows_to_read;

            /// Check the restrictions on the
            ///  * amount of data to read
            ///  * speed of the query
            ///  * quota on the amount of data to read
            /// NOTE: Maybe it makes sense to have them checked directly in ProcessList?

            if (limits.local_limits.mode == LimitsMode::LIMITS_TOTAL)
            {
                if (!limits.local_limits.size_limits.check(
                        rows_to_check_limit, progress.read_bytes, "rows or bytes to read",
                        ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                {
                    return false;
                }
            }

            if (!limits.leaf_limits.check(
                    rows_to_check_limit, progress.read_bytes, "rows or bytes to read on leaf node",
                    ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
            {
                return false;
            }
        }

        size_t total_rows = progress.total_rows_to_read;

        CurrentThread::updatePerformanceCountersIfNeeded();

        /// TODO: Should be done in PipelineExecutor.
        for (const auto & limits : storage_limits)
            limits.local_limits.speed_limits.throttle(progress.read_rows, progress.read_bytes, total_rows, total_stopwatch.elapsedMicroseconds(), limits.local_limits.timeout_overflow_mode);

        if (quota)
            quota->used({QuotaType::READ_ROWS, value.read_rows}, {QuotaType::READ_BYTES, value.read_bytes});
    }

    if (update_profile_events)
    {
        ProfileEvents::increment(ProfileEvents::SelectedRows, value.read_rows);
        ProfileEvents::increment(ProfileEvents::SelectedBytes, value.read_bytes);
    }

    return true;
}

}
