#include "MergeTreeDataMergerMutator.h"

#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>

#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/Context.h>
#include <base/interpolate.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Parsers/queryToString.h>

#include <cmath>
#include <ctime>
#include <numeric>

#include <boost/algorithm/string/replace.hpp>

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
///  because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (MergerMutator)"))
{
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge() const
{
    size_t scheduled_tasks_count = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    return getMaxSourcePartsSizeForMerge(max_tasks_count, scheduled_tasks_count);
}


/**
 *     M(UInt64, max_replicated_merges_in_queue, 1000,
 *     "How many tasks of merging and mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0)
*/
UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge(size_t max_count, size_t scheduled_tasks_count) const
{
    if (scheduled_tasks_count > max_count) // max_replicated_merges_in_queue
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Logical error: invalid argument passed to getMaxSourcePartsSize: scheduled_tasks_count = {} > max_count = {}",
            scheduled_tasks_count, max_count);
    }

    size_t free_entries = max_count - scheduled_tasks_count; // 剩余可调度的free entry
    const auto data_settings = data.getSettings();

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    /**
     *     M(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8,
     *     "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue).
     *     This is to allow small merges to process - not filling the pool with long running merges.", 0) \
     */
    if (scheduled_tasks_count <= 1 || free_entries >= data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge)
        // 如果发现剩余可调度的slot的数量大于等于8
        max_size = data_settings->max_bytes_to_merge_at_max_space_in_pool; //  150GB 最大
    else
        // 如果空闲槽位不足（free slot < 8），则根据空闲槽位与该阈值的比例，进行指数插值计算，得出一个
        // 在 max_bytes_to_merge_at_min_space_in_pool 和 max_bytes_to_merge_at_max_space_in_pool 之间的 max_size。
        /**
         *     M(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge,
         *     8, "When there is less than specified number of free entries in pool (or replicated queue),
         *     start to lower maximum size of merge to process (or to put in queue).
         *     This is to allow small merges to process - not filling the pool with long running merges.", 0) \
         *     按照比例进行插值，free_entries越大，max_size越大
         */
        max_size = static_cast<UInt64>(interpolateExponential(
            data_settings->max_bytes_to_merge_at_min_space_in_pool, // 1MB
            data_settings->max_bytes_to_merge_at_max_space_in_pool, // 150GB
            static_cast<double>(free_entries) / data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge));
    /**
     * 合并的part大小必须小于磁盘剩余可用空间的1/2(DISK_USAGE_COEFFICIENT_TO_SELECT)
     * 但是不能超过150GB
     * UInt64 StoragePolicy::getMaxUnreservedFreeSpace() const
     */
    return std::min(max_size, static_cast<UInt64>(data.getStoragePolicy()->getMaxUnreservedFreeSpace() / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartSizeForMutation() const
{
    const auto data_settings = data.getSettings();
    // 已经占用的task的数量
    // 查看 shared->merge_mutate_executor = std::make_shared<MergeMutateBackgroundExecutor>
    // 当前pending + active中的task的数量
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);
    //     M(UInt64, max_number_of_mutations_for_replica, 0, "Limit the number of part mutations per replica to the specified amount.
    //     Zero means no limit on the number of mutations per replica (the execution can still be constrained by other settings).", 0) \\
    // 最大允许运行的mutation数量，默认是不限制
    if (data_settings->max_number_of_mutations_for_replica > 0 &&
        occupied >= data_settings->max_number_of_mutations_for_replica)
        return 0;

    /// DataPart can be store only at one disk. Get maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace(); // 所有disk中最大的没预留的disk数量
    // 搜索 size_t MergeTreeBackgroundExecutor<Queue>::getMaxTasksCount()
    // 最大任务数量，即给active/pending队列的各自的capacity，这里默认是32
    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount(); // 最大的task数量

    /**
     * Allow mutations only if there are enough threads, leave free threads for merges else
     * M(UInt64, number_of_free_entries_in_pool_to_execute_mutation,
       20, "When there is less than specified number of free entries in pool, do not execute part mutations.
       This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
       如果剩余的允许的task数量(或者是pending，或者是active)大于20，那么就不允许执行mutate，以便给merge留出足够的空间
    **/
    if (occupied <= 1
        || max_tasks_count - occupied >= data_settings->number_of_free_entries_in_pool_to_execute_mutation)
        return static_cast<UInt64>(disk_space / DISK_USAGE_COEFFICIENT_TO_RESERVE); // 最大剩余磁盘空间除以1.1，即最大允许的mutate的大小要稍小于最大磁盘空间大小

    return 0;
}

/**
 * 调用者 ReplicatedMergeTree::mergeSelectingTask
 * 和 StorageReplicatedMergeTree::optimize
 * @return
 */
SelectPartsDecision MergeTreeDataMergerMutator::selectPartsToMerge(
    FutureMergedMutatedPartPtr future_part,
    bool aggressive,
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback, // 搜索 using AllowedMergingPredicate查看 AllowedMergingPredicate
    bool merge_with_ttl_allowed,
    const MergeTreeTransactionPtr & txn, // 没有txn
    String & out_disable_reason,
    const PartitionIdsHint * partitions_hint)
{
    // 第一步，获取可以用于合并的数据部分
    MergeTreeData::DataPartsVector data_parts = getDataPartsToSelectMergeFrom(txn, partitions_hint);

    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    if (data_parts.empty())
    {
        out_disable_reason = "There are no parts in the table";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    // 第二步，检查哪些部分可能满足合并的前提条件 搜索 MergeTreeDataMergerMutator::MergeSelectingInfo MergeTreeDataMergerMutator::getPossibleMergeRanges(
    MergeSelectingInfo info = getPossibleMergeRanges(data_parts, can_merge_callback, txn, out_disable_reason);

    if (info.parts_selected_precondition == 0)
    {
        out_disable_reason = "No parts satisfy preconditions for merge";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    //  第三步，在range确定以后，选择要合并的part，调用查看 MergeTreeDataMergerMutator::selectPartsToMergeFromRanges
    // 这个方法返回的一定是某一个Partition的需要合并的parts
    auto res = selectPartsToMergeFromRanges(future_part, aggressive, max_total_size_to_merge, merge_with_ttl_allowed,
                                            metadata_snapshot, info.parts_ranges, info.current_time, out_disable_reason);

    if (res == SelectPartsDecision::SELECTED) // 选好了，直接返回
        return res;

    // 如果没有成功选择一个PartsRange，那么，选择一个最优的partition，然后对这个partition进行merge
    String best_partition_id_to_optimize = getBestPartitionToOptimizeEntire(info.partitions_info);
    if (!best_partition_id_to_optimize.empty()) // 如果没有找到合适的分区
    {
        // 最佳分区内的所有部分进行优化合并。
        return selectAllPartsToMergeWithinPartition(
            future_part,
            can_merge_callback,
            best_partition_id_to_optimize,
            /*final=*/true,
            metadata_snapshot,
            txn,
            out_disable_reason,
            /*optimize_skip_merged_partitions=*/true);
    }

    if (!out_disable_reason.empty())
        out_disable_reason += ". ";
    out_disable_reason += "There is no need to merge parts according to merge selector algorithm";
    return SelectPartsDecision::CANNOT_SELECT;
}

/**
 * 快速确认有哪些partition需要merge
 * 调用者是void StorageReplicatedMergeTree::mergeSelectingTask()
 * @param max_total_size_to_merge
 * @param can_merge_callback
 * @param merge_with_ttl_allowed
 * @param txn
 * @return
 */
MergeTreeDataMergerMutator::PartitionIdsHint MergeTreeDataMergerMutator::getPartitionsThatMayBeMerged(
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback,
    bool merge_with_ttl_allowed,
    const MergeTreeTransactionPtr & txn) const
{
    PartitionIdsHint res; // 初始化结果集合
    // MergeTreeDataMergerMutator::getDataPartsToSelectMergeFrom
    MergeTreeData::DataPartsVector data_parts = getDataPartsToSelectMergeFrom(txn);
    if (data_parts.empty())
        return res;

    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    String out_reason;
    MergeSelectingInfo info = getPossibleMergeRanges(data_parts, can_merge_callback, txn, out_reason);

    if (info.parts_selected_precondition == 0)
        return res;

    Strings all_partition_ids;
    std::vector<IMergeSelector::PartsRanges> ranges_per_partition;

    String curr_partition;
    for (auto & range : info.parts_ranges)
    {
        if (range.empty())
            continue;
        const String & partition_id = range.front().getDataPartPtr()->info.partition_id;
        if (partition_id != curr_partition)
        {
            curr_partition = partition_id;
            all_partition_ids.push_back(curr_partition);
            ranges_per_partition.emplace_back();
        }
        ranges_per_partition.back().emplace_back(std::move(range));
    }

    for (size_t i = 0; i < all_partition_ids.size(); ++i)
    {
        auto future_part = std::make_shared<FutureMergedMutatedPart>();
        String out_disable_reason;
        /// This method should have been const, but something went wrong... it's const with dry_run = true
        auto status = const_cast<MergeTreeDataMergerMutator *>(this)->selectPartsToMergeFromRanges(
                future_part, /*aggressive*/ false, max_total_size_to_merge, merge_with_ttl_allowed,
                metadata_snapshot, ranges_per_partition[i], info.current_time, out_disable_reason,
                /* dry_run */ true);
        if (status == SelectPartsDecision::SELECTED)
            res.insert(all_partition_ids[i]);
        else
            LOG_TEST(log, "Nothing to merge in partition {}: {}", all_partition_ids[i], out_disable_reason);
    }

    String best_partition_id_to_optimize = getBestPartitionToOptimizeEntire(info.partitions_info);
    if (!best_partition_id_to_optimize.empty())
        res.emplace(std::move(best_partition_id_to_optimize));

    LOG_TRACE(log, "Checked {} partitions, found {} partitions with parts that may be merged: [{}]"
              "(max_total_size_to_merge={}, merge_with_ttl_allowed{})",
              all_partition_ids.size(), res.size(), fmt::join(res, ", "), max_total_size_to_merge, merge_with_ttl_allowed);
    return res;
}

MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::getDataPartsToSelectMergeFrom(
    const MergeTreeTransactionPtr & txn, const PartitionIdsHint * partitions_hint) const
{
    auto res = getDataPartsToSelectMergeFrom(txn);
    if (!partitions_hint)
        return res;

    std::erase_if(res, [partitions_hint](const auto & part)
    {
        return !partitions_hint->contains(part->info.partition_id);
    });
    return res;
}

MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::getDataPartsToSelectMergeFrom(const MergeTreeTransactionPtr & txn) const
{
    MergeTreeData::DataPartsVector res;
    if (!txn) // 如果txn是空的
    {
        /// Simply get all active parts
        // 返回所有的part，只会选择active的part。 搜索 MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage(
        return data.getDataPartsVectorForInternalUsage();
    }

    /// Merge predicate (for simple MergeTree) allows to merge two parts only if both parts are visible for merge transaction.
    /// So at the first glance we could just get all active parts.
    /// Active parts include uncommitted parts, but it's ok and merge predicate handles it.
    /// However, it's possible that some transaction is trying to remove a part in the middle, for example, all_2_2_0.
    /// If parts all_1_1_0 and all_3_3_0 are active and visible for merge transaction, then we would try to merge them.
    /// But it's wrong, because all_2_2_0 may become active again if transaction will roll back.
    /// That's why we must include some outdated parts into `data_part`, more precisely, such parts that removal is not committed.
    MergeTreeData::DataPartsVector active_parts; // 活跃部分
    MergeTreeData::DataPartsVector outdated_parts; // 过时部分

    {
        auto lock = data.lockParts();
        // MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage(
        active_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, lock);
        outdated_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated}, lock);
    }

    ActiveDataPartSet active_parts_set{data.format_version};
    for (const auto & part : active_parts) // 遍历所有的active part，添加到active_parts_set中
        active_parts_set.add(part->name);

    for (const auto & part : outdated_parts) // 遍历所有的outdate part
    {
        /// We don't need rolled back parts.
        /// NOTE When rolling back a transaction we set creation_csn to RolledBackCSN at first
        /// and then remove part from working set, so there's no race condition
        if (part->version.creation_csn == Tx::RolledBackCSN) // 如果部分已经回滚（creation_csn == Tx::RolledBackCSN），则跳过该部分。
            continue;

        /// We don't need parts that are finally removed.
        /// NOTE There's a minor race condition: we may get UnknownCSN if a transaction has been just committed concurrently.
        /// But it's not a problem if we will add such part to `data_parts`.
        if (part->version.removal_csn != Tx::UnknownCSN) // 如果部分已经标记为要删除（removal_csn != Tx::UnknownCSN），则也跳过。
            continue;

        active_parts_set.add(part->name); // 将符合条件的部分添加到 active_parts_set 中，这意味着即使部分处于过时状态，只要它们的删除尚未最终确定，它们仍然可能被视为活跃部分。
    }
    // 使用 remove_pred 函数对活跃和过时部分进行筛选，移除那些不再被 active_parts_set 认为是活跃的部分。这确保了只有当前确实可以合并的部分才会被保留下来。
    /// Restore "active" parts set from selected active and outdated parts
    auto remove_pred = [&](const MergeTreeData::DataPartPtr & part) -> bool
    {
        return active_parts_set.getContainingPart(part->info) != part->name;
    };

    std::erase_if(active_parts, remove_pred);

    std::erase_if(outdated_parts, remove_pred);

    MergeTreeData::DataPartsVector data_parts;
    std::merge(
        active_parts.begin(),
        active_parts.end(),
        outdated_parts.begin(),
        outdated_parts.end(),
        std::back_inserter(data_parts),
        MergeTreeData::LessDataPart());

    return data_parts;
}

/**
 * using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &,
    const MergeTreeData::DataPartPtr &,
    const MergeTreeTransaction *,
    String &)>;
 * @param data_parts
 * @param can_merge_callback
 * @param txn
 * @param out_disable_reason
 * @return
 */
MergeTreeDataMergerMutator::MergeSelectingInfo MergeTreeDataMergerMutator::getPossibleMergeRanges(
    const MergeTreeData::DataPartsVector & data_parts,
    const AllowedMergingPredicate & can_merge_callback, //  bool BaseMergePredicate<VirtualPartsT, MutationsStateT>::operator()(
    const MergeTreeTransactionPtr & txn,
    String & out_disable_reason) const
{
    MergeSelectingInfo res; // 初始化结果集 res:

    res.current_time = std::time(nullptr);
    // using PartsRanges = std::vector<PartsRange>;
    /**
     * 例如: 如果一个分区 P1 有多个可以合并的部分（如 part_1 到 part_3，part_5 到 part_7），那么
     * parts_ranges 可能会包含两个范围：[{part_1, part_2, part_3}, {part_5, part_6, part_7}]。
    */
    IMergeSelector::PartsRanges & parts_ranges = res.parts_ranges;

    StoragePolicyPtr storage_policy = data.getStoragePolicy();
    /// Volumes with stopped merges are extremely rare situation.
    /// Check it once and don't check each part (this is bad for performance).
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    const String * prev_partition_id = nullptr;
    /// Previous part only in boundaries of partition frame
    const MergeTreeData::DataPartPtr * prev_part = nullptr;

    /// collect min_age for each partition while iterating parts
    // 搜索 struct PartitionInfo
    PartitionsInfo & partitions_info = res.partitions_info; // 遍历过程中收集每一个partition的min_age
    // using DataPartPtr = std::shared_ptr<const DataPart>;
    // using DataPart = IMergeTreeDataPart;
    for (const MergeTreeData::DataPartPtr & part : data_parts) // 遍历每一个选择出来的parts
    {
        const String & partition_id = part->info.partition_id; //  获取这个part的partition id
        // 这里可以看到，PartsRanges中的每一个PartsRange一定属于同一个partition，不可能存在一个PartsRange中的不同的part属于不同的partition
        if (!prev_partition_id || partition_id != *prev_partition_id) // 确定是否需要开始一个新的分区区间。如果分区 ID 发生变化，或当前区间为空，则开始新的区间PartsRange。
        {
            if (parts_ranges.empty() || !parts_ranges.back().empty()) // 如果整个区间为空，或者整个区间不为空，且最后一个区间也不为空，需要创建一个
                parts_ranges.emplace_back();

            /// New partition frame.
            prev_partition_id = &partition_id;
            prev_part = nullptr; // 由于开始了一个新的partition，那么prev_part为空，代表这个part是当前partition的第一个part
        }

        /// Check predicate only for the first part in each range.
        if (!prev_part) // 如果这是当前partition的第一个part
        {
            /* Parts can be merged with themselves for TTL needs for example.
            * So we have to check if this part is currently being inserted with quorum and so on and so forth.
            * Obviously we have to check it manually only for the first part
            * of each partition because it will be automatically checked for a pair of parts. */
            if (!can_merge_callback(nullptr, part, txn.get(), out_disable_reason))
                continue;

            /// This part can be merged only with next parts (no prev part exists), so start
            /// new interval if previous was not empty.
            if (!parts_ranges.back().empty()) //  parts_ranges 中最后一个区间是否为空。如果该区间不为空，说明已有部分数据被添加到这个区间中。
                parts_ranges.emplace_back(); // 如果最后一个区间不为空，使用 emplace_back() 向 parts_ranges 添加一个新的空区间，即一个新的IMergeSelector::PartsRanges对象
        }
        else // 这不是当前partition的第一个part
        {
            /// If we cannot merge with previous part we had to start new parts
            /// interval (in the same partition)
            if (!can_merge_callback(*prev_part, part, txn.get(), out_disable_reason))
            {  // 我们不能与前一个part合并，因此我们需要开启一个新的part interval
                /// Now we have no previous part
                prev_part = nullptr;

                /// Mustn't be empty
                assert(!parts_ranges.back().empty());

                /// Some parts cannot be merged with previous parts and also cannot be merged with themselves,
                /// for example, merge is already assigned for such parts, or they participate in quorum inserts
                /// and so on.
                /// Also we don't start new interval here (maybe all next parts cannot be merged and we don't want to have empty interval)
                if (!can_merge_callback(nullptr, part, txn.get(), out_disable_reason))
                    continue; // 检查当前部分是否可以单独合并（即不与前一个部分合并）。如果不能单独合并，则跳过当前循环的剩余部分，不处理这个part
                // 执行到这里，意味着这个part不能与前面的part合并，但是可以与自己和后续的部分合并
                /// Starting new interval in the same partition
                parts_ranges.emplace_back();// 在partition内部开始一个新的区间，即一个新的IMergeSelector::PartsRanges对象
            }
        }

        IMergeSelector::Part part_info;
        part_info.size = part->getBytesOnDisk();
        part_info.age = res.current_time - part->modification_time; // 这个part的修改距离当前时间
        part_info.level = part->info.level;
        part_info.data = &part;
        part_info.ttl_infos = &part->ttl_infos;
        part_info.compression_codec_desc = part->default_codec->getFullCodecDesc();
        part_info.shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true;

        auto & partition_info = partitions_info[partition_id];
        partition_info.min_age = std::min(partition_info.min_age, part_info.age); // 重算最小的age

        ++res.parts_selected_precondition;
        /**
         * back() 方法返回 parts_ranges 中的最后一个区间（即最后一个 IMergeSelector::PartsRanges 对象）。
         * 如果 parts_ranges 是一个二维数组，那么 parts_ranges.back() 就表示这个二维数组的最后一行。
         */
        parts_ranges.back().emplace_back(part_info); //往结果中插入数据

        /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part) // 如果prev_part不为空，那么检查两个part之间是否是交集或者包含关系，如果有交集或者包含关系都不行
        {
            if (part->info.contains((*prev_part)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} contains previous part {}", part->name, (*prev_part)->name);

            if (!part->info.isDisjoint((*prev_part)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}", part->name, (*prev_part)->name);
        }

        prev_part = &part;
    }

    return res;
}

/**
 * 调用者 MergeTreeDataMergerMutator::selectPartsToMerge
 * @param future_part 在方法调用完成以后，将选定的某一个partition的part存放到future_part中
 * @param aggressive
 * @param max_total_size_to_merge
 * @param merge_with_ttl_allowed
 * @param metadata_snapshot
 * @param parts_ranges
 * @param current_time
 * @param out_disable_reason
 * @param dry_run
 * @return
 */
SelectPartsDecision MergeTreeDataMergerMutator::selectPartsToMergeFromRanges(
    FutureMergedMutatedPartPtr future_part,
    bool aggressive,
    size_t max_total_size_to_merge, // 当前计算好的最大可合并的part数量
    bool merge_with_ttl_allowed,
    const StorageMetadataPtr & metadata_snapshot,
    //  using PartsRanges = std::vector<PartsRange>;
    const IMergeSelector::PartsRanges & parts_ranges, // 已经计算好的part rage.
    const time_t & current_time,
    String & out_disable_reason,
    bool dry_run)
{
    const auto data_settings = data.getSettings();
    IMergeSelector::PartsRange parts_to_merge; // 初始化一个空的 parts_to_merge 容器，用于存储选择出的可合并部分。
    // 首先检查是否有任何 TTL 相关的合并需要执行（例如删除过期数据或者重新压缩数据）。
    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && !ttl_merges_blocker.isCancelled())
    {
        // 使用 TTLDeleteMergeSelector 或 TTLRecompressMergeSelector 来选择适合的part进行合并。
        // 如果找到了符合条件的part，就直接将这些part标记为需要 TTL 相关的合并，并将其返回。
        /// TTL delete is preferred to recompression
        TTLDeleteMergeSelector drop_ttl_selector(
                next_delete_ttl_merge_times_by_partition,
                current_time,
                data_settings->merge_with_ttl_timeout,
                /*only_drop_parts*/ true,
                dry_run);

        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
        parts_to_merge = drop_ttl_selector.select(parts_ranges, data_settings->max_bytes_to_merge_at_max_space_in_pool);
        if (!parts_to_merge.empty())
        {
            future_part->merge_type = MergeType::TTLDelete;
        }
        else if (!data_settings->ttl_only_drop_parts)
        {
            TTLDeleteMergeSelector delete_ttl_selector(
                next_delete_ttl_merge_times_by_partition,
                current_time,
                data_settings->merge_with_ttl_timeout,
                /*only_drop_parts*/ false,
                dry_run);

            parts_to_merge = delete_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part->merge_type = MergeType::TTLDelete;
        }

        if (parts_to_merge.empty() && metadata_snapshot->hasAnyRecompressionTTL())
        {
            TTLRecompressMergeSelector recompress_ttl_selector(
                    next_recompress_ttl_merge_times_by_partition,
                    current_time,
                    data_settings->merge_with_recompression_ttl_timeout,
                    metadata_snapshot->getRecompressionTTLs(),
                    dry_run);

            parts_to_merge = recompress_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part->merge_type = MergeType::TTLRecompress;
        }
    }

    if (parts_to_merge.empty())
    {
        SimpleMergeSelector::Settings merge_settings;
        /// Override value from table settings
        /**
         *     M(UInt64, max_parts_to_merge_at_once, 100, "Max amount of parts which can be merged at once (0 - disabled).
         *     Doesn't affect OPTIMIZE FINAL query.", 0) \
         *     最大可以在一个合并中进行的 source parts 的数量。但是如果是来自于OPTIMIZE，这个值会被忽略
         */
        merge_settings.max_parts_to_merge_at_once = data_settings->max_parts_to_merge_at_once;
        /**
         *     M(Bool, min_age_to_force_merge_on_partition_only, false, "Whether min_age_to_force_merge_seconds should be
         *     applied only on the entire partition and not on subset.", false) \
         *
         */
        if (!data_settings->min_age_to_force_merge_on_partition_only)
            /**
             *     M(UInt64, min_age_to_force_merge_seconds, 0, "If all parts in a certain range are older than this value,
             *     range will be always eligible for merging. Set to 0 to disable.", 0) \
             */
            merge_settings.min_age_to_force_merge = data_settings->min_age_to_force_merge_seconds;

        if (aggressive)
            merge_settings.base = 1; // 如果是aggressive，那么设置base = 1
        // 进行常规合并. IMergeSelector::PartsRange parts_to_merge
        // SimpleMergeSelector::PartsRange SimpleMergeSelector::select
        parts_to_merge = SimpleMergeSelector(merge_settings)
                            .select(parts_ranges, max_total_size_to_merge); // 从众多的PartsRange中选择一个最优的PartsRange(一个PartsRange只属于一个Partition)

        /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
        if (parts_to_merge.size() == 1) // 这个PartsRange中只有一个part
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: merge selector returned only one part to merge");

        if (parts_to_merge.empty())
        {
            out_disable_reason = "Did not find any parts to merge (with usual merge selectors)";
            return SelectPartsDecision::CANNOT_SELECT;
        }
    }

    MergeTreeData::DataPartsVector parts;
    parts.reserve(parts_to_merge.size());
    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = part_info.getDataPartPtr();
        parts.push_back(part);
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part->assign(std::move(parts));
    return SelectPartsDecision::SELECTED;
}

/**
 * 当我们无法在众多的PartsRanges中选择出一个合适的PartsRange进行合并，那么，我们尝试在这些parts所属的Partition中选择一个Partition进行optimize
 * @param partitions_info
 * @return
 */
String MergeTreeDataMergerMutator::getBestPartitionToOptimizeEntire(
    const PartitionsInfo & partitions_info) const
{
    const auto & data_settings = data.getSettings();
    /**
     *     M(Bool, min_age_to_force_merge_on_partition_only, false, "Whether min_age_to_force_merge_seconds should be
     *     applied only on the entire partition and not on subset.", false) \
     */
    if (!data_settings->min_age_to_force_merge_on_partition_only)
        return {};
    /**
     * M(UInt64, min_age_to_force_merge_seconds, 0, "If all parts in a certain range are older than this value,
     * range will be always eligible for merging. Set to 0 to disable.", 0) \
     * 可以进行partition内部的force merge的最小年龄。年龄必须大于这个值才能进行force merge
     */
    if (!data_settings->min_age_to_force_merge_seconds)
        return {};
    // 当前pending + active中的task的数量
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);
    /**
     * 最大任务数量，即给active/pending队列的各自的capacity，这里默认是32
     */
    size_t max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    /**
     *     M(UInt64, number_of_free_entries_in_pool_to_execute_optimize_entire_partition, 25,
     *     "When there is less than specified number of free entries in pool,
     *     do not try to execute optimize entire partition with
     *     a merge (this merge is created when set min_age_to_force_merge_seconds > 0 and min_age_to_force_merge_on_partition_only = true).
     *     This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
     *     需要区分 data_settings->number_of_free_entries_in_pool_to_execute_mutation
     */
    if (occupied > 1 && max_tasks_count - occupied < data_settings->number_of_free_entries_in_pool_to_execute_optimize_entire_partition)
    {
        LOG_INFO(
            log,
            "Not enough idle threads to execute optimizing entire partition. See settings "
            "'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' "
            "and 'background_pool_size'");
        return {};
    }
    // 对于每一个PartitionInfo，min_age是这个PartitionInfo下面的所有part最小的age
    // 这里，是选择min_age最大的partition，即还是prefer大龄的age的Partition进行合并
    auto best_partition_it = std::max_element(
        partitions_info.begin(),
        partitions_info.end(),
        [](const auto & e1, const auto & e2) { return e1.second.min_age < e2.second.min_age; });

    assert(best_partition_it != partitions_info.end());
    // 如果即使年龄最大的partition的年龄也小于min_age_to_force_merge_seconds，那么显然做不了。
    if (static_cast<size_t>(best_partition_it->second.min_age) < data_settings->min_age_to_force_merge_seconds)
        return {};

    return best_partition_it->first;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    FutureMergedMutatedPartPtr future_part,
    const AllowedMergingPredicate & can_merge,
    const String & partition_id,
    bool final,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeTransactionPtr & txn,
    String & out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition_id);

    if (parts.empty())
    {
        out_disable_reason = "There are no parts inside partition";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    if (!final && parts.size() == 1)
    {
        out_disable_reason = "There is only one part inside partition";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    /// If final, optimize_skip_merged_partitions is true and we have only one part in partition with level > 0
    /// than we don't select it to merge. But if there are some expired TTL then merge is needed
    if (final && optimize_skip_merged_partitions && parts.size() == 1 && parts[0]->info.level > 0 &&
        (!metadata_snapshot->hasAnyTTL() || parts[0]->checkAllTTLCalculated(metadata_snapshot)))
    {
        out_disable_reason = "Partition skipped due to optimize_skip_merged_partitions";
        return SelectPartsDecision::NOTHING_TO_MERGE;
    }

    auto it = parts.begin();
    auto prev_it = it;

    UInt64 sum_bytes = 0;
    while (it != parts.end())
    {
        /// For the case of one part, we check that it can be merged "with itself".
        if ((it != parts.begin() || parts.size() == 1) && !can_merge(*prev_it, *it, txn.get(), out_disable_reason))
        {
            return SelectPartsDecision::CANNOT_SELECT;
        }

        sum_bytes += (*it)->getBytesOnDisk();

        prev_it = it;
        ++it;
    }

    auto available_disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    /// Enough disk space to cover the new merge with a margin.
    auto required_disk_space = sum_bytes * DISK_USAGE_COEFFICIENT_TO_SELECT;
    if (available_disk_space <= required_disk_space)
    {
        time_t now = time(nullptr);
        if (now - disk_space_warning_time > 3600)
        {
            disk_space_warning_time = now;
            LOG_WARNING(log,
                "Won't merge parts from {} to {} because not enough free space: {} free and unreserved"
                ", {} required now (+{}% on overhead); suppressing similar warnings for the next hour",
                parts.front()->name,
                (*prev_it)->name,
                ReadableSize(available_disk_space),
                ReadableSize(sum_bytes),
                static_cast<int>((DISK_USAGE_COEFFICIENT_TO_SELECT - 1.0) * 100));
        }

        out_disable_reason = fmt::format("Insufficient available disk space, required {}", ReadableSize(required_disk_space));
        return SelectPartsDecision::CANNOT_SELECT;
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part->assign(std::move(parts));

    return SelectPartsDecision::SELECTED;
}


MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::selectAllPartsFromPartition(const String & partition_id)
{
    MergeTreeData::DataPartsVector parts_from_partition;

    MergeTreeData::DataParts data_parts = data.getDataPartsForInternalUsage();

    for (const auto & current_part : data_parts)
    {
        if (current_part->info.partition_id != partition_id)
            continue;

        parts_from_partition.push_back(current_part);
    }

    return parts_from_partition;
}

/// parts should be sorted.
MergeTaskPtr MergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    FutureMergedMutatedPartPtr future_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeList::Entry * merge_entry,
    std::unique_ptr<MergeListElement> projection_merge_list_element,
    TableLockHolder,
    time_t time_of_merge,
    ContextPtr context,
    ReservationSharedPtr space_reservation,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    const MergeTreeData::MergingParams & merging_params,
    const MergeTreeTransactionPtr & txn,
    bool need_prefix,
    IMergeTreeDataPart * parent_part,
    const String & suffix)
{
    return std::make_shared<MergeTask>(
        future_part,
        const_cast<StorageMetadataPtr &>(metadata_snapshot),
        merge_entry,
        std::move(projection_merge_list_element),
        time_of_merge,
        context,
        space_reservation,
        deduplicate,
        deduplicate_by_columns,
        cleanup,
        merging_params,
        need_prefix,
        parent_part,
        suffix,
        txn,
        &data,
        this,
        &merges_blocker,
        &ttl_merges_blocker);
}


MutateTaskPtr MergeTreeDataMergerMutator::mutatePartToTemporaryPart(
    FutureMergedMutatedPartPtr future_part,
    StorageMetadataPtr metadata_snapshot,
    MutationCommandsConstPtr commands,
    MergeListEntry * merge_entry,
    time_t time_of_mutation,
    ContextPtr context,
    const MergeTreeTransactionPtr & txn,
    ReservationSharedPtr space_reservation,
    TableLockHolder & holder,
    bool need_prefix)
{
    return std::make_shared<MutateTask>(
        future_part,
        metadata_snapshot,
        commands,
        merge_entry,
        time_of_mutation,
        context,
        space_reservation,
        holder,
        txn,
        data,
        *this,
        merges_blocker,
        need_prefix
    );
}


MergeTreeData::DataPartPtr MergeTreeDataMergerMutator::renameMergedTemporaryPart(
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const MergeTreeData::DataPartsVector & parts,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData::Transaction & out_transaction)
{
    /// Some of source parts was possibly created in transaction, so non-transactional merge may break isolation.
    if (data.transactions_enabled.load(std::memory_order_relaxed) && !txn)
        throw Exception(ErrorCodes::ABORTED, "Cancelling merge, because it was done without starting transaction,"
                                             "but transactions were enabled for this table");

    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, out_transaction);

    /// Let's check that all original parts have been deleted and only them.
    if (replaced_parts.size() != parts.size())
    {
        /** This is normal, although this happens rarely.
         *
         * The situation - was replaced 0 parts instead of N can be, for example, in the following case
         * - we had A part, but there was no B and C parts;
         * - A, B -> AB was in the queue, but it has not been done, because there is no B part;
         * - AB, C -> ABC was in the queue, but it has not been done, because there are no AB and C parts;
         * - we have completed the task of downloading a B part;
         * - we started to make A, B -> AB merge, since all parts appeared;
         * - we decided to download ABC part from another replica, since it was impossible to make merge AB, C -> ABC;
         * - ABC part appeared. When it was added, old A, B, C parts were deleted;
         * - AB merge finished. AB part was added. But this is an obsolete part. The log will contain the message `Obsolete part added`,
         *   then we get here.
         *
         * When M > N parts could be replaced?
         * - new block was added in ReplicatedMergeTreeSink;
         * - it was added to working dataset in memory and renamed on filesystem;
         * - but ZooKeeper transaction that adds it to reference dataset in ZK failed;
         * - and it is failed due to connection loss, so we don't rollback working dataset in memory,
         *   because we don't know if the part was added to ZK or not
         *   (see ReplicatedMergeTreeSink)
         * - then method selectPartsToMerge selects a range and sees, that EphemeralLock for the block in this part is unlocked,
         *   and so it is possible to merge a range skipping this part.
         *   (NOTE: Merging with part that is not in ZK is not possible, see checks in 'createLogEntryToMergeParts'.)
         * - and after merge, this part will be removed in addition to parts that was merged.
         */
        LOG_WARNING(log, "Unexpected number of parts removed when adding {}: {} instead of {}", new_data_part->name, replaced_parts.size(), parts.size());
    }
    else
    {
        for (size_t i = 0; i < parts.size(); ++i)
            if (parts[i]->name != replaced_parts[i]->name)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected part removed when adding {}: {} instead of {}",
                    new_data_part->name, replaced_parts[i]->name, parts[i]->name);
    }

    LOG_TRACE(log, "Merged {} parts: [{}, {}] -> {}", parts.size(), parts.front()->name, parts.back()->name, new_data_part->name);
    return new_data_part;
}


size_t MergeTreeDataMergerMutator::estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts)
{
    size_t res = 0;
    time_t current_time = std::time(nullptr);
    for (const MergeTreeData::DataPartPtr & part : source_parts)
    {
        /// Exclude expired parts
        time_t part_max_ttl = part->ttl_infos.part_max_ttl;
        if (part_max_ttl && part_max_ttl <= current_time)
            continue;

        res += part->getBytesOnDisk();
    }

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

}
