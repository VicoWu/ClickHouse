#include <Common/ISlotControl.h>
#include <Common/ConcurrencyControl.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event ConcurrencyControlSlotsGranted;
    extern const Event ConcurrencyControlSlotsDelayed;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlSlotsAcquiredNonCompeting;
    extern const Event ConcurrencyControlQueriesDelayed;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlAcquiredNonCompeting;
    extern const Metric ConcurrencyControlSoftLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ConcurrencyControlState::ConcurrencyControlState()
    : max_concurrency_metric(CurrentMetrics::ConcurrencyControlSoftLimit, 0)
{
}

SlotCount ConcurrencyControlState::available(std::unique_lock<std::mutex> &) const
{
    if (cur_concurrency < max_concurrency)
        return max_concurrency - cur_concurrency;
    return 0;
}


// RoundRobin Scheduler（round_robin）：min 计入全局配额，可能 oversubscription
ConcurrencyControlRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_, size_t slot_id_)
    : IAcquiredSlot(slot_id_)
    , allocation(std::move(allocation_))
    , acquired_slot_increment(CurrentMetrics::ConcurrencyControlAcquired)
{
}

ConcurrencyControlRoundRobinScheduler::Slot::~Slot()
{
    static_cast<ConcurrencyControlRoundRobinScheduler::Allocation&>(*allocation).release();
}

/**
 * 在 ConcurrencyControlRoundRobinScheduler::allocate() 中被构造
 * @param parent_
 * @param limit_
 * @param granted_
 * @param waiter_
 */
ConcurrencyControlRoundRobinScheduler::Allocation::Allocation(ConcurrencyControlRoundRobinScheduler & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_)
    : parent(parent_)
    , limit(limit_)
    , allocated(granted_)
    , granted(granted_)
    , waiter(waiter_)
{
    if (allocated < limit)
        *waiter = this;
}

ConcurrencyControlRoundRobinScheduler::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

/**
 * ConcurrencyControlRoundRobinScheduler的slot都是competing的slot
 */
[[nodiscard]] AcquiredSlotPtr ConcurrencyControlRoundRobinScheduler::Allocation::tryAcquire()
{
    // round_robin：全部 max 个 slot 均 competing，无 noncompeting 分支
    SlotCount value = granted.load();
    // granted：已 grant、尚未 acquire 的 slot 数；为 0 返回空，调用方可稍后重试
    while (value)
    {
        // CAS 无锁取走一个 slot；争用失败则用新 value 重试
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            std::unique_lock lock{mutex};
            // Slot 析构时 release() 归还全局 slot；last_slot_id 为本查询内线程编号
            return AcquiredSlotPtr(new Slot(shared_from_this(), last_slot_id++)); // can't use std::make_shared due to private ctor
        }
    }
    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlRoundRobinScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

// Grant single slot to allocation returns true iff more slot(s) are required
bool ConcurrencyControlRoundRobinScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControlRoundRobinScheduler::Allocation::release()
{
    parent.release(1); // 它的parent 是对应的 ConcurrencyControlRoundRobinScheduler
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControlRoundRobinScheduler::ConcurrencyControlRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_)
    : parent(parent_)
    , state(state_)
    , cur_waiter(waiters.end())
{
}

ConcurrencyControlRoundRobinScheduler::~ConcurrencyControlRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

SlotAllocationPtr ConcurrencyControlRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Try allocate slots up to requested `max` (as availability allows)
    // 注意 std::max(min, ...)：min 无条件计入 cur_concurrency，即使全局已满也会 oversubscribe
    // 注意这里的max的意思是，即使 state.available(lock)) < min，也会给min个，这就会造成超售，但是min必须无条件给
    // grant的含义指的是应该给多少，但是可能会因为资源不够而无法立刻给到
    SlotCount granted = std::max(min, std::min(max, state.available(lock)));
    state.cur_concurrency += granted; // 计算grant的数量
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < max) // 如果grant的数量少于请求的最大数量
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, max - granted); //发生了delay的slots的数量
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed); // 发生了delay的query数量
        // using SlotAllocationPtr = std::shared_ptr<ISlotAllocation>;
        // ConcurrencyControlRoundRobinScheduler::Allocation
        return SlotAllocationPtr(new Allocation(*this, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */))); // 构造一个SlotAllocation
    }
    else
    {   // 直接grant到了最大请求数量，都不需要进入waiters队列
        return SlotAllocationPtr(new Allocation(*this, max, granted));
    }
}

void ConcurrencyControlRoundRobinScheduler::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock);
}

void ConcurrencyControlRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock); // ConcurrencyControlRoundRobinScheduler的parent是对应的ConcurrencyControl
}

// Round-robin scheduling of available slots among waiting allocations
void ConcurrencyControlRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
{
    while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
    {
        state.cur_concurrency++;
        if (cur_waiter == waiters.end())
            cur_waiter = waiters.begin();
        Allocation * allocation = *cur_waiter;
        if (allocation->grant())
            ++cur_waiter;
        else
            cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
    }
}


// FairRoundRobin Scheduler（fair_round_robin）：min 为 non-competing，不占全局配额，无 oversubscription
ConcurrencyControlFairRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_, bool competing_, size_t slot_id_)
    : IAcquiredSlot(slot_id_)
    , allocation(std::move(allocation_))
    , competing(competing_) // 是否是competing slot，只有当是competing的slot，析构的时候才会改变全局配额
    , acquired_slot_increment(competing ? CurrentMetrics::ConcurrencyControlAcquired : CurrentMetrics::ConcurrencyControlAcquiredNonCompeting)
{
}

ConcurrencyControlFairRoundRobinScheduler::Slot::~Slot()
{
    // ConcurrencyControlFairRoundRobinScheduler只负责Competing slot, 查看allocate()
    if (competing) // 只有当这个slot是competing的，才需要release， 而RoundRobinScheduler则没有这个判断
        static_cast<ConcurrencyControlFairRoundRobinScheduler::Allocation&>(*allocation).release();
}

/**
 * 在 ConcurrencyControlFairRoundRobinScheduler::allocate() 中被构造
 * @param parent_
 * @param min_
 * @param max
 * @param granted_
 * @param waiter_
 */
ConcurrencyControlFairRoundRobinScheduler::Allocation::Allocation(ConcurrencyControlFairRoundRobinScheduler & parent_, SlotCount min_, SlotCount max, SlotCount granted_, Waiters::iterator waiter_)
    : parent(parent_)
    , min(min_)
    , limit(max - min)
    , allocated(granted_)
    , noncompeting(min) // noncompeting在grant的时候不参与竞争，但是在acquire()的时候需要单独优先分配
    , granted(granted_)
    , waiter(waiter_)
{
    if (allocated < limit)
        *waiter = this;
}

ConcurrencyControlFairRoundRobinScheduler::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

/**
 * 需要区分competing和non-competing，因为它管理的slot包含了competing和non-competing
 * 每次调用一次tryAcquire()，都只尝试获取一个Slot。这里优先获取non-competing slot，只有当所有的non-competing slot都获取了，才会尝试获取competing slot
 * 如果non-competing还没完成分配，先分配noncompeting，分配完了non-competing，再分配 competing
 * @return
 */
[[nodiscard]] AcquiredSlotPtr ConcurrencyControlFairRoundRobinScheduler::Allocation::tryAcquire()
{
    // First try acquire non-competing slot (if any)
    SlotCount value = noncompeting.load();
    // noncompeting：前 min 个 slot，不计入全局 cur_concurrency，构造时一次性就绪
    while (value)
    {
        // CAS 取走 non-competing slot；通常 master 线程走此路径
        // 无锁方式实现同步
        if (noncompeting.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquiredNonCompeting, 1);
            std::unique_lock lock{mutex};
            // competing=false：Slot 析构不 release 全局配额（non-competing 不占全局计数）
            // using AcquiredSlotPtr = std::shared_ptr<IAcquiredSlot>;
            return AcquiredSlotPtr(new Slot(shared_from_this(), false, last_slot_id++)); // can't use std::make_shared due to private ctor
        }
    }

    // If all non-competing slots are already acquired - try acquire granted (competing) slot
    value = granted.load();
    // granted：货架上「已 grant、尚未被线程取走」的 competing slot 数量；为 0 则直接返回空
    while (value)
    {
        // CAS：若 granted 仍等于 value，则原子减 1（取走一个 slot）；失败说明其他线程抢先取走，用更新后的 value 重试
        // 全程不加 mutex，多线程并发 tryAcquire 时走无锁快路径
        if (granted.compare_exchange_strong(value, value - 1))
        {
            // 获得了锁
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            // 取走 slot 成功后才加锁：仅为 last_slot_id++ 与构造 Slot 提供互斥（allocated 不在此修改）
            std::unique_lock lock{mutex};
            // competing=true：Slot 析构时会 release()，归还全局 cur_concurrency 并触发 schedule
            // shared_from_this() 保证 Allocation 在 Slot 存活期间不被析构；last_slot_id 为本查询内线程编号
            return AcquiredSlotPtr(new Slot(shared_from_this(), true, last_slot_id++)); // can't use std::make_shared due to private ctor
        }
    }

    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlFairRoundRobinScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

// Grant single slot to allocation returns true iff more slot(s) are required
// 这里的grant是grant **一个** slot
bool ConcurrencyControlFairRoundRobinScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;// 这里的返回值的含义是: 是否还有更多的请求存在
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControlFairRoundRobinScheduler::Allocation::release()
{
    // 调用 ConcurrencyControlFairRoundRobinScheduler::release()，这里每次调用都只release一个slot
    parent.release(1); // ConcurrencyControlFairRoundRobinScheduler::Allocation的parent是 ConcurrencyControlFairRoundRobinScheduler
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControlFairRoundRobinScheduler::ConcurrencyControlFairRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_)
    : parent(parent_)
    , state(state_)
    , cur_waiter(waiters.end())
{
}

ConcurrencyControlFairRoundRobinScheduler::~ConcurrencyControlFairRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

SlotAllocationPtr ConcurrencyControlFairRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Try allocate slots up to requested `max - min` (as availability allows).
    // Do not count `min` slots towards the limit. They are NOT considered as taking part in competition.
    // min 不占全局配额，min是non-competing的，而max-min是competing的
    SlotCount limit = max - min;
    SlotCount granted = std::min(limit, state.available(lock)); // 最多只能grant到available的slot数量
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);
    // 下面的分配过程和 ConcurrencyControlRoundRobinScheduler 一样
    // Create allocation and start waiting if more slots are required
    if (granted < limit)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, limit - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        return SlotAllocationPtr(new Allocation(*this, min, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
    }
    else
    {
        return SlotAllocationPtr(new Allocation(*this, min, max, granted));
    }
}

/**
 * ConcurrencyControlFairRoundRobinScheduler::free和ConcurrencyControlRoundRobinScheduler::free一模一样，没有区别
 * @param allocation
 */
void ConcurrencyControlFairRoundRobinScheduler::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock);
}

void ConcurrencyControlFairRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount; // cur_concurrency减去slot的数量
    parent.schedule(lock); // 调度下一个
}

// Round-robin scheduling of available slots among waiting allocations
// 代码和ConcurrencyControlRoundRobinScheduler::schedule() 一模一样
void ConcurrencyControlFairRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
{
    while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
    {
        // 进入循环，代表还有剩余可分配资源
        state.cur_concurrency++;
        if (cur_waiter == waiters.end())
            cur_waiter = waiters.begin();
        Allocation * allocation = *cur_waiter; // 找到一个分配请求
        if (allocation->grant()) // 每次只grant()一个slot，就转移到下一个allocation。
            // 返回True，说明这个allocation还有更多的请求存在，因此，不把这个allocation从waiters中移除
            ++cur_waiter;
        else
            // 这个allocation已经不需要分配更多资源了，因此从sheudler的队列中移除
            cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
    }
}

/**
 * 可以看到，一个ClickHouse Server会构造一个 ConcurrencyControl对象，
 * 一个ConcurrencyControl对象包含了一个 RoundRobinScheduler和一个FairRoundRobinScheduler对象，即两个Scheduler共享一个ConcurrencyControl对象
 */
ConcurrencyControl::ConcurrencyControl()
    : round_robin(*this, state)
    , fair_round_robin(*this, state)
{
}

ConcurrencyControl & ConcurrencyControl::instance()
{
    static ConcurrencyControl result;
    return result;
}

/**
 * min = master_threads
 * max = num_threads
 * @param min
 * @param max
 * @return
 */
[[nodiscard]] SlotAllocationPtr ConcurrencyControl::allocate(SlotCount min, SlotCount max)
{
    if (min > max)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConcurrencyControl: invalid allocation requirements");

    std::unique_lock lock{state.mutex};
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            return round_robin.allocate(lock, min, max);
        case Scheduler::FairRoundRobin:
            return fair_round_robin.allocate(lock, min, max);
    }
}

void ConcurrencyControl::setMaxConcurrency(SlotCount value)
{
    std::unique_lock lock{state.mutex};
    state.max_concurrency = std::max<SlotCount>(1, value); // never allow max_concurrency to be zero
    state.max_concurrency_metric.changeTo(state.max_concurrency == UnlimitedSlots ? 0 : state.max_concurrency);
    schedule(lock);
}

bool ConcurrencyControl::setScheduler(const String & value)
{
    std::unique_lock lock{state.mutex};
    if (value == "fair_round_robin")
    {
        scheduler = Scheduler::FairRoundRobin;
        return true;
    }
    if (value == "round_robin")
    {
        scheduler = Scheduler::RoundRobin;
        return true;
    }
    return false; // invalid value - stick to the current scheduler
}

String ConcurrencyControl::getScheduler() const
{
    std::unique_lock lock{state.mutex};
    switch (scheduler)
    {
        case Scheduler::RoundRobin: return "round_robin";
        case Scheduler::FairRoundRobin: return "fair_round_robin";
    }
}

void ConcurrencyControl::schedule(std::unique_lock<std::mutex> & lock)
{
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            fair_round_robin.schedule(lock); // first schedule from old scheduler (works only during transition period)
            round_robin.schedule(lock);
            return;
        case Scheduler::FairRoundRobin:
            round_robin.schedule(lock); // first schedule from old scheduler (works only during transition period)
            fair_round_robin.schedule(lock);
            return;
    }
}

}
