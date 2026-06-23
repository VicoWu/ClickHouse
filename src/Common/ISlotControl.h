#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <base/types.h>
#include <base/defines.h>
#include <boost/core/noncopyable.hpp>


namespace DB
{

// Interfaces for abstract "slot" allocation and control.
// Slot is a virtual entity existing in a limited amount (CPUs or memory chunks, etc).
//
// Every slot can be in one of the following states:
//  * free: slot is available to be allocated.
//  * allocated: slot is allocated to a specific ISlotAllocation.
//
// Allocated slots can be in one of the following states:
//  * granted: allocated, but not yet acquired.
//  * acquired: a granted slot becomes acquired (e.g. by using IAcquiredSlot).
//
// Example for CPU (see ConcurrencyControl.h). Every slot represents one CPU in the system.
// Slot allocation is a request to allocate specific number of CPUs for a specific query.
// Acquired slot is an entity that is held by a thread as long as it is running. This allows
// total number of threads in the system to be limited and the distribution process to be controlled.
//
// TODO:
// - for memory allocations - variable size of slots (in bytes).
//
// Slot 分配与控制的抽象接口层。状态机：free -> granted -> acquired。
// 典型用法：allocate(min, max) -> tryAcquire() -> IAcquiredSlot 析构时 release。

/// Number of slots
using SlotCount = UInt64;

/// Unlimited number of slots
constexpr SlotCount UnlimitedSlots = std::numeric_limits<SlotCount>::max();

/// Acquired slot holder. Slot is considered to be acquired as long as the object exists.
/// 已 acquire 的 slot 持有者（RAII）；析构时由子类负责 release
class IAcquiredSlot : public std::enable_shared_from_this<IAcquiredSlot>, boost::noncopyable
{
public:
    const size_t slot_id; /// Unique identifier of a slot within specific ISlotAllocation.

    explicit IAcquiredSlot(size_t slot_id_)
        : slot_id(slot_id_)
    {}

    virtual ~IAcquiredSlot() = default;
};

using AcquiredSlotPtr = std::shared_ptr<IAcquiredSlot>;

/// Lease provides a slot for a limited time duration.
/// Specialization of IAcquiredSlot that supports preemption.
/// 带租约的 slot，支持抢占；到期需 renew 续租
class ISlotLease : public IAcquiredSlot
{
public:
    explicit ISlotLease(size_t slot_id_)
        : IAcquiredSlot(slot_id_)
    {}

    /// This method is for CPU consumption only.
    /// It should be called from a thread that started using the slot.
    /// Required for obtaining CPU time for the thread, because ctor is called in another thread.
    virtual void startConsumption() = 0;

    /// Renew the slot. This method should be called periodically.
    /// Call may block while waiting for the slot to be reacquired in case of preemption.
    /// Returns true if the slot is still acquired (possibly after a preemption period).
    /// Returns false if the slot is released and holder should stop using it (e.g. thread should be stopped).
    virtual bool renew() = 0;
};

using SlotLeasePtr = std::shared_ptr<ISlotLease>;

/// Request for allocation of slots from ISlotControl.
/// Allows for more slots to be acquired and the whole request to be canceled.
/// 一次 slot 分配请求；allocate(min, max) 创建，析构时自动 free
class ISlotAllocation : public std::enable_shared_from_this<ISlotAllocation>, boost::noncopyable
{
public:
    virtual ~ISlotAllocation() = default;

    /// Free the allocated slots, cancel slot requests and wake up preempted threads.
    virtual void free() {}

    /// Take one already granted slot if available.
    [[nodiscard]] virtual AcquiredSlotPtr tryAcquire() = 0;

    /// Take one granted slot or wait until it is available.
    [[nodiscard]] virtual AcquiredSlotPtr acquire() = 0;

    /// For tests. Returns true iff resource request is currently enqueued into the scheduler.
    virtual bool isRequesting() const { return false; }
};

using SlotAllocationPtr = std::shared_ptr<ISlotAllocation>;

class ISlotControl : boost::noncopyable
{
public:
    virtual ~ISlotControl() = default;

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a "subscription" for later allocation is created
    [[nodiscard]] virtual SlotAllocationPtr allocate(SlotCount min, SlotCount max) = 0;
};

/// Allocation that grants all the slots immediately on creation
/// 简单实现：一次性 grant 全部 slot，不参与 scheduler 竞争
class GrantedAllocation : public ISlotAllocation
{
public:
    explicit GrantedAllocation(SlotCount granted_)
        : total(granted_)
        , granted(granted_)
    {}

    [[nodiscard]] AcquiredSlotPtr tryAcquire() override
    {
        // use_concurrency_control=0 时使用：创建时已 grant 全部 slot，无 scheduler 竞争
        SlotCount value = granted.load();
        while (value)
        {
            // CAS 取走一个 slot；slot_id = total - value 表示第几个被取走（从 0 递增）
            if (granted.compare_exchange_strong(value, value - 1))
                return std::make_shared<IAcquiredSlot>(total - value);
        }
        return {};
    }

    [[nodiscard]] AcquiredSlotPtr acquire() override
    {
        auto result = tryAcquire();
        chassert(result);
        return result;
    }

private:
    const SlotCount total; // thread-safe constant total number of slots
    std::atomic<SlotCount> granted; // allocated, but not yet acquired
};


}
