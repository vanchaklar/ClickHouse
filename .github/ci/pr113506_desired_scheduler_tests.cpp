#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/ResourceAllocation.h>

#include <exception>
#include <memory>

using namespace DB;

struct SuctionLimitTest
{
    SpaceSharedScheduler scheduler;
    SchedulerNodePtr root;

    ~SuctionLimitTest()
    {
        if (root)
        {
            scheduler.removeChild(root.get());
            root.reset();
            drain();
        }
    }

    AllocationQueue * createQueue(ResourceCost max_allocated)
    {
        auto limit = std::make_shared<AllocationLimit>(
            scheduler.event_queue, SchedulerNodeInfo{}, max_allocated);
        auto queue = std::make_shared<AllocationQueue>(
            scheduler.event_queue, SchedulerNodeInfo{});
        queue->basename = "queue";
        AllocationQueue * result = queue.get();
        limit->attachChild(queue);
        root = limit;
        scheduler.attachChild(root);
        drain();
        return result;
    }

    void drain()
    {
        while (true)
        {
            if (scheduler.event_queue.forceProcess())
                continue;
            if (scheduler.decrease)
                scheduler.approveDecrease();
            else if (scheduler.increase)
                scheduler.approveIncrease();
            else
                break;
        }
    }
};


struct ManualAllocation : public ResourceAllocation
{
    ManualAllocation(
        SuctionLimitTest & test_,
        AllocationQueue * queue_,
        const String & name_,
        ResourceCost initial_size,
        MemoryPressurePolicy memory_pressure_policy = {})
        : ResourceAllocation(*queue_, name_, memory_pressure_policy)
        , test(test_)
    {
        if (initial_size > 0)
            increase_enqueued = true;
        queue.insertAllocation(*this, initial_size);
        test.drain();
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    ~ManualAllocation() override
    {
        if (removed || fail_reason)
            return;
        queue.removeAllocation(*this);
        test.drain();
        chassert(removed || fail_reason);
    }

    void increase(ResourceCost size)
    {
        increase_enqueued = true;
        queue.increaseAllocation(*this, size);
        test.drain();
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    void decrease(ResourceCost size)
    {
        decrease_enqueued = true;
        queue.decreaseAllocation(*this, size);
        test.drain();
        if (fail_reason)
            std::rethrow_exception(fail_reason);
        chassert(!decrease_enqueued);
    }

    void enableRecovery()
    {
        has_recovery_controller = true;
    }

    size_t pressureCount() const
    {
        return total_pressure_events;
    }

    bool recoveryActive() const
    {
        return recovery_active;
    }

    size_t killCount() const
    {
        return kills;
    }

    void recoveryCheckpoint()
    {
        recovery_active = false;
        queue.notifyRecoveryProgress(*this);
        test.drain();
    }

private:
    GrowthPressureAction onGrowthPressure() override
    {
        recovery_active = has_recovery_controller;
        if (recovery_active)
            ++total_pressure_events;
        return recovery_active ? GrowthPressureAction::Yield : GrowthPressureAction::Protect;
    }

    void onGrowthPressureResolved() override
    {
        recovery_active = false;
    }

    bool isGrowthRecoveryActive() override
    {
        return recovery_active;
    }

    ResourceCost reconcilePendingIncrease(ResourceCost, ResourceCost requested_size) override
    {
        return requested_size;
    }

    void increaseCancelled() override
    {
        increase_enqueued = false;
    }

    void increaseApproved(const IncreaseRequest & increase_) override
    {
        allocated_size += increase_.size;
        increase_enqueued = false;
    }

    void decreaseApproved(const DecreaseRequest & decrease_) override
    {
        allocated_size -= decrease_.size;
        decrease_enqueued = false;
        if (decrease_.removing_allocation)
            removed = true;
    }

    void allocationFailed(const std::exception_ptr & reason) override
    {
        fail_reason = reason;
        removed = true;
        allocated_size = 0;
    }

    void killAllocation(const std::exception_ptr &) override
    {
        ++kills;
    }

    SuctionLimitTest & test;
    std::exception_ptr fail_reason;
    bool increase_enqueued = false;
    bool decrease_enqueued = false;
    bool removed = false;
    size_t kills = 0;
    ResourceCost allocated_size = 0;
    bool has_recovery_controller = false;
    size_t total_pressure_events = 0;
    bool recovery_active = false;
};


TEST(SchedulerSuctionLimits, ProspectiveTotalIsCheckedAfterSpillCompletes)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 2000;
    policy.suction_max_allocation_bytes = 5000;
    ManualAllocation requester(t, queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 7000, policy);
    requester.enableRecovery();

    requester.increase(3000);
    EXPECT_EQ(requester.pressureCount(), 1u);
    EXPECT_TRUE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 0u);

    requester.recoveryCheckpoint();
    EXPECT_EQ(requester.killCount(), 1u);
    EXPECT_EQ(ordinary->killCount(), 0u);
}


TEST(SchedulerSuctionLimits, UnlimitedPreSuctionAllocationStartsImmediately)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 0;
    ManualAllocation requester(t, queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 7000, policy);
    requester.enableRecovery();

    requester.increase(3000);
    EXPECT_EQ(requester.pressureCount(), 1u);
    EXPECT_FALSE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 1u);
}


TEST(SchedulerSuctionLimits, PreSuctionWaitEndsWhenAllocationFalls)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    ManualAllocation requester(t, queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increase(3000);
    EXPECT_EQ(requester.pressureCount(), 1u);
    EXPECT_TRUE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 0u);

    requester.decrease(1000);
    EXPECT_FALSE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 1u);
}


TEST(SchedulerSuctionLimits, SpillCompletionStartsSuctionAbovePreSuctionLimit)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    ManualAllocation requester(t, queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increase(3000);
    EXPECT_EQ(requester.pressureCount(), 1u);
    EXPECT_TRUE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 0u);

    requester.recoveryCheckpoint();
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 1u);
}


TEST(SchedulerSuctionLimits, LimitsApplyInSequence)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    policy.suction_max_allocation_bytes = 7000;
    ManualAllocation requester(t, queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increase(3000);
    EXPECT_EQ(requester.pressureCount(), 1u);
    EXPECT_TRUE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 0u);

    requester.decrease(1000);
    EXPECT_FALSE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 1u);
    EXPECT_EQ(ordinary->killCount(), 0u);
}


TEST(SchedulerSuctionLimits, ProspectiveTotalAtLimitIsAllowed)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.suction_max_allocation_bytes = 5000;
    ManualAllocation requester(t, queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(t, queue, "ordinary", 7000, policy);

    requester.increase(2000);
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(ordinary->killCount(), 1u);
}
