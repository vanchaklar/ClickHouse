#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/ResourceAllocation.h>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <future>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

using namespace DB;

struct SuctionLimitTest
{
    SpaceSharedScheduler scheduler;
    SchedulerNodePtr root;

    SuctionLimitTest()
    {
        scheduler.start(ThreadName::TEST_SCHEDULER);
    }

    ~SuctionLimitTest()
    {
        if (root)
        {
            std::promise<void> removed;
            scheduler.event_queue.enqueue([this, &removed]
            {
                scheduler.removeChild(root.get());
                root.reset();
                removed.set_value();
            });
            removed.get_future().get();
        }
        scheduler.stop(true);
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

        std::promise<void> attached;
        scheduler.event_queue.enqueue([this, &attached]
        {
            scheduler.attachChild(root);
            attached.set_value();
        });
        attached.get_future().get();
        return result;
    }
};


struct ManualAllocation : public ResourceAllocation
{
    ManualAllocation(
        AllocationQueue * queue_,
        const String & name_,
        ResourceCost initial_size,
        MemoryPressurePolicy memory_pressure_policy = {})
        : ResourceAllocation(*queue_, name_, memory_pressure_policy)
    {
        if (initial_size > 0)
            increase_enqueued = true;
        queue.insertAllocation(*this, initial_size);
        if (initial_size > 0)
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [this] { return !increase_enqueued || fail_reason; });
            if (fail_reason)
                std::rethrow_exception(fail_reason);
        }
    }

    ~ManualAllocation() override
    {
        {
            std::unique_lock lock(mutex);
            if (removed || fail_reason)
                return;
        }
        queue.removeAllocation(*this);
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return removed || fail_reason; });
    }

    void increaseAsync(ResourceCost size)
    {
        {
            std::unique_lock lock(mutex);
            increase_enqueued = true;
        }
        queue.increaseAllocation(*this, size);
    }

    void decreaseAsync(ResourceCost size)
    {
        {
            std::unique_lock lock(mutex);
            decrease_enqueued = true;
        }
        queue.decreaseAllocation(*this, size);
    }

    void waitDecreaseSynced()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return fail_reason || !decrease_enqueued; });
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    bool waitKillsFor(size_t count, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return kills >= count; });
    }

    size_t killCount()
    {
        std::unique_lock lock(mutex);
        return kills;
    }

    void enableRecovery()
    {
        std::unique_lock lock(mutex);
        has_recovery_controller = true;
    }

    void waitPressureCount(size_t count)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return total_pressure_events >= count; });
    }

    size_t pressureCount()
    {
        std::unique_lock lock(mutex);
        return total_pressure_events;
    }

    bool recoveryActive()
    {
        std::unique_lock lock(mutex);
        return recovery_active;
    }

    void recoveryCheckpoint()
    {
        {
            std::unique_lock lock(mutex);
            recovery_active = false;
        }
        queue.notifyRecoveryProgress(*this);
    }

private:
    GrowthPressureAction onGrowthPressure() override
    {
        std::unique_lock lock(mutex);
        recovery_active = has_recovery_controller;
        if (recovery_active)
            ++total_pressure_events;
        cv.notify_all();
        return recovery_active ? GrowthPressureAction::Yield : GrowthPressureAction::Protect;
    }

    void onGrowthPressureResolved() override
    {
        std::unique_lock lock(mutex);
        recovery_active = false;
        cv.notify_all();
    }

    bool isGrowthRecoveryActive() override
    {
        std::unique_lock lock(mutex);
        return recovery_active;
    }

    ResourceCost reconcilePendingIncrease(ResourceCost, ResourceCost requested_size) override
    {
        return requested_size;
    }

    void increaseCancelled() override
    {
        std::unique_lock lock(mutex);
        increase_enqueued = false;
        cv.notify_all();
    }

    void increaseApproved(const IncreaseRequest & increase) override
    {
        std::unique_lock lock(mutex);
        allocated_size += increase.size;
        increase_enqueued = false;
        cv.notify_all();
    }

    void decreaseApproved(const DecreaseRequest & decrease) override
    {
        std::unique_lock lock(mutex);
        allocated_size -= decrease.size;
        decrease_enqueued = false;
        if (decrease.removing_allocation)
            removed = true;
        cv.notify_all();
    }

    void allocationFailed(const std::exception_ptr & reason) override
    {
        std::unique_lock lock(mutex);
        fail_reason = reason;
        removed = true;
        allocated_size = 0;
        cv.notify_all();
    }

    void killAllocation(const std::exception_ptr &) override
    {
        std::unique_lock lock(mutex);
        ++kills;
        cv.notify_all();
    }

    std::mutex mutex;
    std::condition_variable cv;
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
    ManualAllocation requester(queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 7000, policy);
    requester.enableRecovery();

    requester.increaseAsync(3000);
    requester.waitPressureCount(1);
    EXPECT_TRUE(requester.recoveryActive());

    requester.recoveryCheckpoint();
    ASSERT_TRUE(requester.waitKillsFor(1, std::chrono::seconds(5)));
    EXPECT_EQ(ordinary->killCount(), 0u);
}


TEST(SchedulerSuctionLimits, UnlimitedPreSuctionAllocationStartsImmediately)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 0;
    ManualAllocation requester(queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 7000, policy);
    requester.enableRecovery();

    requester.increaseAsync(3000);
    requester.waitPressureCount(1);
    ASSERT_TRUE(ordinary->waitKillsFor(1, std::chrono::seconds(5)))
        << "An unlimited pre-suction threshold waited for spill completion";
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_EQ(requester.pressureCount(), 1u);
}


TEST(SchedulerSuctionLimits, PreSuctionWaitEndsWhenAllocationFalls)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    ManualAllocation requester(queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increaseAsync(3000);
    requester.waitPressureCount(1);
    EXPECT_TRUE(requester.recoveryActive());

    requester.decreaseAsync(1000);
    requester.waitDecreaseSynced();

    ASSERT_TRUE(ordinary->waitKillsFor(1, std::chrono::seconds(5)));
    EXPECT_FALSE(requester.recoveryActive());
    EXPECT_EQ(requester.killCount(), 0u);
}


TEST(SchedulerSuctionLimits, SpillCompletionStartsSuctionAbovePreSuctionLimit)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    ManualAllocation requester(queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increaseAsync(3000);
    requester.waitPressureCount(1);
    EXPECT_TRUE(requester.recoveryActive());

    requester.recoveryCheckpoint();
    ASSERT_TRUE(ordinary->waitKillsFor(1, std::chrono::seconds(5)));
    EXPECT_EQ(requester.killCount(), 0u);
}


TEST(SchedulerSuctionLimits, LimitsApplyInSequence)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.max_allocation_before_suction_bytes = 5000;
    policy.suction_max_allocation_bytes = 7000;
    ManualAllocation requester(queue, "requester", 6000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 4000, policy);
    requester.enableRecovery();

    requester.increaseAsync(3000);
    requester.waitPressureCount(1);
    EXPECT_TRUE(requester.recoveryActive());

    requester.decreaseAsync(1000);
    requester.waitDecreaseSynced();

    ASSERT_TRUE(requester.waitKillsFor(1, std::chrono::seconds(5)));
    EXPECT_FALSE(requester.recoveryActive());
    EXPECT_EQ(ordinary->killCount(), 0u);
}


TEST(SchedulerSuctionLimits, ProspectiveTotalAtLimitIsAllowed)
{
    SuctionLimitTest t;
    AllocationQueue * queue = t.createQueue(10000);

    ResourceAllocation::MemoryPressurePolicy policy;
    policy.suction_max_allocation_bytes = 5000;
    ManualAllocation requester(queue, "requester", 3000, policy);
    auto ordinary = std::make_unique<ManualAllocation>(queue, "ordinary", 7000, policy);

    requester.increaseAsync(2000);
    ASSERT_TRUE(ordinary->waitKillsFor(1, std::chrono::seconds(5)));
    EXPECT_EQ(requester.killCount(), 0u);
}
