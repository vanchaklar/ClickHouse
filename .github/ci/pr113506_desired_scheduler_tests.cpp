/// CI-only executable specification for the intended memory-pressure policy.
///
/// This translation unit deliberately includes the production scheduler tests so it can reuse their
/// deterministic ManualAllocation harness without copying it. It is built only by the isolated PR
/// workflow; it is not part of the ClickHouse source target or the PR branch.
#include <Common/Scheduler/Nodes/tests/gtest_space_shared_scheduler.cpp>

/// A late pending request that fits must wake an idle queue even when older pending requests were
/// parked earlier in the same suspension round.
TEST(SchedulerSpaceSharedDesired, LateFittingAdmissionWakesSuspendedQueue)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(queue, "beneficiary", 1000, false);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, false);
    release.set_value();

    beneficiary->waitSynced();
    ASSERT_EQ(beneficiary->size(), 1000);
    ASSERT_EQ(heavy.killCount(), 0u);

    auto late_fitting = std::make_unique<ManualAllocation>(queue, "late_fitting", 1000, false);
    late_fitting->waitSynced();

    EXPECT_EQ(late_fitting->size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// The equivalent wake-up rule applies to regular growth from an already admitted allocation, not
/// only to newly admitted queries.
TEST(SchedulerSpaceSharedDesired, LateFittingRegularGrowthWakesSuspendedQueue)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 7000);
    ManualAllocation late_grower(queue, "late_grower", 500);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(queue, "beneficiary", 1000, false);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, false);
    release.set_value();

    beneficiary->waitSynced();
    late_grower.increaseAsync(500);
    late_grower.waitSynced();

    EXPECT_EQ(late_grower.size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Detaching an unrelated empty subtree must not forget a parked request or sever its release-driven
/// retry path.
TEST(SchedulerSpaceSharedDesired, UnrelatedDetachKeepsSuspendedGrowthRetryable)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    FairAllocation * policy_ptr = policy.get();
    policy->basename = "policy";
    limit->attachChild(policy);

    auto heavy_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    heavy_queue->basename = "heavy_queue";
    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    policy->attachChild(heavy_queue);

    auto beneficiary_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    beneficiary_queue->basename = "beneficiary_queue";
    AllocationQueue * beneficiary_queue_ptr = beneficiary_queue.get();
    policy->attachChild(beneficiary_queue);

    auto unrelated_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    unrelated_queue->basename = "unrelated_queue";
    policy->attachChild(unrelated_queue);

    r.root_node = limit;
    beneficiary_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(
        beneficiary_queue_ptr, "beneficiary", 1000, false);
    release.set_value();
    beneficiary->waitSynced();
    ASSERT_EQ(heavy.killCount(), 0u);

    std::promise<void> detached;
    auto detached_future = detached.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        policy_ptr->removeChild(unrelated_queue.get());
        unrelated_queue.reset();
        detached.set_value();
    });
    detached_future.get();

    beneficiary.reset();
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}


/// Without an explicit opt-in pressure policy, suspension must not silently override a precedence
/// boundary. The high-precedence workload reaches its normal last resort; lower-precedence work is
/// not admitted merely because it happens to fit.
TEST(SchedulerSpaceSharedDesired, DefaultSuspensionDoesNotCrossPrecedenceBoundary)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto high_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    high_queue->basename = "high";
    AllocationQueue * high_queue_ptr = high_queue.get();
    policy->attachChild(high_queue);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    auto low_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, low_info);
    low_queue->basename = "low";
    AllocationQueue * low_queue_ptr = low_queue.get();
    policy->attachChild(low_queue);

    r.root_node = limit;
    low_queue.reset();
    high_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(high_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto lower_precedence = std::make_unique<ManualAllocation>(
        low_queue_ptr, "lower_precedence", 1000, false);
    release.set_value();

    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
    EXPECT_EQ(lower_precedence->size(), 0);
}


/// Reference half of the request-quantization invariant: one large growth request is stalled and
/// leaves the fixed pressure zone available to fitting work.
TEST(SchedulerSpaceSharedDesired, LargeGrowthLeavesProtectedCapacityForFittingWork)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    ManualAllocation releaser(queue, "releaser", 3000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(3000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 500, false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.size(), 6000);
    EXPECT_EQ(small->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);

    releaser.decreaseAsync(3000);
    releaser.waitSynced();
    heavy.waitSynced();
    EXPECT_EQ(heavy.size(), 9000);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// The same intended +3000 growth split at a MemoryTracker sync point must not consume the protected
/// pressure zone before a fitting query gets its chance. Scheduling must not depend on whether growth
/// arrives as 1x3000 or 3x1000.
TEST(SchedulerSpaceSharedDesired, SmallStepGrowthLeavesTheSameProtectedCapacity)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    ManualAllocation releaser(queue, "releaser", 3000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(1000); // First chunk of an intended +3000 growth.
    auto small = std::make_unique<ManualAllocation>(queue, "small", 500, false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.size(), 6000) << "Growth must stall on entering the fixed pressure zone";
    EXPECT_EQ(small->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Reclamation by the suspended holder is useful progress. It must remain able to decrease, and the
/// resulting headroom must approve its parked growth without killing either allocation.
TEST(SchedulerSpaceSharedDesired, SuspendedHolderCanReclaimAndResume)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(3000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 1000, false);
    release.set_value();
    small->waitSynced();

    heavy.decreaseAsync(2000);
    heavy.waitSynced();

    EXPECT_EQ(heavy.size(), 9000);
    EXPECT_EQ(small->size(), 1000);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Search is complete within the constrained policy subtree: every fitting sibling is admitted before
/// the scheduler considers eviction, even with an earlier non-fitting sibling.
TEST(SchedulerSpaceSharedDesired, AllFittingFairSiblingsRunBeforeEviction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    auto make_queue = [&](const String & name)
    {
        auto queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
        queue->basename = name;
        policy->attachChild(queue);
        return queue;
    };

    auto heavy_queue = make_queue("heavy");
    auto blocked_queue = make_queue("blocked");
    auto fitting_a_queue = make_queue("fitting_a");
    auto fitting_b_queue = make_queue("fitting_b");
    auto fitting_c_queue = make_queue("fitting_c");

    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    AllocationQueue * blocked_queue_ptr = blocked_queue.get();
    AllocationQueue * fitting_a_queue_ptr = fitting_a_queue.get();
    AllocationQueue * fitting_b_queue_ptr = fitting_b_queue.get();
    AllocationQueue * fitting_c_queue_ptr = fitting_c_queue.get();

    r.root_node = limit;
    fitting_c_queue.reset();
    fitting_b_queue.reset();
    fitting_a_queue.reset();
    blocked_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 7000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto blocked = std::make_unique<ManualAllocation>(blocked_queue_ptr, "blocked", 4000, false);
    auto fitting_a = std::make_unique<ManualAllocation>(fitting_a_queue_ptr, "fitting_a", 1000, false);
    auto fitting_b = std::make_unique<ManualAllocation>(fitting_b_queue_ptr, "fitting_b", 500, false);
    auto fitting_c = std::make_unique<ManualAllocation>(fitting_c_queue_ptr, "fitting_c", 500, false);
    release.set_value();

    fitting_a->waitSynced();
    fitting_b->waitSynced();
    fitting_c->waitSynced();

    EXPECT_EQ(fitting_a->size(), 1000);
    EXPECT_EQ(fitting_b->size(), 500);
    EXPECT_EQ(fitting_c->size(), 500);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Concurrent fitting arrivals must not race with suspension state or remain hidden. Their aggregate
/// request exactly matches the free 2 KB, so every one has a valid admission and no victim is needed.
TEST(SchedulerSpaceSharedDesired, ConcurrentFittingArrivalsAllProgress)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.increaseAsync(5000);

    constexpr size_t query_count = 8;
    std::barrier<> start(query_count + 1);
    std::vector<std::unique_ptr<ManualAllocation>> fitting(query_count);
    std::vector<std::thread> threads;
    threads.reserve(query_count);

    for (size_t index = 0; index < query_count; ++index)
    {
        threads.emplace_back([&, index]
        {
            start.arrive_and_wait();
            fitting[index] = std::make_unique<ManualAllocation>(
                queue, fmt::format("fitting_{}", index), 250, false);
            fitting[index]->waitSynced();
        });
    }

    start.arrive_and_wait();
    for (auto & thread : threads)
        thread.join();

    for (const auto & allocation : fitting)
        EXPECT_EQ(allocation->size(), 250);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Baseline copy of the PR regression: productive fitting work may run first, but if that
/// beneficiary itself blocks, the original holder must remain available as the last-resort victim.
TEST(SchedulerSpaceSharedDesired, BeneficiaryBlockedOnGrowthCanEvictSuspendedAllocation)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 1000, /* wait_for_admission = */ false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.killCount(), 0u);

    small->increaseAsync(2000); // 8000 + 1000 + 2000 > 10000: the winner itself can no longer progress.
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}
