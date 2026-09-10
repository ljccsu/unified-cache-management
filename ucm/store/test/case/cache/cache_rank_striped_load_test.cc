/**
 * MIT License
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 */
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <gtest/gtest.h>
#include "cache/cc/load_queue.h"
#include "cache/cc/shm_numa.h"
#include "detail/mock_store.h"
#include "detail/random.h"

namespace {
using namespace UC::CacheStore;
using UC::Status;
using namespace testing;

enum class Outcome { SUCCESS, CHECK_FAILURE, WAIT_FAILURE, CANCEL };

// Run with the simu runtime on Linux and UCM_TEST_NUMA_NODES set. The two segments
// may use the same physical NUMA node: this regression concerns segment ordering.
void CheckSharedLoads(Outcome outcome, size_t streams, bool mismatch = true)
{
    const auto* nodesText = std::getenv("UCM_TEST_NUMA_NODES");
    if (nodesText == nullptr) { GTEST_SKIP() << "Set UCM_TEST_NUMA_NODES on a Linux simu build"; }
    const auto nodes = ShmNuma::ParseNodes(nodesText);
    ASSERT_FALSE(nodes.empty());
    UC::Test::Detail::Random random;
    Config config;
    config.uniqueId = random.RandomString(16);
    config.shareBufferEnable = true;
    config.shareBufferRankStriped = true;
    config.shareBufferNumaNodes = {nodes.front()};
    config.localRankSize = 2;
    config.shardSize = ShmNuma::PageSize();
    config.blockSize = config.shardSize;
    config.bufferCapacity = config.shardSize * 64;
    config.loadExclusiveBufferNumber = 0;
    config.tensorSizes = {config.shardSize};
    config.streamNumber = streams;
    config.cacheSdmaDirect = false;
    config.timeoutMs = 2000;
    config.waitingQueueDepth = 8;
    // Only one usable entry. Completion must not depend on the owner shard
    // already having been popped from running_.
    config.runningQueueDepth = 2;

    std::array<TransBuffer, 2> buffers;
    std::array<std::future<Status>, 2> setups;
    for (int rank = 0; rank < 2; ++rank) {
        auto rankConfig = config;
        rankConfig.deviceId = rank + 8;
        rankConfig.shareBufferRank = static_cast<size_t>(rank);
        setups[rank] = std::async(std::launch::async,
                                  [&, rank, rankConfig] { return buffers[rank].Setup(rankConfig); });
    }
    for (auto& setup : setups) { EXPECT_EQ(setup.get(), Status::OK()); }
    if (Test::HasFailure()) { return; }

    UC::Detail::BlockId a{}, b{};
    a[0] = std::byte{0xa1};
    b[0] = std::byte{0xb2};
    const size_t segmentA = mismatch ? 1 : 0;
    const size_t segmentB = mismatch ? 0 : 1;
    buffers[0].Prealloc(a, 0, true, segmentA);
    buffers[0].Prealloc(b, 0, true, segmentB);
    {
        auto ha = buffers[0].Get(a, 0, true, true, 0);
        auto hb = buffers[0].Get(b, 0, true, true, 1);
        ASSERT_EQ(ha.Segment(), segmentA);
        ASSERT_EQ(hb.Segment(), segmentB);
        ASSERT_FALSE(ha.Ready());
        ASSERT_FALSE(hb.Ready());
    }

    std::array<NiceMock<UC::Test::Detail::MockStore>, 2> backends;
    std::array<UC::HashSet<UC::Detail::TaskHandle>, 2> failures;
    std::array<std::array<std::vector<unsigned char>, 2>, 2> outputs;
    std::array<std::shared_ptr<TransTask>, 2> tasks;
    std::array<std::shared_ptr<UC::Latch>, 2> waiters;
    std::array<std::promise<void>, 2> submitted;
    std::array<std::shared_future<void>, 2> submittedFutures{
        submitted[0].get_future().share(), submitted[1].get_future().share()};
    std::atomic<bool> allowCompletion{outcome != Outcome::CANCEL};
    std::array<std::atomic<size_t>, 2> checks{};
    // Queues must be destroyed before their backends, outputs and buffers.
    std::array<LoadQueue, 2> queues;
    for (int rank = 0; rank < 2; ++rank) {
        const auto backendId = static_cast<size_t>(rank + 1);
        EXPECT_CALL(backends[rank], Load)
            .WillOnce(Invoke([&, rank, backendId](UC::Detail::TaskDesc desc)
                                 -> UC::Expected<UC::Detail::TaskHandle> {
                EXPECT_EQ(desc.size(), 1U);
                EXPECT_EQ(desc.front().owner, rank == 0 ? a : b);
                std::memset(desc.front().addrs.front(), rank == 0 ? 0xa1 : 0xb2,
                            config.shardSize);
                // Each rank claims its first shard before either can claim the second.
                submitted[rank].set_value();
                if (submittedFutures[1 - rank].wait_for(std::chrono::seconds(2)) !=
                    std::future_status::ready) {
                    return Status::Timeout();
                }
                return UC::Detail::TaskHandle{backendId};
            }));
        ON_CALL(backends[rank], Check(backendId))
            .WillByDefault(Invoke([&, rank](UC::Detail::TaskHandle) -> UC::Expected<bool> {
                ++checks[rank];
                if (rank == 1 && outcome == Outcome::CHECK_FAILURE) {
                    return Status::NotFound();
                }
                // Keep A pending until rank 1 checks B, so the check-failure
                // case exercises peer-wait progress regardless of scheduling.
                if (rank == 0 && outcome == Outcome::CHECK_FAILURE && checks[1] == 0) {
                    return false;
                }
                return allowCompletion.load();
            }));
        if (!mismatch) {
            // Matching placement puts each rank's own read first. Even if that
            // read takes time, the direct Wait path must not poll any backend.
            EXPECT_CALL(backends[rank], Check(_)).Times(0);
        }
        EXPECT_CALL(backends[rank], Wait(backendId))
            .WillOnce(Invoke([&, rank](UC::Detail::TaskHandle) {
                if (!mismatch) { std::this_thread::sleep_for(std::chrono::milliseconds(5)); }
                return rank == 1 && outcome == Outcome::WAIT_FAILURE ? Status::NotFound()
                                                                   : Status::OK();
            }));
        auto rankConfig = config;
        rankConfig.deviceId = rank + 8;
        rankConfig.shareBufferRank = static_cast<size_t>(rank);
        rankConfig.storeBackend = &backends[rank];
        ASSERT_EQ(queues[rank].Setup(rankConfig, &failures[rank], &buffers[rank]), Status::OK());
        for (auto& output : outputs[rank]) { output.resize(config.shardSize, 0); }
        UC::Detail::TaskDesc desc{
            {a, 0, {outputs[rank][0].data()}},
            {b, 0, {outputs[rank][1].data()}}
        };
        tasks[rank] = std::make_shared<TransTask>(TransTask::Type::LOAD, std::move(desc));
        waiters[rank] = std::make_shared<UC::Latch>();
    }
    for (int rank = 0; rank < 2; ++rank) { queues[rank].Submit(tasks[rank], waiters[rank]); }
    if (outcome == Outcome::CANCEL) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (checks[0].load() == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        failures[0].Insert(tasks[0]->id);
        allowCompletion = true;
    }
    const auto done0 = waiters[0]->WaitForDuration(2000);
    const auto done1 = waiters[1]->WaitForDuration(2000);
    // Release a regressed FIFO waiter before destroying the queues; report failure
    // instead of leaving the unit-test process hanging in a destructor.
    if (!done0 || !done1) {
        for (int rank = 0; rank < 2; ++rank) { failures[rank].Insert(tasks[rank]->id); }
    }
    EXPECT_TRUE(done0);
    EXPECT_TRUE(done1);
    for (int rank = 0; rank < 2; ++rank) {
        EXPECT_TRUE(waiters[rank]->WaitForDuration(2000));
        const auto shouldFail = outcome == Outcome::CHECK_FAILURE ||
                               outcome == Outcome::WAIT_FAILURE ||
                               (outcome == Outcome::CANCEL && rank == 0);
        EXPECT_EQ(failures[rank].Contains(tasks[rank]->id), shouldFail);
        if (outcome == Outcome::CHECK_FAILURE || outcome == Outcome::WAIT_FAILURE) {
            EXPECT_EQ(tasks[rank]->FailureStatus(), Status::NotFound());
        }
        if (shouldFail || !done0 || !done1) { continue; }
        EXPECT_EQ(outputs[rank][0], std::vector<unsigned char>(config.shardSize, 0xa1));
        EXPECT_EQ(outputs[rank][1], std::vector<unsigned char>(config.shardSize, 0xb2));
    }
    if (done0 && done1 && (outcome == Outcome::SUCCESS || outcome == Outcome::CANCEL)) {
        auto ha = buffers[0].Get(a, 0, true, true);
        auto hb = buffers[0].Get(b, 0, true, true);
        EXPECT_TRUE(ha.Ready());
        EXPECT_TRUE(hb.Ready());
    }
    if (done0 && done1 && outcome == Outcome::SUCCESS) {
        // The same placement is now a pure SHM hit. No new backend
        // Load/Wait is permitted by the expectations above.
        for (int rank = 0; rank < 2; ++rank) {
            for (auto& output : outputs[rank]) { std::fill(output.begin(), output.end(), 0); }
            UC::Detail::TaskDesc desc{
                {a, 0, {outputs[rank][0].data()}},
                {b, 0, {outputs[rank][1].data()}}
            };
            tasks[rank] = std::make_shared<TransTask>(TransTask::Type::LOAD, std::move(desc));
            waiters[rank] = std::make_shared<UC::Latch>();
            queues[rank].Submit(tasks[rank], waiters[rank]);
        }
        for (int rank = 0; rank < 2; ++rank) {
            const auto done = waiters[rank]->WaitForDuration(2000);
            if (!done) { failures[rank].Insert(tasks[rank]->id); }
            EXPECT_TRUE(done);
            EXPECT_TRUE(waiters[rank]->WaitForDuration(2000));
            EXPECT_FALSE(failures[rank].Contains(tasks[rank]->id));
            if (!done) { continue; }
            EXPECT_EQ(outputs[rank][0], std::vector<unsigned char>(config.shardSize, 0xa1));
            EXPECT_EQ(outputs[rank][1], std::vector<unsigned char>(config.shardSize, 0xb2));
        }
    }
}
}  // namespace

TEST(UCCacheRankStripedLoadTest, MismatchedOwnersCompleteWithOneStream)
{ CheckSharedLoads(Outcome::SUCCESS, 1); }

TEST(UCCacheRankStripedLoadTest, MismatchedOwnersCompleteWithSixteenStreams)
{ CheckSharedLoads(Outcome::SUCCESS, 16); }

TEST(UCCacheRankStripedLoadTest, MatchingOwnersWaitDirectlyWithOneStream)
{ CheckSharedLoads(Outcome::SUCCESS, 1, false); }

TEST(UCCacheRankStripedLoadTest, MatchingOwnersWaitDirectlyWithSixteenStreams)
{ CheckSharedLoads(Outcome::SUCCESS, 16, false); }

TEST(UCCacheRankStripedLoadTest, DirectWaitFailureIsPublishedToPeer)
{ CheckSharedLoads(Outcome::WAIT_FAILURE, 1, false); }

TEST(UCCacheRankStripedLoadTest, CheckFailureIsDrainedAndPublished)
{ CheckSharedLoads(Outcome::CHECK_FAILURE, 1); }

TEST(UCCacheRankStripedLoadTest, WaitFailureIsPublishedToPeer)
{ CheckSharedLoads(Outcome::WAIT_FAILURE, 1); }

TEST(UCCacheRankStripedLoadTest, CancellationDrainsOwnedReadsForPeer)
{ CheckSharedLoads(Outcome::CANCEL, 1); }
