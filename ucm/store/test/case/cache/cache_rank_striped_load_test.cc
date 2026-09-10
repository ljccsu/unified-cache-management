/**
 * MIT License
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 */
#include <algorithm>
#include <array>
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

// Run with the simu runtime on Linux and UCM_TEST_NUMA_NODES set. Deliberately
// place A and B in the opposite segments: Load must still preserve Get order.
void CheckSharedLoads(size_t streams)
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
    config.timeoutMs = 2000;
    config.waitingQueueDepth = 8;
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
    buffers[0].Prealloc(a, 0, true, 1);
    buffers[0].Prealloc(b, 0, true, 0);

    std::array<NiceMock<UC::Test::Detail::MockStore>, 2> backends;
    std::array<UC::HashSet<UC::Detail::TaskHandle>, 2> failures;
    std::array<std::array<std::vector<unsigned char>, 2>, 2> outputs;
    std::array<std::shared_ptr<TransTask>, 2> tasks;
    std::array<std::shared_ptr<UC::Latch>, 2> waiters;
    std::array<std::promise<void>, 2> submitted;
    std::array<std::shared_future<void>, 2> submittedFutures{
        submitted[0].get_future().share(), submitted[1].get_future().share()};
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
                // Force each rank to claim its first shard before either advances.
                submitted[rank].set_value();
                if (submittedFutures[1 - rank].wait_for(std::chrono::seconds(2)) !=
                    std::future_status::ready) {
                    return Status::Timeout();
                }
                return UC::Detail::TaskHandle{backendId};
            }));
        EXPECT_CALL(backends[rank], Check(_)).Times(0);
        EXPECT_CALL(backends[rank], Wait(backendId)).WillOnce(Return(Status::OK()));

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
    const auto done0 = waiters[0]->WaitForDuration(2000);
    const auto done1 = waiters[1]->WaitForDuration(2000);
    if (!done0 || !done1) {
        for (int rank = 0; rank < 2; ++rank) { failures[rank].Insert(tasks[rank]->id); }
    }
    ASSERT_TRUE(done0);
    ASSERT_TRUE(done1);

    for (int rank = 0; rank < 2; ++rank) {
        EXPECT_FALSE(failures[rank].Contains(tasks[rank]->id));
        EXPECT_EQ(outputs[rank][0], std::vector<unsigned char>(config.shardSize, 0xa1));
        EXPECT_EQ(outputs[rank][1], std::vector<unsigned char>(config.shardSize, 0xb2));
    }
}
}  // namespace

TEST(UCCacheRankStripedLoadTest, MismatchedPlacementKeepsGetOrderWithOneStream)
{ CheckSharedLoads(1); }

TEST(UCCacheRankStripedLoadTest, MismatchedPlacementKeepsGetOrderWithSixteenStreams)
{ CheckSharedLoads(16); }
