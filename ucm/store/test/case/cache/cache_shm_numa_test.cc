/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include <cstdlib>
#include <future>
#include <gtest/gtest.h>
#include "cache/cc/posix_shm.h"
#include "cache/cc/shm_numa.h"
#include "cache/cc/trans_buffer.h"
#include "detail/random.h"
#include "detail/types_helper.h"

namespace {
namespace Numa = UC::CacheStore::ShmNuma;
using UC::CacheStore::Config;
using UC::CacheStore::PosixShm;
using UC::CacheStore::TransBuffer;
Config SmallConfig()
{
    UC::Test::Detail::Random random;
    Config config;
    config.uniqueId = random.RandomString(16);
    config.deviceId = 0;
    config.shardSize = Numa::PageSize();
    config.bufferCapacity = config.shardSize * 64;
    config.loadExclusiveBufferNumber = 0;
    config.timeoutMs = 1000;
    return config;
}
}  // namespace

TEST(UCCacheShmNumaTest, RankStripedRejectsInvalidConfigurationBeforeAllocation)
{
    auto config = SmallConfig();
    config.shareBufferRankStriped = true;
    config.localRankSize = 2;
    config.shareBufferNumaNodes = {0, 0};
    TransBuffer duplicate;
    EXPECT_TRUE(duplicate.Setup(config).Failure());
    config.shareBufferNumaNodes = {0, 1};
    config.localRankSize = 1;
    TransBuffer uneven;
    EXPECT_TRUE(uneven.Setup(config).Failure());
    config.shareBufferNumaNodes.clear();
    TransBuffer defaultNodes;
    // Missing nodes still selects all eight defaults, which one rank cannot cover.
    EXPECT_TRUE(defaultNodes.Setup(config).Failure());
}

TEST(UCCacheShmNumaTest, OrdinaryShmIgnoresNumaSetting)
{
    auto config = SmallConfig();
    config.shareBufferRankStriped = false;
    // Even an unusable NUMA list is inert for the baseline ordinary SHM.
    config.shareBufferNumaNodes = {static_cast<size_t>(-1), 0, 0};
    TransBuffer baseline;
    ASSERT_EQ(baseline.Setup(config), UC::Status::OK());
    const auto block = UC::Test::Detail::TypesHelper::MakeBlockIdRandomly();
    auto handle = baseline.Get(block, 0);
    *static_cast<unsigned char*>(handle.Data()) = 0x5a;
    auto peerConfig = config;
    peerConfig.shareBufferNumaNodes.clear();
    TransBuffer peer;
    ASSERT_EQ(peer.Setup(peerConfig), UC::Status::OK());
    auto same = peer.Get(block, 0);
    EXPECT_EQ(*static_cast<unsigned char*>(same.Data()), 0x5a);
}

TEST(UCCacheShmNumaTest, RankMetadataWaitsForTruncateWithoutUnlink)
{
    auto config = SmallConfig();
    config.shareBufferRankStriped = true;
    config.shareBufferNumaNodes = {0};
    config.localRankSize = 1;
    config.timeoutMs = 30;
    const auto name = "uc_shm_cache_" + config.uniqueId + "_rs_meta";
    PosixShm file{name};
    ASSERT_EQ(file.ShmOpen(PosixShm::OpenFlag::CREATE | PosixShm::OpenFlag::EXCL |
                           PosixShm::OpenFlag::READ_WRITE),
              UC::Status::OK());
    {
        TransBuffer peer;
        const auto status = peer.Setup(config);
        EXPECT_TRUE(status.Failure());
        EXPECT_NE(status.ToString().find("truncate not ready"), std::string::npos);
    }
    PosixShm reopened{name};
    EXPECT_EQ(reopened.ShmOpen(PosixShm::OpenFlag::READ_WRITE), UC::Status::OK());
    file.ShmUnlink();
}

// Opt in on a Linux NUMA host. Use the simu backend to exercise real SHM without devices.
TEST(UCCacheShmNumaTest, LiveConcurrentRankStriped)
{
    const auto* text = std::getenv("UCM_TEST_NUMA_NODES");
    if (text == nullptr) { GTEST_SKIP() << "Set UCM_TEST_NUMA_NODES, e.g. 0-7"; }
    const auto nodes = Numa::ParseNodes(text);
    auto base = SmallConfig();
    const auto ranks = nodes.size() * 2;
    base.shareBufferRankStriped = true;
    base.shareBufferNumaNodes = nodes;
    base.localRankSize = ranks;
    base.bufferCapacity = base.shardSize * 32 * ranks;
    base.timeoutMs = 10000;
    std::vector<Config> configs(ranks, base);
    std::vector<TransBuffer> buffers(ranks);
    std::vector<std::future<UC::Status>> setups;
    for (size_t rank = 0; rank < ranks; ++rank) {
        configs[rank].deviceId = static_cast<int32_t>(rank);
        setups.push_back(std::async(std::launch::async,
                                    [&, rank] { return buffers[rank].Setup(configs[rank]); }));
    }
    for (auto& setup : setups) { EXPECT_EQ(setup.get(), UC::Status::OK()); }
    if (::testing::Test::HasFailure()) { return; }
    const auto block = UC::Test::Detail::TypesHelper::MakeBlockIdRandomly();
    auto owner = buffers[0].Get(block, 0, false, false, 0);
    *static_cast<unsigned char*>(owner.Data()) = 0xa5;
    for (size_t rank = 1; rank < ranks; ++rank) {
        auto peer = buffers[rank].Get(block, 0);
        EXPECT_EQ(*static_cast<unsigned char*>(peer.Data()), 0xa5);
    }
    {
        auto different = base;
        different.shareBufferNumaNodes = {65535};
        TransBuffer mismatch;
        const auto status = mismatch.Setup(different);
        EXPECT_TRUE(status.Failure());
        EXPECT_NE(status.ToString().find("NUMA"), std::string::npos);
    }
    TransBuffer reattached;
    ASSERT_EQ(reattached.Setup(base), UC::Status::OK());
    Config watcherConfig = base;
    watcherConfig.deviceId = -1;
    TransBuffer watcher;
    ASSERT_EQ(watcher.Setup(watcherConfig), UC::Status::OK());
    EXPECT_TRUE(watcher.Exist(block, 0));
    for (size_t segment = 0; segment < ranks; ++segment) {
        auto handle = buffers[0].Get(UC::Test::Detail::TypesHelper::MakeBlockIdRandomly(), 0, false,
                                     false, segment);
        EXPECT_EQ(handle.Segment(), segment);
        const std::vector<Numa::Range> range{
            {0, base.shardSize, nodes[segment % nodes.size()]}
        };
        EXPECT_NO_THROW(Numa::Verify(handle.Data(), range, "live-rank-segment"));
    }
}
