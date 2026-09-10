/**
 * MIT License
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 */
#include <csignal>
#include <filesystem>
#include <memory>
#include <poll.h>
#include <sys/wait.h>
#include <gtest/gtest.h>
#include "cache/cc/posix_shm.h"
#include "cache/cc/rank_shm_lease.h"
#include "detail/random.h"

namespace {
using UC::CacheStore::PosixShm;
using UC::CacheStore::RankShmLease;
using UC::Status;

bool Exists(const std::string& name)
{ return std::filesystem::exists("/dev/shm/" + name); }

bool CreatePayload(const std::string& name)
{
    PosixShm file{name};
    return file.ShmOpen(PosixShm::OpenFlag::READ_WRITE | PosixShm::OpenFlag::CREATE |
                        PosixShm::OpenFlag::EXCL).Success() && file.Truncate(4096).Success();
}

TEST(UCCacheRankShmLeaseTest, OnlyLastParticipantRemovesWholeGroup)
{
    UC::Test::Detail::Random random;
    const auto uuid = random.RandomString(20);
    const auto stem = RankShmLease::Stem(uuid);
    auto worker = std::make_unique<RankShmLease>();
    auto watcher = std::make_unique<RankShmLease>();
    ASSERT_EQ(worker->Acquire(uuid, 1000), Status::OK());
    ASSERT_EQ(watcher->Acquire(uuid, 1000), Status::OK());
    ASSERT_TRUE(CreatePayload(stem + "_rs_meta"));
    ASSERT_TRUE(CreatePayload(stem + "_rs_data_0"));
    ASSERT_TRUE(CreatePayload(stem + "_rs_data_15"));
    RankShmLease::ReapUnused();
    EXPECT_TRUE(Exists(stem + "_rs_data_15"));
    worker.reset();
    RankShmLease::ReapUnused();
    EXPECT_TRUE(Exists(stem + "_rs_meta"));
    EXPECT_TRUE(Exists(stem + "_rs_data_0"));
    watcher.reset();
    EXPECT_FALSE(Exists(stem + "_rs_meta"));
    EXPECT_FALSE(Exists(stem + "_rs_data_0"));
    EXPECT_FALSE(Exists(stem + "_rs_data_15"));
    EXPECT_FALSE(Exists(stem + "_rs_lock"));
}

void CheckKilledCreator(bool livePeer)
{
    UC::Test::Detail::Random random;
    const auto uuid = random.RandomString(20);
    const auto stem = RankShmLease::Stem(uuid);
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);
    const auto child = fork();
    if (child == 0) {
        close(pipeFds[0]);
        RankShmLease lease;
        // Kill during partial initialization: data exists but no metadata yet.
        const char ready = lease.Acquire(uuid, 1000).Success() &&
                           CreatePayload(stem + "_rs_data_0");
        (void)write(pipeFds[1], &ready, 1);
        for (;;) { pause(); }
    }
    close(pipeFds[1]);
    if (child < 0) {
        close(pipeFds[0]);
        FAIL() << "fork failed";
    }
    pollfd pending{pipeFds[0], POLLIN, 0};
    char ready = 0;
    if (poll(&pending, 1, 2000) > 0) { (void)read(pipeFds[0], &ready, 1); }
    close(pipeFds[0]);
    EXPECT_EQ(ready, 1);
    std::unique_ptr<RankShmLease> peer;
    if (livePeer && ready) {
        peer = std::make_unique<RankShmLease>();
        EXPECT_EQ(peer->Acquire(uuid, 1000), Status::OK());
    }
    RankShmLease::ReapUnused();
    if (ready) { EXPECT_TRUE(Exists(stem + "_rs_data_0")); }
    EXPECT_EQ(kill(child, SIGKILL), 0);
    int status = 0;
    EXPECT_EQ(waitpid(child, &status, 0), child);
    RankShmLease::ReapUnused();
    EXPECT_EQ(Exists(stem + "_rs_data_0"), livePeer && ready);
    peer.reset();
    EXPECT_FALSE(Exists(stem + "_rs_data_0"));
    EXPECT_FALSE(Exists(stem + "_rs_lock"));
}

TEST(UCCacheRankShmLeaseTest, ReclaimsPartialGroupAfterSigkill)
{ CheckKilledCreator(false); }

TEST(UCCacheRankShmLeaseTest, SigkillDoesNotReclaimLivePeers)
{ CheckKilledCreator(true); }

TEST(UCCacheRankShmLeaseTest, PreservesLegacyFilesWithoutLease)
{
    UC::Test::Detail::Random random;
    const auto name = "uc_shm_cache_" + random.RandomString(20) + "_rs_data_0";
    ASSERT_TRUE(CreatePayload(name));
    RankShmLease::ReapUnused();
    EXPECT_TRUE(Exists(name));
    PosixShm{name}.ShmUnlink();
}
}  // namespace
