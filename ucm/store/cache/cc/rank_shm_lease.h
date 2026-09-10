/**
 * MIT License
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 */
#ifndef UNIFIEDCACHE_CACHE_STORE_CC_RANK_SHM_LEASE_H
#define UNIFIEDCACHE_CACHE_STORE_CC_RANK_SHM_LEASE_H

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include "logger/logger.h"
#include "status/status.h"

namespace UC::CacheStore {

// All workers and watchers hold a separate shared flock for their full mapping
// lifetime. Kernel fd cleanup also releases locks after SIGKILL. The versioned
// stem excludes older processes which did not participate in this protocol.
class RankShmLease {
    static constexpr const char* suffix = "_lease_v1_rs_lock";
    std::string stem_;
    int fd_{-1};

    static std::string Path(const std::string& name) { return "/dev/shm/" + name; }
    static bool SameFile(int fd, const std::string& path)
    {
        struct stat opened{}, named{};
        return fstat(fd, &opened) == 0 && lstat(path.c_str(), &named) == 0 &&
               S_ISREG(named.st_mode) && opened.st_uid == geteuid() &&
               opened.st_dev == named.st_dev && opened.st_ino == named.st_ino;
    }
    static bool IsPayload(const std::string& name, const std::string& stem)
    {
        if (name == stem + "_rs_meta") { return true; }
        const auto prefix = stem + "_rs_data_";
        if (name.compare(0, prefix.size(), prefix) != 0 || name.size() == prefix.size()) {
            return false;
        }
        return std::all_of(name.begin() + prefix.size(), name.end(),
                           [](char ch) { return ch >= '0' && ch <= '9'; });
    }
    // Called only with the group's exclusive lock. Remove the lock name last;
    // a concurrent opener must verify its locked inode before using the group.
    static void RemoveGroup(int fd, const std::string& stem)
    {
        const auto lockPath = Path(stem + "_rs_lock");
        if (!SameFile(fd, lockPath)) { return; }
        namespace fs = std::filesystem;
        std::error_code error;
        fs::directory_iterator entries("/dev/shm", error), end;
        bool complete = !error;
        while (!error && entries != end) {
            const auto name = entries->path().filename().string();
            if (IsPayload(name, stem)) {
                const auto path = Path(name);
                struct stat info{};
                if (lstat(path.c_str(), &info) != 0) {
                    if (errno != ENOENT) { complete = false; }
                } else if (!S_ISREG(info.st_mode) || info.st_uid != geteuid()) {
                    complete = false;
                } else if (unlink(path.c_str()) != 0 && errno != ENOENT) {
                    complete = false;
                }
            }
            entries.increment(error);
        }
        if (complete && !error) {
            if (unlink(lockPath.c_str()) == 0) {
                UC_INFO("Reclaimed unused rank-striped SHM group({}).", stem);
            }
        } else {
            UC_WARN("Incomplete cleanup of rank-striped SHM group({}); retaining lease.", stem);
        }
    }

public:
    RankShmLease() = default;
    RankShmLease(const RankShmLease&) = delete;
    RankShmLease& operator=(const RankShmLease&) = delete;
    ~RankShmLease()
    {
        if (fd_ < 0) { return; }
        // A nonblocking upgrade may drop our shared lock. That is safe only
        // after all our mappings/registrations have been released.
        if (flock(fd_, LOCK_EX | LOCK_NB) == 0) {
            try { RemoveGroup(fd_, stem_); } catch (...) {}
        }
        close(fd_);
    }
    static std::string Stem(const std::string& uuid)
    { return "uc_shm_cache_" + uuid + "_lease_v1"; }
    Status Acquire(const std::string& uuid, size_t timeoutMs)
    {
        if (fd_ >= 0 || uuid.empty() || uuid.find('/') != std::string::npos ||
            uuid.find('\0') != std::string::npos) {
            return Status::InvalidParam("invalid rank-striped SHM lease");
        }
        stem_ = Stem(uuid);
        const auto path = Path(stem_ + "_rs_lock");
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
        do {
            const auto fd = open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
            if (fd < 0) { return Status::Error(std::strerror(errno)); }
            const auto result = flock(fd, LOCK_SH | LOCK_NB);
            const auto error = errno;
            if (result == 0 && SameFile(fd, path)) {
                fd_ = fd;
                return Status::OK();
            }
            close(fd);
            if (result != 0 && error != EWOULDBLOCK && error != EAGAIN && error != EINTR) {
                return Status::Error(std::strerror(error));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return Status::Error("timed out acquiring rank-striped SHM lease: " + path);
    }
    static void ReapUnused(const std::string& exceptStem = {})
    {
        namespace fs = std::filesystem;
        std::error_code error;
        fs::directory_iterator entries("/dev/shm", error), end;
        while (!error && entries != end) {
            const auto name = entries->path().filename().string();
            const std::string tail = suffix;
            if (name.compare(0, 13, "uc_shm_cache_") == 0 && name.size() > tail.size() &&
                name.compare(name.size() - tail.size(), tail.size(), tail) == 0) {
                const auto stem = name.substr(0, name.size() - std::strlen("_rs_lock"));
                if (stem != exceptStem) {
                    const auto path = Path(name);
                    const auto fd = open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
                    if (fd >= 0) {
                        if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
                            try { RemoveGroup(fd, stem); } catch (...) {}
                        }
                        close(fd);
                    }
                }
            }
            entries.increment(error);
        }
    }
};

}  // namespace UC::CacheStore
#endif
