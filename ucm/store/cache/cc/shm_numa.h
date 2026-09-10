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
#ifndef UNIFIEDCACHE_CACHE_STORE_CC_SHM_NUMA_H
#define UNIFIEDCACHE_CACHE_STORE_CC_SHM_NUMA_H

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <linux/mempolicy.h>
#include <map>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "logger/logger.h"
#include "shm_numa_layout.h"
#include "status/status.h"

namespace UC::CacheStore::ShmNuma {

inline void CheckSystemCall(long result, const std::string& operation)
{
    if (result < 0) {
        const auto error = errno;
        throw std::runtime_error(operation + ": " + std::strerror(error) +
                                 " (errno=" + std::to_string(error) + ")");
    }
}

inline size_t PageSize()
{
    const auto size = sysconf(_SC_PAGESIZE);
    if (size <= 0) { throw std::runtime_error("cannot determine the system page size"); }
    return static_cast<size_t>(size);
}

inline std::vector<size_t> AllowedNodes()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    const std::string prefix = "Mems_allowed_list:";
    while (std::getline(status, line)) {
        if (line.compare(0, prefix.size(), prefix) != 0) { continue; }
        const auto start = line.find_first_not_of(" \t", prefix.size());
        if (start == std::string::npos) { break; }
        return ParseNodes(line.substr(start));
    }
    throw std::runtime_error("cannot read Mems_allowed_list from /proc/self/status");
}

inline void ValidateAllowedNodes(const std::vector<size_t>& nodes)
{
    const auto allowed = AllowedNodes();
    for (const auto node : nodes) {
        if (std::find(allowed.begin(), allowed.end(), node) == allowed.end()) {
            throw std::runtime_error("NUMA node " + std::to_string(node) +
                                     " is outside Mems_allowed_list");
        }
    }
}

// For fresh POSIX SHM only. tmpfs stores mbind policy on the shared object.
// MAP_POPULATE must be absent: no page may be faulted before all ranges are bound.
inline void BindBeforeTouch(void* base, const std::vector<Range>& ranges, const std::string& name)
{
    const auto pageSize = PageSize();
    if (reinterpret_cast<size_t>(base) % pageSize != 0) {
        throw std::invalid_argument("SHM address is not page-aligned");
    }
    for (const auto& range : ranges) {
        ValidateAllowedNodes({range.node});
        const auto mask = SingleNodeMask(range.node);
        auto* address = static_cast<char*>(base) + range.offset;
        CheckSystemCall(syscall(SYS_mbind, address, range.bytes, MPOL_BIND | MPOL_F_STATIC_NODES,
                                mask.words.data(), mask.maxNode, 0UL),
                        "mbind node=" + std::to_string(range.node) +
                            " maxnode=" + std::to_string(mask.maxNode) + " shm=" + name);
        UC_INFO_UNLIMITED("SHM NUMA bind: file={} offset={} bytes={} node={}.", name, range.offset,
                          range.bytes, range.node);
    }
}

// Query every base page, without migrating any page (nodes=nullptr, flags=0).
// Failure or an unexpected node is an error, never a successful fallback.
inline void Verify(void* base, const std::vector<Range>& ranges, const std::string& name)
{
    const auto pageSize = PageSize();
    constexpr size_t batchSize = 4096;
    std::vector<void*> addresses(batchSize);
    std::vector<int> status(batchSize);
    for (const auto& range : ranges) {
        std::map<int, size_t> counts;
        size_t mismatches = 0;
        const auto pageCount = range.bytes / pageSize;
        for (size_t first = 0; first < pageCount;) {
            const auto count = std::min(batchSize, pageCount - first);
            for (size_t i = 0; i < count; ++i) {
                addresses[i] = static_cast<char*>(base) + range.offset + (first + i) * pageSize;
            }
            std::fill(status.begin(), status.end(), -EIO);
            CheckSystemCall(
                syscall(SYS_move_pages, 0, count, addresses.data(), nullptr, status.data(), 0),
                "move_pages query shm=" + name);
            for (size_t i = 0; i < count; ++i) {
                if (status[i] < 0) {
                    throw std::runtime_error(
                        "move_pages page query failed: " + std::string(std::strerror(-status[i])) +
                        " shm=" + name);
                }
                ++counts[status[i]];
                if (static_cast<size_t>(status[i]) != range.node) { ++mismatches; }
            }
            first += count;
        }
        for (const auto& [node, pages] : counts) {
            UC_INFO_UNLIMITED(
                "SHM NUMA verify: file={} offset={} expectedNode={} actualNode={} "
                "pages={} bytes={} mismatches={}.",
                name, range.offset, range.node, node, pages, pages * pageSize, mismatches);
        }
        if (mismatches != 0) {
            throw std::runtime_error("SHM NUMA placement verification failed: " + name);
        }
    }
    UC_INFO_UNLIMITED("SHM NUMA verified: file={} ranges={}.", name, ranges.size());
}

// Only the creator calls this, before publishing shared readiness or registering with a device.
inline Status Initialize(void* data, size_t bytes, const std::vector<size_t>& nodes,
                         const std::string& name)
{
    try {
        const auto ranges = Plan(bytes, PageSize(), nodes);
        if (ranges.empty()) { return Status::InvalidParam("empty SHM NUMA nodes"); }
        ValidateAllowedNodes(nodes);
        BindBeforeTouch(data, ranges, name);
        std::memset(data, 0, bytes);
        Verify(data, ranges, name);
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::Error(error.what());
    }
}

}  // namespace UC::CacheStore::ShmNuma
#endif
