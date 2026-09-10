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
#include <cstdio>
#include "cache/cc/shm_numa_layout.h"
#ifndef UCM_SHM_NUMA_STANDALONE
#include <gtest/gtest.h>
#endif

namespace {
namespace Numa = UC::CacheStore::ShmNuma;
void Require(bool condition)
{
    if (!condition) { throw std::runtime_error("SHM NUMA layout regression"); }
}
template <typename F>
void Reject(F function)
{
    try {
        function();
    } catch (const std::invalid_argument&) {
        return;
    }
    throw std::runtime_error("invalid SHM NUMA configuration accepted");
}
void CheckLayouts()
{
    const auto nodes = Numa::DefaultNodes();
    Require(nodes == std::vector<size_t>({0, 1, 2, 3, 4, 5, 6, 7}));
    Require(Numa::ParseNodes("0-7") == nodes);
    constexpr size_t gib = 1024ULL * 1024 * 1024;
    for (size_t pageSize : {4096, 65536}) {
        const auto equal = Numa::Plan(32 * gib, pageSize, nodes);
        for (size_t i = 0; i < 8; ++i) {
            Require(equal[i].node == i && equal[i].offset == i * 4 * gib &&
                    equal[i].bytes == 4 * gib);
        }
        // Actual UCM payload can lose a shard at the capacity boundary or end in a partial page.
        for (size_t bytes : {32 * gib - 176 * 1024, 32 * gib - 1}) {
            const auto ranges = Numa::Plan(bytes, pageSize, nodes);
            size_t end = 0;
            size_t smallest = std::numeric_limits<size_t>::max(), largest = 0;
            for (const auto& range : ranges) {
                Require(range.offset == end && range.bytes % pageSize == 0);
                end += range.bytes;
                smallest = std::min(smallest, range.bytes);
                largest = std::max(largest, range.bytes);
            }
            Require(end >= bytes && end - bytes < pageSize && largest - smallest <= pageSize);
        }
    }
    std::vector<size_t> counts(8);
    for (size_t segment = 0; segment < 16; ++segment) {
        ++counts[Numa::SegmentNodes(nodes, 16, segment)[0]];
    }
    Require(counts == std::vector<size_t>(8, 2));
    Require(Numa::SegmentNodes({7, 3}, 4, 2) == std::vector<size_t>{7});
    constexpr size_t bits = sizeof(unsigned long) * 8;
    for (size_t node : {0, 1, 7, 31, 32, 63, 64, 127, 128}) {
        const auto mask = Numa::SingleNodeMask(node);
        const auto consumedBits = mask.maxNode - 1;  // Linux get_nodes ABI.
        Require(consumedBits > node && (consumedBits + bits - 1) / bits <= mask.words.size());
        std::vector<size_t> decoded;
        for (size_t bit = 0; bit < consumedBits; ++bit) {
            if ((mask.words[bit / bits] >> (bit % bits)) & 1UL) { decoded.push_back(bit); }
        }
        Require(decoded == std::vector<size_t>{node});
    }
    Reject([] { Numa::ValidateNodes({0, 0}); });
    Reject([] { Numa::ValidateNodes({static_cast<size_t>(-1)}); });
    Reject([&] { Numa::Plan(0, 4096, nodes); });
    Reject([&] { Numa::Plan(4096, 4096, nodes); });
    Reject([&] { Numa::Plan(gib, 0, nodes); });
    Reject([&] { Numa::Plan(std::numeric_limits<size_t>::max(), 4096, nodes); });
    Reject([&] { Numa::SegmentNodes(nodes, 4, 0); });
    Reject([&] { Numa::SegmentNodes(nodes, 16, 16); });
    Require(Numa::Plan(1, 4096, {}).empty());
}
}  // namespace

#ifdef UCM_SHM_NUMA_STANDALONE
int main()
{
    CheckLayouts();
    std::puts("UCM SHM NUMA layout tests passed");
}
#else
TEST(UCCacheShmNumaTest, LayoutAndNodeMask) { EXPECT_NO_THROW(CheckLayouts()); }
#endif
