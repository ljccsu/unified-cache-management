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
#ifndef UNIFIEDCACHE_CACHE_STORE_CC_SHM_NUMA_LAYOUT_H
#define UNIFIEDCACHE_CACHE_STORE_CC_SHM_NUMA_LAYOUT_H

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace UC::CacheStore::ShmNuma {

inline std::vector<size_t> DefaultNodes() { return {0, 1, 2, 3, 4, 5, 6, 7}; }

// Physical node IDs, in the user's order. Ranges such as 0-7 are accepted.
inline std::vector<size_t> ParseNodes(std::string_view text)
{
    std::vector<size_t> nodes;
    auto parse = [](std::string_view token) {
        size_t value = 0;
        const auto [end, error] = std::from_chars(token.data(), token.data() + token.size(), value);
        if (token.empty() || error != std::errc() || end != token.data() + token.size() ||
            value > 65535) {
            throw std::invalid_argument("invalid NUMA node ID: " + std::string(token));
        }
        return value;
    };
    do {
        const auto comma = text.find(',');
        const auto token = text.substr(0, comma);
        const auto dash = token.find('-');
        const auto first = parse(token.substr(0, dash));
        const auto last = dash == std::string_view::npos ? first : parse(token.substr(dash + 1));
        if (last < first) { throw std::invalid_argument("descending NUMA node range"); }
        for (size_t node = first; node <= last; ++node) {
            if (std::find(nodes.begin(), nodes.end(), node) != nodes.end()) {
                throw std::invalid_argument("duplicate NUMA node: " + std::to_string(node));
            }
            nodes.push_back(node);
        }
        if (comma == std::string_view::npos) { break; }
        text.remove_prefix(comma + 1);
    } while (true);
    return nodes;
}

struct Range {
    size_t offset;
    size_t bytes;
    size_t node;
};

struct NodeMask {
    std::vector<unsigned long> words;
    unsigned long maxNode;
};

inline NodeMask SingleNodeMask(size_t node)
{
    if (node > 65535) { throw std::invalid_argument("NUMA node ID exceeds 65535"); }
    constexpr size_t bitsPerWord = sizeof(unsigned long) * 8;
    std::vector<unsigned long> words(node / bitsPerWord + 1, 0);
    words[node / bitsPerWord] = 1UL << (node % bitsPerWord);
    // Linux get_nodes() decrements maxnode before copying/masking the bitmap.
    // Pass the allocated bit capacity + 1 so even the last bit survives.
    const auto maxNode = static_cast<unsigned long>(words.size() * bitsPerWord + 1);
    return {std::move(words), maxNode};
}

inline void ValidateNodes(const std::vector<size_t>& nodes)
{
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i] > 65535 ||
            std::find(nodes.begin(), nodes.begin() + i, nodes[i]) != nodes.begin() + i) {
            throw std::invalid_argument("invalid or duplicate share_buffer_numa_nodes");
        }
    }
}

// Preserve usable cache capacity. Distribute base pages with at most one page of imbalance.
inline std::vector<Range> Plan(size_t bytes, size_t pageSize, const std::vector<size_t>& nodes)
{
    ValidateNodes(nodes);
    if (nodes.empty()) { return {}; }
    if (bytes == 0 || pageSize == 0 ||
        bytes > std::numeric_limits<size_t>::max() - (pageSize - 1)) {
        throw std::invalid_argument("invalid SHM NUMA size or page size");
    }
    const auto pages = (bytes + pageSize - 1) / pageSize;
    if (pages < nodes.size()) {
        throw std::invalid_argument("SHM has fewer pages than NUMA nodes");
    }
    std::vector<Range> ranges;
    size_t offset = 0;
    for (size_t i = 0; i < nodes.size(); ++i) {
        const auto rangeBytes = (pages / nodes.size() + (i < pages % nodes.size())) * pageSize;
        ranges.push_back({offset, rangeBytes, nodes[i]});
        offset += rangeBytes;
    }
    return ranges;
}

inline std::vector<size_t> SegmentNodes(const std::vector<size_t>& nodes, size_t segments,
                                        size_t segment)
{
    if (nodes.empty()) { return {}; }
    if (segments == 0 || segment >= segments || segments % nodes.size() != 0) {
        throw std::invalid_argument("rank-striped segment count must be a multiple of NUMA nodes");
    }
    return {nodes[segment % nodes.size()]};
}

}  // namespace UC::CacheStore::ShmNuma

#endif
