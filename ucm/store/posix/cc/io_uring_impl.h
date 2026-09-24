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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_IO_URING_IMPL_H
#define UNIFIEDCACHE_POSIX_STORE_CC_IO_URING_IMPL_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <liburing.h>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "status/status.h"
#include "template/spsc_ring_queue.h"

namespace UC::PosixStore {

class IoUringImpl {
public:
    struct Result {
        ssize_t nBytes;
        int32_t error;
    };
    using Callback = std::function<void(Result)>;
    struct Io {
        int32_t fd;
        uint64_t offset;
        uint32_t length;
        void* buffer;
        Callback callback;
        uint64_t tag{0};
    };
    struct OpenIo {
        std::string path;
        int32_t flags;
        int32_t mode;
        Callback callback;
        uint64_t tag{0};
    };
    using SweepFn = std::function<void()>;

    ~IoUringImpl();
    Status Setup(size_t timeoutMs);
    Status ReadAsync(Io&& io);
    Status WriteAsync(Io&& io);
    Status OpenAsync(OpenIo&& io);
    void SetSweepFn(SweepFn fn) { sweepFn_ = std::move(fn); }
    void CancelTask(uint64_t tag);

private:
    void CompletionLoop();
    void ProcessOpenQueue();
    void ProcessCancels();
    void MaybeSweep();
    size_t HarvestCompletions(std::vector<io_uring_cqe*>& cqes);

    void Track(uint64_t tag, void* userData);
    void Untrack(void* userData);

    size_t queueDepth_{4096};
    size_t waitTimeoutMs_{10};
    size_t sweepIntervalMs_{100};
    size_t submitTimeoutMs_{0};
    size_t batchCompleteSize{512};
    static constexpr size_t kOpenQueueDepth = 8192;
    struct io_uring ring_{};
    bool initialized_{false};
    std::atomic_bool stop_{false};
    std::thread eventThread_;
    SweepFn sweepFn_{nullptr};
    double lastSweepTp_{0};
    std::mutex tableMutex_;
    std::mutex cancelMutex_;
    SpscRingQueue<OpenIo> openQueue_;
    std::vector<void*> cancelRequests_;
    std::unordered_map<uint64_t, std::vector<void*>> tagToUserData_;
    std::unordered_map<void*, uint64_t> userDataToTag_;
};

}  // namespace UC::PosixStore

#endif