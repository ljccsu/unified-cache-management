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
#ifndef UNIFIEDCACHE_INFRA_BACKOFF_H
#define UNIFIEDCACHE_INFRA_BACKOFF_H

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <thread>

namespace UC {

// Polling helper for waits that cannot block on a notification (e.g. state in shared memory).
// Yields for the first kSpinLimit rounds so short waits stay fast, then sleeps with an
// exponentially growing interval capped at kMaxSleepUs, so long waits do not pin a core.
class Backoff {
public:
    static constexpr size_t kSpinLimit = 64;
    static constexpr size_t kMinSleepUs = 10;
    static constexpr size_t kMaxSleepUs = 100;

    void Pause() noexcept
    {
        if (spins_ < kSpinLimit) {
            ++spins_;
            std::this_thread::yield();
            return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(sleepUs_));
        sleepUs_ = std::min(sleepUs_ * 2, kMaxSleepUs);
    }
    void Reset() noexcept
    {
        spins_ = 0;
        sleepUs_ = kMinSleepUs;
    }
    bool Sleeping() const noexcept { return spins_ >= kSpinLimit; }

private:
    size_t spins_{0};
    size_t sleepUs_{kMinSleepUs};
};

}  // namespace UC

#endif  // UNIFIEDCACHE_INFRA_BACKOFF_H
