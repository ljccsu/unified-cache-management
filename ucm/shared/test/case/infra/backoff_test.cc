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
#include "thread/backoff.h"
#include <atomic>
#include <chrono>
#include <ctime>
#include <gtest/gtest.h>
#include <thread>
#include "template/spsc_ring_queue.h"

class UCBackoffTest : public ::testing::Test {};

namespace {

double ThreadCpuSeconds()
{
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

}  // namespace

TEST_F(UCBackoffTest, SpinsBeforeSleeping)
{
    UC::Backoff backoff;
    for (size_t i = 0; i < UC::Backoff::kSpinLimit; ++i) {
        EXPECT_FALSE(backoff.Sleeping());
        backoff.Pause();
    }
    EXPECT_TRUE(backoff.Sleeping());
}

TEST_F(UCBackoffTest, SleepIntervalGrowsToCap)
{
    UC::Backoff backoff;
    for (size_t i = 0; i < UC::Backoff::kSpinLimit; ++i) { backoff.Pause(); }
    // 10 + 20 + 40 + 80 + 100 * 6 = 750us at least.
    constexpr size_t nSleep = 10;
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < nSleep; ++i) { backoff.Pause(); }
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(elapsed, std::chrono::microseconds(750));
}

TEST_F(UCBackoffTest, ResetRestartsSpinning)
{
    UC::Backoff backoff;
    for (size_t i = 0; i < UC::Backoff::kSpinLimit; ++i) { backoff.Pause(); }
    ASSERT_TRUE(backoff.Sleeping());
    backoff.Reset();
    EXPECT_FALSE(backoff.Sleeping());
}

TEST_F(UCBackoffTest, LongWaitDoesNotPinCore)
{
    std::atomic<bool> ready{false};
    double cpuSeconds = 0;
    std::thread waiter([&] {
        auto cpuStart = ThreadCpuSeconds();
        UC::Backoff backoff;
        while (!ready.load(std::memory_order_acquire)) { backoff.Pause(); }
        cpuSeconds = ThreadCpuSeconds() - cpuStart;
    });
    constexpr double waitSeconds = 0.2;
    std::this_thread::sleep_for(std::chrono::duration<double>(waitSeconds));
    ready.store(true, std::memory_order_release);
    waiter.join();
    // A pure yield loop burns close to waitSeconds of CPU here.
    EXPECT_LT(cpuSeconds, waitSeconds * 0.2);
}

TEST_F(UCBackoffTest, SpscPushWaitsUntilSpaceIsFreed)
{
    UC::SpscRingQueue<size_t> queue;
    queue.Setup(2);
    ASSERT_TRUE(queue.TryPush(1));
    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        queue.Push(2);
        pushed.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(pushed.load(std::memory_order_acquire));
    size_t value = 0;
    ASSERT_TRUE(queue.TryPop(value));
    EXPECT_EQ(value, 1);
    producer.join();
    EXPECT_TRUE(pushed.load(std::memory_order_acquire));
    ASSERT_TRUE(queue.TryPop(value));
    EXPECT_EQ(value, 2);
}
