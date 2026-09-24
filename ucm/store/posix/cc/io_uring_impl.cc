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
#include "io_uring_impl.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <sys/stat.h>
#include "logger/logger.h"
#include "thread/cpu_affinity.h"
#include "time/now_time.h"

namespace UC::PosixStore {

IoUringImpl::~IoUringImpl()
{
    stop_ = true;
    if (eventThread_.joinable()) { eventThread_.join(); }
    if (initialized_) { io_uring_queue_exit(&ring_); }
    std::lock_guard<std::mutex> lk(tableMutex_);
    if (!userDataToTag_.empty()) {
        UC_WARN("io_uring teardown: {} in-flight IO(s) abandoned (never completed; not reclaimed).",
                userDataToTag_.size());
    }
}

Status IoUringImpl::Setup(size_t timeoutMs)
{
    constexpr size_t defaultSweepIntervalMs = 100;
    constexpr size_t defaultWaitTimeoutMs = 10;
    sweepIntervalMs_ = defaultSweepIntervalMs;
    waitTimeoutMs_ = defaultWaitTimeoutMs;
    submitTimeoutMs_ = timeoutMs;
    UC_INFO(
        "io_uring setup: queueDepth={}, waitTimeoutMs={}, sweepIntervalMs={}, "
        "submitTimeoutMs={}.",
        queueDepth_, waitTimeoutMs_, sweepIntervalMs_, submitTimeoutMs_);
    auto ret = io_uring_queue_init(queueDepth_, &ring_, 0);
    if (ret < 0) {
        auto eno = -ret;
        UC_ERROR("Failed(ret={}, errno={}, message={}) to call io_uring_queue_init.", ret, eno,
                 strerror(eno));
        return Status{eno, std::string(strerror(eno))};
    }
    initialized_ = true;
    openQueue_.Setup(kOpenQueueDepth);
    eventThread_ = std::thread([this] { CompletionLoop(); });
    return Status::OK();
}

Status IoUringImpl::ReadAsync(Io&& io)
{
    auto data = std::make_unique<Callback>(std::move(io.callback));
    Track(io.tag, data.get());
    auto* sqe = io_uring_get_sqe(&ring_);
    if (sqe != nullptr) {
        io_uring_prep_read(sqe, io.fd, io.buffer, io.length, io.offset);
        io_uring_sqe_set_data(sqe, data.get());
        data.release();
        return Status::OK();
    }
    Untrack(data.get());
    UC_ERROR("Failed to acquire SQE for read io (SQ saturated).");
    return Status::Timeout();
}

Status IoUringImpl::WriteAsync(Io&& io)
{
    auto data = std::make_unique<Callback>(std::move(io.callback));
    Track(io.tag, data.get());
    auto* sqe = io_uring_get_sqe(&ring_);
    if (sqe != nullptr) {
        io_uring_prep_write(sqe, io.fd, io.buffer, io.length, io.offset);
        io_uring_sqe_set_data(sqe, data.get());
        data.release();
        return Status::OK();
    }
    Untrack(data.get());
    UC_ERROR("Failed to acquire SQE for write io (SQ saturated).");
    return Status::Timeout();
}

Status IoUringImpl::OpenAsync(OpenIo&& io)
{
    openQueue_.Push(std::move(io));
    return Status::OK();
}

void IoUringImpl::ProcessOpenQueue()
{
    OpenIo io;
    while (openQueue_.TryPop(io)) {
        auto pathPtr = std::make_shared<std::string>(std::move(io.path));
        auto userCallback = std::move(io.callback);
        auto data = std::make_unique<Callback>(
            [pathPtr, userCallback = std::move(userCallback)](Result result) {
                userCallback(result);
            });
        Track(io.tag, data.get());
        auto* sqe = io_uring_get_sqe(&ring_);
        if (sqe == nullptr) {
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
        }
        if (sqe == nullptr) {
            Untrack(data.get());
            (*data)(Result{-1, EAGAIN});
            continue;
        }
        io_uring_prep_openat(sqe, AT_FDCWD, pathPtr->c_str(), io.flags, io.mode);
        io_uring_sqe_set_data(sqe, data.get());
        data.release();
    }
}

void IoUringImpl::ProcessCancels()
{
    std::vector<void*> toCancel;
    {
        std::lock_guard<std::mutex> lk(cancelMutex_);
        toCancel.swap(cancelRequests_);
    }
    for (auto* ud : toCancel) {
        auto* sqe = io_uring_get_sqe(&ring_);
        if (sqe == nullptr) {
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
        }
        if (sqe == nullptr) { continue; }
        io_uring_prep_cancel(sqe, ud, 0);
        io_uring_sqe_set_data(sqe, nullptr);
    }
}

void IoUringImpl::CompletionLoop()
{
    auto nameStatus = CpuAffinity::SetCurrentThreadName("ucm_posix_uring");
    if (nameStatus.Failure()) {
        UC_WARN("Failed({}) to set UCM posix io_uring thread name.", nameStatus);
    }
    std::vector<io_uring_cqe*> cqes(batchCompleteSize);
    while (!stop_) {
        ProcessOpenQueue();
        ProcessCancels();
        io_uring_submit(&ring_);
        if (HarvestCompletions(cqes) == 0) {
            MaybeSweep();
            std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeoutMs_));
        }
    }
}

void IoUringImpl::MaybeSweep()
{
    if (!sweepFn_) { return; }
    auto now = NowTime::Now();
    if (sweepIntervalMs_ == 0 ||
        (now - lastSweepTp_) * 1e3 >= static_cast<double>(sweepIntervalMs_)) {
        lastSweepTp_ = now;
        sweepFn_();
    }
}

size_t IoUringImpl::HarvestCompletions(std::vector<io_uring_cqe*>& cqes)
{
    auto num = io_uring_peek_batch_cqe(&ring_, cqes.data(), static_cast<unsigned>(cqes.size()));
    for (unsigned i = 0; i < num; ++i) {
        auto* cqe = cqes[i];
        auto* cb = reinterpret_cast<Callback*>(io_uring_cqe_get_data(cqe));
        auto resVal = cqe->res;
        io_uring_cqe_seen(&ring_, cqe);
        if (cb == nullptr) { continue; }
        Result res;
        if (resVal >= 0) {
            res.nBytes = resVal;
            res.error = 0;
        } else {
            res.nBytes = -1;
            res.error = -resVal;
        }
        Untrack(cb);
        (*cb)(res);
        delete cb;
    }
    return num;
}

void IoUringImpl::Track(uint64_t tag, void* userData)
{
    if (tag == 0 || userData == nullptr) { return; }
    std::lock_guard<std::mutex> lk(tableMutex_);
    tagToUserData_[tag].push_back(userData);
    userDataToTag_[userData] = tag;
}

void IoUringImpl::Untrack(void* userData)
{
    if (userData == nullptr) { return; }
    std::lock_guard<std::mutex> lk(tableMutex_);
    auto it = userDataToTag_.find(userData);
    if (it == userDataToTag_.end()) { return; }
    auto tag = it->second;
    userDataToTag_.erase(it);
    auto vit = tagToUserData_.find(tag);
    if (vit == tagToUserData_.end()) { return; }
    auto& vec = vit->second;
    vec.erase(std::remove(vec.begin(), vec.end(), userData), vec.end());
    if (vec.empty()) { tagToUserData_.erase(vit); }
}

void IoUringImpl::CancelTask(uint64_t tag)
{
    std::vector<void*> toCancel;
    {
        std::lock_guard<std::mutex> lk(tableMutex_);
        auto it = tagToUserData_.find(tag);
        if (it == tagToUserData_.end()) {
            UC_DEBUG("io_uring cancel task({}): no tracked in-flight IO.", tag);
            return;
        }
        toCancel = std::move(it->second);
        tagToUserData_.erase(it);
        for (auto* ud : toCancel) { userDataToTag_.erase(ud); }
    }
    if (!toCancel.empty()) {
        std::lock_guard<std::mutex> lk(cancelMutex_);
        cancelRequests_.insert(cancelRequests_.end(), toCancel.begin(), toCancel.end());
        UC_WARN("io_uring cancel task({}): {} request(s) queued for cancellation.", tag,
                toCancel.size());
    }
}


}  // namespace UC::PosixStore
