/**
 * MIT License
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 */
#include <algorithm>
#include <array>
#include <cstdio>
#include <limits>
#include <stdexcept>
#include <sys/types.h>
#include "cache/cc/copy_stream.h"
#include "cache/cc/global_config.h"
#include "trans/simu/simu_stream.h"

namespace {
using UC::Status;

void Require(bool condition, const char* message)
{
    if (!condition) { throw std::runtime_error(message); }
}

class RecordingStream : public UC::Trans::SimuStream {
public:
    inline static size_t alive = 0;
    size_t loads = 0;
    size_t dumps = 0;
    size_t waits = 0;
    size_t syncs = 0;
    uintptr_t eventHandle = 0;
    bool failWait = false;
    bool failSync = false;

    RecordingStream() { ++alive; }
    ~RecordingStream() override { --alive; }

    Status HostToDeviceAsync(void* host, void* device[], const std::vector<size_t>& sizes) override
    {
        ++loads;
        return UC::Trans::Stream::HostToDeviceAsync(host, device, sizes);
    }
    Status DeviceToHostAsync(void* device[], void* host, const std::vector<size_t>& sizes) override
    {
        ++dumps;
        return UC::Trans::Stream::DeviceToHostAsync(device, host, sizes);
    }
    Status WaitEvent(const UC::Trans::Event& event) override
    {
        ++waits;
        eventHandle = event.NativeHandle();
        return failWait ? Status::Error() : Status::OK();
    }
    Status Synchronized() override
    {
        ++syncs;
        auto status = SimuStream::Synchronized();
        return failSync ? Status::Error() : status;
    }
};

std::vector<std::weak_ptr<RecordingStream>> created;
size_t attempts = 0;
size_t failAt = std::numeric_limits<size_t>::max();

std::shared_ptr<UC::Trans::Stream> MakeTestStream()
{
    if (attempts++ == failAt) { return nullptr; }
    auto stream = std::make_shared<RecordingStream>();
    Require(stream->Setup().Success(), "simulated stream setup failed");
    created.push_back(stream);
    return stream;
}

void CheckDefaults()
{
    UC::CacheStore::Config config;
    config.cacheSdmaDirect = false;
    Require(config.EffectiveStreamNumber() == 4, "ordinary default changed");
    config.cacheSdmaDirect = true;
    Require(config.EffectiveStreamNumber() == 1, "SDMA default changed");
    for (const size_t number : {1, 4, 16, 32}) {
        config.streamNumber = number;
        Require(config.EffectiveStreamNumber() == number, "SDMA stream override ignored");
        config.cacheSdmaDirect = false;
        Require(config.EffectiveStreamNumber() == number, "ordinary stream override ignored");
        config.cacheSdmaDirect = true;
    }
}

void CheckCopies(size_t number)
{
    created.clear();
    UC::CacheStore::CopyStream streams;
    Require(streams.SetupSdmaDirect(0, number, false).Success(), "SDMA pool setup failed");
    Require(created.size() == number, "wrong number of streams created");
    for (size_t i = 0; i < 2 * number; ++i) {
        Require(streams.NextStream() == created[i % number].lock(), "round-robin order changed");
    }

    constexpr size_t shards = 512;
    using Payload = std::array<unsigned char, 176>;
    std::vector<Payload> host(shards), device(shards), output(shards);
    const std::vector<size_t> sizes{128, 16, 32};
    Require(streams.WaitEvent(UC::Trans::Event{123}).Success(), "input event wait failed");
    for (size_t i = 0; i < shards; ++i) {
        for (size_t j = 0; j < host[i].size(); ++j) {
            host[i][j] = static_cast<unsigned char>((i + j) % 251);
        }
        void* destinations[]{device[i].data(), device[i].data() + 128, device[i].data() + 144};
        Require(streams.HostToDeviceAsync(host[i].data(), destinations, sizes).Success(),
                "H2D submission failed");
    }
    Require(streams.Synchronize().Success(), "H2D synchronization failed");
    Require(host == device, "H2D completion did not cover all streams");
    for (size_t i = 0; i < shards; ++i) {
        void* sources[]{device[i].data(), device[i].data() + 128, device[i].data() + 144};
        Require(streams.DeviceToHostAsync(sources, output[i].data(), sizes).Success(),
                "D2H submission failed");
    }
    Require(streams.Synchronize().Success(), "D2H synchronization failed");
    Require(host == output, "D2H completion did not cover all streams");
    for (const auto& weak : created) {
        const auto stream = weak.lock();
        Require(stream->loads == shards / number && stream->dumps == shards / number,
                "shards were not distributed across all streams");
        Require(stream->waits == 1 && stream->eventHandle == 123,
                "input event did not reach every stream");
        Require(stream->syncs == 2, "not every stream was synchronized");
    }

    // An error on the first stream must not skip waits or draining later streams.
    created.front().lock()->failWait = true;
    created.front().lock()->failSync = true;
    Require(streams.WaitEvent(UC::Trans::Event{456}).Failure(), "event error was lost");
    Require(streams.Synchronize().Failure(), "synchronization error was lost");
    for (const auto& weak : created) {
        const auto stream = weak.lock();
        Require(stream->waits == 2 && stream->syncs == 3, "error skipped a later stream");
    }
}

void CheckSetupFailures()
{
    created.clear();
    UC::CacheStore::CopyStream streams;
    Require(streams.SetupSdmaDirect(0, 0, false).Failure(), "zero streams accepted");
    Require(streams.SetupSdmaDirect(0, 33, false).Failure(), "too many streams accepted");
    Require(streams.SetupSdmaDirect(0, 16, true).Failure(), "GDR accepted for SDMA Direct");
    Require(streams.SetupSdmaDirect(-1, 16, false).Failure(), "invalid device accepted");
    Require(created.empty(), "invalid setup created streams");

    failAt = attempts + 3;
    Require(streams.SetupSdmaDirect(0, 16, false).Failure(), "factory failure was lost");
    Require(RecordingStream::alive == 0, "partial setup leaked streams");
    Require(streams.NextStream() == nullptr, "partial pool was published");
    failAt = std::numeric_limits<size_t>::max();
    created.clear();
    Require(streams.SetupSdmaDirect(0, 4, false).Success(), "retry after setup failure failed");
}
}  // namespace

// These definitions are deliberately isolated from the production device library.
namespace UC::Trans {
Status Device::Setup(int32_t deviceId)
{ return deviceId < 0 ? Status::InvalidParam() : Status::OK(); }
std::shared_ptr<Stream> Device::MakeSdmaDirectStream() { return MakeTestStream(); }
}  // namespace UC::Trans

int main()
{
    try {
        CheckDefaults();
        for (const size_t number : {1, 4, 16, 32}) { CheckCopies(number); }
        CheckSetupFailures();
        Require(RecordingStream::alive == 0, "stream teardown leaked resources");
        std::puts("Cache stream defaults, round-robin copies, events, sync and failure cleanup passed");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "%s\n", error.what());
        return 1;
    }
}
