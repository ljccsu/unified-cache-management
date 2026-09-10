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
#include "trans_buffer.h"
#include <atomic>
#include <cstring>
#include <filesystem>
#include <thread>
#include <unistd.h>
#include "logger/logger.h"
#include "posix_shm.h"
#include "shm_numa.h"
#include "trans/buffer.h"
#include "trans/device.h"

namespace UC::CacheStore {

static constexpr size_t nHashTableBucket = 16411;
static constexpr auto invalidIndex = std::numeric_limits<size_t>::max();

static inline size_t Hash(const Detail::BlockId& blockId, size_t shard)
{
    static UC::Detail::BlockIdHasher blockIdHasher;
    static std::hash<size_t> shardHasher;
    constexpr auto goldenSection = 0x9e3779b97f4a7c15ULL;
    size_t h1 = blockIdHasher(blockId);
    size_t h2 = shardHasher(shard);
    return (h1 ^ (h2 + goldenSection + (h1 << 6) + (h1 >> 2))) % nHashTableBucket;
}

struct BufferMetaNode {
    Detail::BlockId block;
    size_t shard;
    size_t reference;
    size_t hash;
    size_t prev;
    size_t next;
    alignas(64) std::atomic<TransBuffer::State> state;
    std::atomic<int32_t> errorCode;
    void Init()
    {
        reference = 0;
        hash = invalidIndex;
        prev = invalidIndex;
        next = invalidIndex;
        state.store(TransBuffer::State::LOADING, std::memory_order_relaxed);
        errorCode.store(Status::OK().Underlying(), std::memory_order_relaxed);
    }
};
static_assert(std::atomic<TransBuffer::State>::is_always_lock_free, "state must be lock-free");
static_assert(std::atomic<int32_t>::is_always_lock_free, "errorCode must be lock-free");

class BufferStrategy {
protected:
    struct BaseConfig {
        int32_t deviceId{-1};
        size_t nodeSize{0};
        size_t totalSize{0};
        size_t reservedNumber{0};
    };
    BaseConfig base_;

public:
    BufferStrategy(int32_t deviceId, size_t nodeSize, size_t totalSize, size_t reservedNumber)
        : base_({deviceId, nodeSize, totalSize, reservedNumber})
    {
    }
    virtual ~BufferStrategy() = default;
    virtual Status Setup() = 0;
    virtual void BucketLock(size_t iBucket) = 0;
    virtual bool BucketTryLock(size_t iBucket) = 0;
    virtual void BucketUnlock(size_t iBucket) = 0;
    virtual void NodeLock(size_t iNode) = 0;
    virtual void NodeUnlock(size_t iNode) = 0;
    virtual size_t& FirstAt(size_t iBucket) = 0;
    virtual size_t FetchNode(bool allowReserved, size_t preferredSegment, size_t attempt) = 0;
    virtual void* DataAt(size_t iNode) = 0;
    virtual void* DeviceDataAt(size_t iNode) = 0;
    virtual size_t SegmentAt(size_t iNode) const = 0;
    virtual BufferMetaNode* MetaAt(size_t iNode) = 0;
    virtual void MarkAccessed(size_t iNode) = 0;
};

class LocalBufferStrategy : public BufferStrategy {
    struct BufferHeader {
        size_t buckets[nHashTableBucket];
        size_t nodeCursor;
        size_t nodeSize;
        size_t nNode;
    };
    struct LocalMutex {
        pthread_mutex_t mutex;
        ~LocalMutex() { pthread_mutex_destroy(&mutex); }
        void Init()
        {
            pthread_mutexattr_t attr;
            pthread_mutexattr_init(&attr);
            pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE);
            pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
            pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);
            pthread_mutex_init(&mutex, &attr);
            pthread_mutexattr_destroy(&attr);
        }
        void Lock() { pthread_mutex_lock(&mutex); }
        bool TryLock() { return pthread_mutex_trylock(&mutex) == 0; }
        void Unlock() { pthread_mutex_unlock(&mutex); }
    };
    struct LocalLock {
        pthread_spinlock_t lock;
        ~LocalLock() { pthread_spin_destroy(&lock); }
        void Init() { pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE); }
        void Lock() { pthread_spin_lock(&lock); }
        bool TryLock() { return pthread_spin_trylock(&lock) == 0; }
        void Unlock() { pthread_spin_unlock(&lock); }
    };

    bool ioDirect_{false};
    bool mapHostToDevice_{false};
    BufferHeader header_;
    LocalMutex bucketLocks_[nHashTableBucket];
    std::unique_ptr<LocalLock[]> nodeLocks_;
    std::unique_ptr<BufferMetaNode[]> meta_;
    std::unique_ptr<std::atomic<uint8_t>[]> accessed_;
    std::shared_ptr<void> data_;
    std::byte* dataOnDevice_{nullptr};
    bool registeredMappedHost_{false};

public:
    LocalBufferStrategy(int32_t deviceId, size_t nodeSize, size_t totalSize, size_t reservedNumber,
                        bool ioDirect, bool mapHostToDevice)
        : BufferStrategy(deviceId, nodeSize, totalSize, reservedNumber),
          ioDirect_(ioDirect),
          mapHostToDevice_(mapHostToDevice)
    {
    }
    ~LocalBufferStrategy() override
    {
        if (registeredMappedHost_ && data_) { Trans::Buffer::UnregisterHostBuffer(data_.get()); }
    }
    Status Setup() override
    {
        const auto deviceId = base_.deviceId;
        const auto totalSize = base_.totalSize;
        const auto nodeSize = base_.nodeSize;
        auto nNode = totalSize / nodeSize;
        try {
            nodeLocks_ = std::make_unique<LocalLock[]>(nNode);
            meta_ = std::make_unique<BufferMetaNode[]>(nNode);
            accessed_ = std::make_unique<std::atomic<uint8_t>[]>(nNode);
            for (size_t i = 0; i < nHashTableBucket; i++) { bucketLocks_[i].Init(); }
            for (size_t i = 0; i < nNode; i++) {
                nodeLocks_[i].Init();
                accessed_[i].store(0, std::memory_order_relaxed);
            }
        } catch (const std::exception& e) {
            UC_ERROR("Failed({}) to alloc buffer.", e.what());
            return Status::Error(e.what());
        }
        Trans::Device device;
        auto s = device.Setup(deviceId);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to setup device({}).", s, deviceId);
            return s;
        }
        auto buffer = device.MakeBuffer();
        if (!buffer) [[unlikely]] {
            UC_ERROR("Failed to make buffer on device({}).", deviceId);
            return Status::Error();
        }
        data_ = ioDirect_ ? buffer->MakeHostBuffer4DirectIo(nodeSize * nNode)
                          : buffer->MakeHostBuffer(nodeSize * nNode);
        if (!data_) [[unlikely]] {
            UC_ERROR("Failed to make pinned({}) for device({}).", nodeSize * nNode, deviceId);
            return Status::OutOfMemory();
        }
        if (mapHostToDevice_) {
            void* deviceData = nullptr;
            auto s = Status::OK();
            if (ioDirect_) {
                s = Trans::Buffer::GetHostDevicePointer(data_.get(), &deviceData);
            } else {
                s = Trans::Buffer::RegisterHostBuffer(data_.get(), nodeSize * nNode, &deviceData);
                registeredMappedHost_ = s.Success();
            }
            if (s.Failure()) [[unlikely]] {
                UC_ERROR("Failed({}) to map pinned host buffer({}) to device({}).", s,
                         nodeSize * nNode, deviceId);
                return s;
            }
            dataOnDevice_ = static_cast<std::byte*>(deviceData);
        }
        for (size_t i = 0; i < nHashTableBucket; i++) { header_.buckets[i] = invalidIndex; }
        for (size_t i = 0; i < nNode; i++) { meta_[i].Init(); }
        header_.nodeCursor = 0;
        header_.nodeSize = nodeSize;
        header_.nNode = nNode;
        return Status::OK();
    }
    void BucketLock(size_t iBucket) override { bucketLocks_[iBucket].Lock(); }
    bool BucketTryLock(size_t iBucket) override { return bucketLocks_[iBucket].TryLock(); }
    void BucketUnlock(size_t iBucket) override { bucketLocks_[iBucket].Unlock(); }
    void NodeLock(size_t iNode) override { nodeLocks_[iNode].Lock(); }
    void NodeUnlock(size_t iNode) override { nodeLocks_[iNode].Unlock(); }
    size_t& FirstAt(size_t iBucket) override { return header_.buckets[iBucket]; }
    size_t FetchNode(bool allowReserved, size_t /*preferredSegment*/, size_t /*attempt*/) override
    {
        const auto total = header_.nNode - (allowReserved ? 0 : base_.reservedNumber);
        for (size_t i = 0; i < 2 * total; ++i) {
            auto cur = header_.nodeCursor++ % total;
            uint8_t expected = 1;
            if (accessed_[cur].compare_exchange_strong(expected, 0, std::memory_order_relaxed,
                                                       std::memory_order_relaxed)) {
                continue;
            }
            return cur;
        }
        return header_.nodeCursor++ % total;
    }
    void MarkAccessed(size_t iNode) override
    { accessed_[iNode].store(1, std::memory_order_relaxed); }
    void* DataAt(size_t iNode) override
    { return ((std::byte*)data_.get()) + header_.nodeSize * iNode; }
    void* DeviceDataAt(size_t iNode) override
    {
        if (dataOnDevice_ == nullptr) { return nullptr; }
        return dataOnDevice_ + header_.nodeSize * iNode;
    }
    size_t SegmentAt(size_t /*iNode*/) const override { return 0; }
    BufferMetaNode* MetaAt(size_t iNode) override { return meta_.get() + iNode; }
};

class SharedBufferStrategy : public BufferStrategy {
protected:
    struct ShareMutex {
        pthread_mutex_t mutex;
        ~ShareMutex() = delete;
        void Init()
        {
            pthread_mutexattr_t attr;
            pthread_mutexattr_init(&attr);
            pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
            pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
            pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);
            pthread_mutex_init(&mutex, &attr);
            pthread_mutexattr_destroy(&attr);
        }
        void Lock() { pthread_mutex_lock(&mutex); }
        bool TryLock() { return pthread_mutex_trylock(&mutex) == 0; }
        void Unlock() { pthread_mutex_unlock(&mutex); }
    };
    struct ShareLock {
        pthread_spinlock_t lock;
        ~ShareLock() = delete;
        void Init() { pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED); }
        void Lock() { pthread_spin_lock(&lock); }
        bool TryLock() { return pthread_spin_trylock(&lock) == 0; }
        void Unlock() { pthread_spin_unlock(&lock); }
    };
    static constexpr size_t sharedBufferMagic = (('S' << 16) | ('b' << 8) | 3);
    static constexpr size_t rankNumaMagic = (('S' << 16) | ('b' << 8) | 4);
    enum class SharedLayout : size_t { SINGLE = 0, RANK_STRIPED = 1 };
    struct BufferHeader {
        std::atomic<size_t> magic;
        size_t nNode;
        size_t segmentCount;
        size_t nodesPerSegment;
        SharedLayout layout;
        alignas(64) std::atomic<size_t> nodeCursor;
        char nodeCursorPad[64 - sizeof(std::atomic<size_t>)];
        size_t buckets[nHashTableBucket];
        ShareMutex bucketLocks[nHashTableBucket];
        ShareLock nodeLocks[0];
    };
    static_assert(std::atomic<size_t>::is_always_lock_free, "nodeCursor must be lock-free");
    static_assert(std::atomic<uint8_t>::is_always_lock_free, "accessed must be lock-free");

    BufferHeader* header_{nullptr};
    BufferMetaNode* meta_{nullptr};
    std::atomic<uint8_t>* accessed_{nullptr};
    std::atomic<size_t>* segmentCursors_{nullptr};
    std::atomic<uint8_t>* segmentReady_{nullptr};
    std::byte* data_{nullptr};
    std::byte* dataOnDevice_{nullptr};
    const std::string& uuid_;
    std::string shmName_;
    size_t nodeSize_{0};
    size_t nNode_{0};
    size_t segmentCount_{1};
    size_t nodesPerSegment_{0};
    SharedLayout layout_{SharedLayout::SINGLE};
    void* addrress_{nullptr};
    size_t totalSize_{0};
    bool unlinkShm_{true};
    size_t setupTimeoutMs_{30000};

    virtual void InitLayoutMetadata() {}
    virtual Status CheckLayoutMetadata() const { return Status::OK(); }
    size_t ExpectedMagic() const
    {
        return layout_ == SharedLayout::SINGLE ? sharedBufferMagic : rankNumaMagic;
    }
    Status WaitShmSize(PosixShm& file, size_t minimum) const
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(setupTimeoutMs_);
        for (;;) {
            size_t size = 0;
            auto status = file.Size(size);
            if (status.Failure()) { return status; }
            if (size >= minimum) { return Status::OK(); }
            if (std::chrono::steady_clock::now() >= deadline) {
                return Status::Error(
                    fmt::format("rank-striped SHM({}) truncate not ready", file.ShmName()));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    size_t AccessedOffset() const noexcept
    {
        constexpr auto align = 64;
        auto off = sizeof(BufferHeader) + sizeof(ShareLock) * nNode_;
        return (off + align - 1) & ~(align - 1);
    }
    size_t AccessedSize() const noexcept { return sizeof(std::atomic<uint8_t>) * nNode_; }
    size_t SegmentCursorsOffset() const noexcept
    {
        constexpr auto align = alignof(std::atomic<size_t>);
        auto off = AccessedOffset() + AccessedSize();
        return (off + align - 1) & ~(align - 1);
    }
    size_t SegmentCursorsSize() const noexcept
    { return sizeof(std::atomic<size_t>) * segmentCount_; }
    size_t SegmentReadyOffset() const noexcept
    {
        auto off = SegmentCursorsOffset() + SegmentCursorsSize();
        constexpr auto align = alignof(std::atomic<uint8_t>);
        return (off + align - 1) & ~(align - 1);
    }
    size_t SegmentReadySize() const noexcept
    { return sizeof(std::atomic<uint8_t>) * segmentCount_; }
    size_t MetaOffset() const noexcept
    {
        constexpr auto align = alignof(BufferMetaNode);
        auto off = SegmentReadyOffset() + SegmentReadySize();
        return (off + align - 1) & ~(align - 1);
    }
    virtual size_t DataOffset() const noexcept
    {
        static const auto pageSize = sysconf(_SC_PAGESIZE);
        const auto size = MetaOffset() + sizeof(BufferMetaNode) * nNode_;
        return (size + pageSize - 1) & ~(pageSize - 1);
    }
    size_t DataSize() const noexcept { return nodeSize_ * nNode_; }
    void InitPointers() noexcept
    {
        auto base = static_cast<std::byte*>(addrress_);
        accessed_ = reinterpret_cast<std::atomic<uint8_t>*>(base + AccessedOffset());
        segmentCursors_ = reinterpret_cast<std::atomic<size_t>*>(base + SegmentCursorsOffset());
        segmentReady_ = reinterpret_cast<std::atomic<uint8_t>*>(base + SegmentReadyOffset());
        meta_ = reinterpret_cast<BufferMetaNode*>(base + MetaOffset());
    }
    static const std::string& ShmPrefix() noexcept
    {
        static std::string prefix{"uc_shm_cache_"};
        return prefix;
    }
    static void CleanUpShmFileExceptMe(const std::string& me)
    {
        namespace fs = std::filesystem;
        std::string_view prefix = ShmPrefix();
        fs::path shmDir = "/dev/shm";
        if (!fs::exists(shmDir)) { return; }
        const auto now = fs::file_time_type::clock::now();
        const auto keepThreshold = std::chrono::minutes(10);
        for (const auto& entry : fs::directory_iterator(shmDir)) {
            const auto& path = entry.path();
            const auto& name = path.filename().string();
            if (!entry.is_regular_file() || name.compare(0, prefix.size(), prefix) != 0 ||
                name == me) {
                continue;
            }
            // Rank-striped payload is split across sibling files. A legacy cleanup scan cannot
            // tell whether those siblings still belong to a live mapping, so only their exact
            // owner lifecycle may unlink them.
            if (name.find("_rs_") != std::string::npos) { continue; }
            try {
                const auto lwt = fs::last_write_time(path);
                if (now - lwt <= keepThreshold) { continue; }
                fs::remove(path);
            } catch (...) {
            }
        }
    }
    static Status MmapShmFile(PosixShm& shmFile, const size_t size, void*& addr,
                              bool needTrunc = true, bool populate = true)
    {
        auto s = Status::OK();
        if (needTrunc) {
            s = shmFile.Truncate(size);
            if (s.Failure()) [[unlikely]] {
                UC_ERROR("Failed({}) to trunc file({}) with size({}).", s, shmFile.ShmName(), size);
                return s;
            }
        }
        s = shmFile.MMap(addr, size, true, true, true, populate);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to mmap file({}) with size({}).", s, shmFile.ShmName(), size);
            return s;
        }
        return Status::OK();
    }
    Status WaitShmHeaderReady(BufferHeader* header) const
    {
        constexpr auto retryInterval = std::chrono::milliseconds(100);
        const auto maxTryTime =
            layout_ == SharedLayout::SINGLE ? size_t{100} : setupTimeoutMs_ / 100;
        size_t tryTime = 0;
        do {
            const auto magic = header->magic.load(std::memory_order_acquire);
            if (magic == ExpectedMagic()) { break; }
            if (layout_ == SharedLayout::RANK_STRIPED && magic != 0) {
                return Status::InvalidParam(
                    "rank-striped SHM({}) version mismatch; use a fresh unique_id", shmName_);
            }
            if (tryTime > maxTryTime) { return Status::Retry(); }
            std::this_thread::sleep_for(retryInterval);
            tryTime++;
        } while (true);
        return Status::OK();
    }
    Status InitShmBuffer(PosixShm& shmFile)
    {
        auto s = MmapShmFile(shmFile, totalSize_, addrress_);
        if (s.Failure()) [[unlikely]] { return s; }
        header_ = static_cast<BufferHeader*>(addrress_);
        InitPointers();
        header_->nNode = nNode_;
        header_->segmentCount = segmentCount_;
        header_->nodesPerSegment = nodesPerSegment_;
        header_->layout = layout_;
        header_->nodeCursor.store(0, std::memory_order_relaxed);
        for (size_t i = 0; i < segmentCount_; i++) {
            segmentCursors_[i].store(0, std::memory_order_relaxed);
            segmentReady_[i].store(0, std::memory_order_relaxed);
        }
        for (size_t i = 0; i < nHashTableBucket; i++) {
            header_->buckets[i] = invalidIndex;
            header_->bucketLocks[i].Init();
        }
        for (size_t i = 0; i < nNode_; i++) {
            header_->nodeLocks[i].Init();
            meta_[i].Init();
            accessed_[i].store(0, std::memory_order_relaxed);
        }
        InitLayoutMetadata();
        header_->magic.store(ExpectedMagic(), std::memory_order_release);
        return Status::OK();
    }
    Status LoadShmBuffer(PosixShm& shmFile)
    {
        auto s = shmFile.ShmOpen(PosixShm::OpenFlag::READ_WRITE);
        if (s.Failure()) {
            UC_ERROR("Failed({}) to open file({}).", s, shmFile.ShmName());
            return s;
        }
        if (layout_ == SharedLayout::RANK_STRIPED) {
            s = WaitShmSize(shmFile, sizeof(BufferHeader));
            if (s.Failure()) { return s; }
        }
        s = MmapShmFile(shmFile, totalSize_, addrress_, false, layout_ == SharedLayout::SINGLE);
        if (s.Failure()) [[unlikely]] { return s; }
        header_ = static_cast<BufferHeader*>(addrress_);
        s = WaitShmHeaderReady(header_);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Shm file({}) not ready.", shmFile.ShmName());
            return s;
        }
        if (header_->nNode != nNode_ || header_->segmentCount != segmentCount_ ||
            header_->nodesPerSegment != nodesPerSegment_ || header_->layout != layout_) {
            return Status::InvalidParam(
                "shared buffer layout mismatch: nodes={}/{}, segments={}/{}, perSegment={}/{}",
                nNode_, header_->nNode, segmentCount_, header_->segmentCount, nodesPerSegment_,
                header_->nodesPerSegment);
        }
        s = CheckLayoutMetadata();
        if (s.Failure()) { return s; }
        InitPointers();
        return Status::OK();
    }
    Status RegisterBuffer(int32_t deviceId)
    {
        data_ = static_cast<std::byte*>(addrress_) + DataOffset();
        Trans::Device device;
        auto s = device.Setup(deviceId);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to setup device({}).", s, deviceId);
            return s;
        }
        const auto dataSize = DataSize();
        s = Trans::Buffer::RegisterHostBuffer((void*)data_, dataSize, (void**)&dataOnDevice_);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to register buffer({}) to device({}).", s, dataSize, deviceId);
            return s;
        }
        return Status::OK();
    }

public:
    SharedBufferStrategy(const std::string& uuid, int32_t deviceId, size_t nodeSize,
                         size_t totalSize, size_t reservedNumber)
        : BufferStrategy(deviceId, nodeSize, totalSize, reservedNumber), uuid_(uuid)
    {
    }
    ~SharedBufferStrategy() override
    {
        if (data_) { Trans::Buffer::UnregisterHostBuffer(data_); }
        if (addrress_) { PosixShm::MUnmap(addrress_, totalSize_); }
        if (unlinkShm_) { PosixShm{shmName_}.ShmUnlink(); }
    }
    Status Setup() override
    {
        const auto& uuid = uuid_;
        const auto deviceId = base_.deviceId;
        const auto nodeSize = base_.nodeSize;
        const auto totalSize = base_.totalSize;
        shmName_ = ShmPrefix() + uuid;
        nodeSize_ = nodeSize;
        nNode_ = totalSize / nodeSize;
        segmentCount_ = 1;
        nodesPerSegment_ = nNode_;
        CleanUpShmFileExceptMe(shmName_);
        PosixShm shmFile{shmName_};
        const auto dataOffset = DataOffset();
        totalSize_ = dataOffset + DataSize();
        const auto flags =
            PosixShm::OpenFlag::CREATE | PosixShm::OpenFlag::EXCL | PosixShm::OpenFlag::READ_WRITE;
        auto s = shmFile.ShmOpen(flags);
        if (s.Success()) {
            s = InitShmBuffer(shmFile);
        } else if (s == Status::DuplicateKey()) {
            s = LoadShmBuffer(shmFile);
        } else {
            UC_ERROR("Failed({}) to open file({}) with flags({}).", s, shmName_, flags);
            return s;
        }
        return RegisterBuffer(deviceId);
    }
    void BucketLock(size_t iBucket) override { header_->bucketLocks[iBucket].Lock(); }
    bool BucketTryLock(size_t iBucket) override { return header_->bucketLocks[iBucket].TryLock(); }
    void BucketUnlock(size_t iBucket) override { header_->bucketLocks[iBucket].Unlock(); }
    void NodeLock(size_t iNode) override { header_->nodeLocks[iNode].Lock(); }
    void NodeUnlock(size_t iNode) override { header_->nodeLocks[iNode].Unlock(); }
    size_t& FirstAt(size_t iBucket) override { return header_->buckets[iBucket]; }
    size_t FetchNode(bool allowReserved, size_t /*preferredSegment*/, size_t /*attempt*/) override
    {
        const auto total = header_->nNode - (allowReserved ? 0 : base_.reservedNumber);
        for (size_t i = 0; i < 2 * total; ++i) {
            auto cur = header_->nodeCursor.fetch_add(1, std::memory_order_relaxed) % total;
            uint8_t expected = 1;
            if (accessed_[cur].compare_exchange_strong(expected, 0, std::memory_order_relaxed,
                                                       std::memory_order_relaxed)) {
                continue;
            }
            return cur;
        }
        return header_->nodeCursor.fetch_add(1, std::memory_order_relaxed) % total;
    }
    void MarkAccessed(size_t iNode) override
    { accessed_[iNode].store(1, std::memory_order_relaxed); }
    void* DataAt(size_t iNode) override { return data_ + nodeSize_ * iNode; }
    void* DeviceDataAt(size_t iNode) override { return dataOnDevice_ + nodeSize_ * iNode; }
    size_t SegmentAt(size_t /*iNode*/) const override { return 0; }
    BufferMetaNode* MetaAt(size_t iNode) override { return meta_ + iNode; }
};

class RankStripedSharedBufferStrategy : public SharedBufferStrategy {
    size_t timeoutMs_{0};
    size_t segmentSize_{0};
    std::vector<std::string> dataShmNames_{};
    std::vector<std::byte*> dataBases_{};
    std::vector<std::byte*> dataOnDeviceBases_{};
    std::vector<uint8_t> registered_{};
    std::vector<uint8_t> owned_{};
    std::vector<size_t> numaNodes_;
    struct NumaMetadata {
        size_t shardSize;
        size_t nodeCount;
    };
    NumaMetadata* NumaInfo() const
    {
        return reinterpret_cast<NumaMetadata*>(static_cast<std::byte*>(addrress_) +
                                               SharedBufferStrategy::DataOffset());
    }
    size_t DataOffset() const noexcept override
    {
        const auto page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
        const auto end = SharedBufferStrategy::DataOffset() + sizeof(NumaMetadata) +
                         sizeof(size_t) * numaNodes_.size();
        return (end + page - 1) / page * page;
    }
    void InitLayoutMetadata() override
    {
        auto* info = NumaInfo();
        info->shardSize = nodeSize_;
        info->nodeCount = numaNodes_.size();
        std::copy(numaNodes_.begin(), numaNodes_.end(), reinterpret_cast<size_t*>(info + 1));
    }
    Status CheckLayoutMetadata() const override
    {
        auto* info = NumaInfo();
        if (info->shardSize != nodeSize_ || info->nodeCount != numaNodes_.size()) {
            return Status::InvalidParam(
                "rank-striped SHM({}) shard size or NUMA node count mismatch", shmName_);
        }
        if (!std::equal(numaNodes_.begin(), numaNodes_.end(),
                        reinterpret_cast<size_t*>(info + 1))) {
            return Status::InvalidParam("rank-striped SHM({}) NUMA node list/order mismatch",
                                        shmName_);
        }
        return Status::OK();
    }

    Status WaitSegmentReady(size_t segment) const
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs_);
        for (;;) {
            const auto ready = segmentReady_[segment].load(std::memory_order_acquire);
            if (ready == 1) { return Status::OK(); }
            if (ready == 2) {
                return Status::Error(
                    fmt::format("rank-striped segment({}) initialization failed", segment));
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return Status::Error(
                    fmt::format("rank-striped shared buffer segment({}) not ready", segment));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    Status WaitAllSegmentsReady() const
    {
        for (size_t segment = 0; segment < segmentCount_; ++segment) {
            auto s = WaitSegmentReady(segment);
            if (s.Failure()) { return s; }
        }
        return Status::OK();
    }

    Status MapOwnedSegment(size_t segment)
    {
        PosixShm shmFile{dataShmNames_[segment]};
        const auto flags =
            PosixShm::OpenFlag::CREATE | PosixShm::OpenFlag::EXCL | PosixShm::OpenFlag::READ_WRITE;
        auto s = shmFile.ShmOpen(flags);
        if (s.Success()) {
            owned_[segment] = 1;
            void* addr = nullptr;
            s = MmapShmFile(shmFile, segmentSize_, addr, true, false);
            if (s.Failure()) {
                segmentReady_[segment].store(2, std::memory_order_release);
                return s;
            }
            dataBases_[segment] = static_cast<std::byte*>(addr);
            s = ShmNuma::Initialize(dataBases_[segment], segmentSize_,
                                    ShmNuma::SegmentNodes(numaNodes_, segmentCount_, segment),
                                    dataShmNames_[segment]);
            if (s.Failure()) {
                segmentReady_[segment].store(2, std::memory_order_release);
                return s;
            }
            segmentReady_[segment].store(1, std::memory_order_release);
            UC_INFO("Created rank-striped shared buffer segment({}) file({}) size({}).", segment,
                    dataShmNames_[segment], segmentSize_);
            return Status::OK();
        }
        if (s != Status::DuplicateKey()) {
            UC_ERROR("Failed({}) to create rank-striped file({}).", s, dataShmNames_[segment]);
            return s;
        }
        s = WaitSegmentReady(segment);
        if (s.Failure()) { return s; }
        s = shmFile.ShmOpen(PosixShm::OpenFlag::READ_WRITE);
        if (s.Failure()) { return s; }
        void* addr = nullptr;
        s = MmapShmFile(shmFile, segmentSize_, addr, false, false);
        if (s.Success()) { dataBases_[segment] = static_cast<std::byte*>(addr); }
        return s;
    }

    Status MapPeerSegment(size_t segment)
    {
        PosixShm shmFile{dataShmNames_[segment]};
        auto s = shmFile.ShmOpen(PosixShm::OpenFlag::READ_WRITE);
        if (s.Failure()) {
            UC_ERROR("Failed({}) to open rank-striped file({}).", s, dataShmNames_[segment]);
            return s;
        }
        void* addr = nullptr;
        s = MmapShmFile(shmFile, segmentSize_, addr, false, false);
        if (s.Success()) { dataBases_[segment] = static_cast<std::byte*>(addr); }
        return s;
    }

    Status RegisterSegments(int32_t deviceId)
    {
        for (size_t segment = 0; segment < segmentCount_; ++segment) {
            void* deviceData = nullptr;
            auto s =
                Trans::Buffer::RegisterHostBuffer(dataBases_[segment], segmentSize_, &deviceData);
            if (s.Failure()) {
                UC_ERROR("Failed({}) to register rank-striped segment({}) to device({}).", s,
                         segment, deviceId);
                return s;
            }
            dataOnDeviceBases_[segment] = static_cast<std::byte*>(deviceData);
            registered_[segment] = 1;
            UC_DEBUG("Registered rank-striped segment({}) host({}) device({}) on device({}).",
                     segment, fmt::ptr(dataBases_[segment]), fmt::ptr(deviceData), deviceId);
        }
        return Status::OK();
    }

public:
    RankStripedSharedBufferStrategy(const std::string& uuid, int32_t deviceId, size_t nodeSize,
                                    size_t totalSize, size_t reservedNumber, size_t localRankSize,
                                    size_t timeoutMs, const std::vector<size_t>& numaNodes)
        : SharedBufferStrategy(uuid, deviceId, nodeSize, totalSize, reservedNumber),
          timeoutMs_(timeoutMs),
          numaNodes_(numaNodes)
    {
        segmentCount_ = localRankSize;
        layout_ = SharedLayout::RANK_STRIPED;
        setupTimeoutMs_ = timeoutMs;
        unlinkShm_ = false;
    }
    ~RankStripedSharedBufferStrategy() override
    {
        for (size_t segment = 0; segment < dataBases_.size(); ++segment) {
            if (registered_[segment]) { Trans::Buffer::UnregisterHostBuffer(dataBases_[segment]); }
            if (dataBases_[segment]) { PosixShm::MUnmap(dataBases_[segment], segmentSize_); }
        }
        for (size_t segment = 0; segment < owned_.size(); ++segment) {
            if (owned_[segment]) { PosixShm{dataShmNames_[segment]}.ShmUnlink(); }
        }
    }
    Status Setup() override
    {
        const auto deviceId = base_.deviceId;
        if (base_.nodeSize == 0 || segmentCount_ == 0 || deviceId < 0 ||
            static_cast<size_t>(deviceId) >= segmentCount_) {
            return Status::InvalidParam("invalid rank-striped shared buffer layout");
        }
        ShmNuma::ValidateNodes(numaNodes_);
        ShmNuma::SegmentNodes(numaNodes_, segmentCount_, 0);
        UC_INFO_UNLIMITED("Rank-striped SHM NUMA nodes: {}.", numaNodes_);
        const auto nodeCount = base_.totalSize / base_.nodeSize;
        if (base_.reservedNumber % segmentCount_ != 0 ||
            nodeCount / segmentCount_ <= base_.reservedNumber / segmentCount_) {
            return Status::InvalidParam(
                "rank-striped nodes({}) or reserved nodes({}) cannot be split across {} ranks",
                nodeCount, base_.reservedNumber, segmentCount_);
        }
        Trans::Device device;
        auto s = device.Setup(deviceId);
        if (s.Failure()) {
            UC_ERROR("Failed({}) to setup device({}) for rank-striped shared buffer.", s, deviceId);
            return s;
        }

        nodeSize_ = base_.nodeSize;
        nodesPerSegment_ = nodeCount / segmentCount_;
        nNode_ = nodesPerSegment_ * segmentCount_;
        segmentSize_ = nodeSize_ * nodesPerSegment_;
        UC_INFO(
            "Setting up rank-striped shared buffer: rank={}, segments={}, nodesPerSegment={}, "
            "segmentSize={}.",
            deviceId, segmentCount_, nodesPerSegment_, segmentSize_);
        const auto unusedBytes = base_.totalSize - segmentSize_ * segmentCount_;
        if (unusedBytes > 0) {
            UC_INFO("Rank-striped shared buffer leaves {} tail bytes unused for equal segments.",
                    unusedBytes);
        }
        shmName_ = ShmPrefix() + uuid_ + "_rs_meta";
        dataShmNames_.reserve(segmentCount_);
        for (size_t segment = 0; segment < segmentCount_; ++segment) {
            dataShmNames_.push_back(ShmPrefix() + uuid_ + "_rs_data_" + std::to_string(segment));
        }
        dataBases_.assign(segmentCount_, nullptr);
        dataOnDeviceBases_.assign(segmentCount_, nullptr);
        registered_.assign(segmentCount_, 0);
        owned_.assign(segmentCount_, 0);
        totalSize_ = DataOffset();

        PosixShm metaFile{shmName_};
        const auto flags =
            PosixShm::OpenFlag::CREATE | PosixShm::OpenFlag::EXCL | PosixShm::OpenFlag::READ_WRITE;
        s = metaFile.ShmOpen(flags);
        if (s.Success()) {
            unlinkShm_ = true;
            for (const auto& name : dataShmNames_) { PosixShm{name}.ShmUnlink(); }
            CleanUpShmFileExceptMe(shmName_);
            s = InitShmBuffer(metaFile);
        } else if (s == Status::DuplicateKey()) {
            s = LoadShmBuffer(metaFile);
        } else {
            UC_ERROR("Failed({}) to open rank-striped meta file({}).", s, shmName_);
            return s;
        }
        if (s.Failure()) { return s; }
        if (header_->layout != SharedLayout::RANK_STRIPED) {
            return Status::InvalidParam("shared buffer({}) is not rank-striped", shmName_);
        }

        const auto localRank = static_cast<size_t>(deviceId);
        s = MapOwnedSegment(localRank);
        if (s.Failure()) { return s; }
        s = WaitAllSegmentsReady();
        if (s.Failure()) { return s; }
        for (size_t segment = 0; segment < segmentCount_; ++segment) {
            if (segment == localRank) { continue; }
            s = MapPeerSegment(segment);
            if (s.Failure()) { return s; }
        }
        return RegisterSegments(deviceId);
    }
    size_t FetchNode(bool allowReserved, size_t preferredSegment, size_t attempt) override
    {
        const auto reservedPerSegment = base_.reservedNumber / segmentCount_;
        const auto total = nodesPerSegment_ - (allowReserved ? 0 : reservedPerSegment);
        size_t segment = 0;
        if (preferredSegment < segmentCount_) {
            // Exhaust candidates in the preferred segment before falling back to the next one.
            // A single busy node must not make an otherwise non-full preferred segment spill.
            segment = (preferredSegment + attempt / total) % segmentCount_;
        } else {
            segment = header_->nodeCursor.fetch_add(1, std::memory_order_relaxed) % segmentCount_;
        }
        for (size_t i = 0; i < 2 * total; ++i) {
            const auto local =
                segmentCursors_[segment].fetch_add(1, std::memory_order_relaxed) % total;
            const auto cur = segment * nodesPerSegment_ + local;
            uint8_t expected = 1;
            if (accessed_[cur].compare_exchange_strong(expected, 0, std::memory_order_relaxed,
                                                       std::memory_order_relaxed)) {
                continue;
            }
            return cur;
        }
        const auto local = segmentCursors_[segment].fetch_add(1, std::memory_order_relaxed) % total;
        return segment * nodesPerSegment_ + local;
    }
    void* DataAt(size_t iNode) override
    {
        const auto segment = SegmentAt(iNode);
        const auto local = iNode % nodesPerSegment_;
        return dataBases_[segment] + nodeSize_ * local;
    }
    void* DeviceDataAt(size_t iNode) override
    {
        const auto segment = SegmentAt(iNode);
        const auto local = iNode % nodesPerSegment_;
        return dataOnDeviceBases_[segment] + nodeSize_ * local;
    }
    size_t SegmentAt(size_t iNode) const override { return iNode / nodesPerSegment_; }
};

class SharedBufferWatcherStrategy : public SharedBufferStrategy {
public:
    SharedBufferWatcherStrategy(const std::string& uuid, bool rankStriped, size_t timeoutMs)
        : SharedBufferStrategy(uuid, -1, 0, 0, 0)
    {
        layout_ = rankStriped ? SharedLayout::RANK_STRIPED : SharedLayout::SINGLE;
        if (rankStriped) { unlinkShm_ = false; }
        setupTimeoutMs_ = timeoutMs;
    }
    Status Setup() override
    {
        shmName_ = ShmPrefix() + uuid_;
        if (layout_ == SharedLayout::RANK_STRIPED) { shmName_ += "_rs_meta"; }
        // In the rank-striped layout, the sibling data files are live payload segments and must
        // not be removed by the legacy single-file cleanup scan.
        if (layout_ == SharedLayout::SINGLE) { CleanUpShmFileExceptMe(shmName_); }
        PosixShm shmFile{shmName_};
        auto s = shmFile.ShmOpen(PosixShm::OpenFlag::READ_WRITE);
        if (s.Failure()) {
            UC_ERROR("Failed({}) to open file({}).", s, shmFile.ShmName());
            return s;
        }
        void* addr = nullptr;
        auto size = sizeof(BufferHeader);
        if (layout_ == SharedLayout::RANK_STRIPED) {
            s = WaitShmSize(shmFile, size);
            if (s.Failure()) { return s; }
        }
        s = MmapShmFile(shmFile, size, addr, false);
        if (s.Failure()) [[unlikely]] { return s; }
        auto header = static_cast<BufferHeader*>(addr);
        s = WaitShmHeaderReady(header);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Shm file({}) not ready.", shmFile.ShmName());
            return s;
        }
        if (header->layout != layout_) {
            shmFile.MUnmap(addr, size);
            return Status::InvalidParam("shared buffer({}) layout mismatch", shmName_);
        }
        nNode_ = header->nNode;
        segmentCount_ = header->segmentCount;
        nodesPerSegment_ = header->nodesPerSegment;
        shmFile.MUnmap(addr, size);
        totalSize_ = DataOffset();
        s = MmapShmFile(shmFile, totalSize_, addrress_, false, layout_ == SharedLayout::SINGLE);
        if (s.Failure()) [[unlikely]] { return s; }
        header_ = static_cast<BufferHeader*>(addrress_);
        InitPointers();
        return Status::OK();
    }
    void* DataAt(size_t iNode) override { return nullptr; }
    void* DeviceDataAt(size_t iNode) override { return nullptr; }
    void MarkAccessed(size_t /*iNode*/) override {}
};

Status TransBuffer::Setup(const Config& config)
{
    bypassHitOnLoad_ = config.cacheLoadBackendOnly;
    try {
        if (!config.shareBufferEnable) {
            strategy_ = std::make_shared<LocalBufferStrategy>(
                config.deviceId, config.shardSize, config.bufferCapacity,
                config.loadExclusiveBufferNumber, config.ioDirect, config.cacheSdmaDirect);
        } else if (config.deviceId >= 0) {
            if (config.shareBufferRankStriped) {
                const auto numaNodes = config.shareBufferNumaNodes.empty()
                                           ? ShmNuma::DefaultNodes()
                                           : config.shareBufferNumaNodes;
                strategy_ = std::make_shared<RankStripedSharedBufferStrategy>(
                    config.uniqueId, config.deviceId, config.shardSize, config.bufferCapacity,
                    config.loadExclusiveBufferNumber, config.localRankSize, config.timeoutMs,
                    numaNodes);
            } else {
                strategy_ = std::make_shared<SharedBufferStrategy>(
                    config.uniqueId, config.deviceId, config.shardSize, config.bufferCapacity,
                    config.loadExclusiveBufferNumber);
            }
        } else {
            strategy_ = std::make_shared<SharedBufferWatcherStrategy>(
                config.uniqueId, config.shareBufferRankStriped, config.timeoutMs);
        }
        return strategy_->Setup();
    } catch (const std::exception& e) {
        return Status::Error(fmt::format("failed({}) to setup buffer strategy", e.what()));
    }
}

TransBuffer::Handle TransBuffer::Get(const Detail::BlockId& blockId, size_t shardIdx,
                                     bool allowReserved, bool isLoad, size_t preferredSegment)
{
    auto iBucket = Hash(blockId, shardIdx);
    bool owner = false;
    strategy_->BucketLock(iBucket);
    auto iNode = FindAt(iBucket, blockId, shardIdx, owner);
    if (iNode != invalidIndex) {
        if (bypassHitOnLoad_ && isLoad && owner && Ready(iNode)) { MarkNotReady(iNode); }
        strategy_->BucketUnlock(iBucket);
        return Handle{this, iNode, owner};
    }
    iNode = Alloc(blockId, shardIdx, iBucket, allowReserved, preferredSegment);
    strategy_->BucketUnlock(iBucket);
    return Handle(this, iNode, true);
}

void TransBuffer::Prealloc(const Detail::BlockId& blockId, size_t shardIdx, bool allowReserved,
                           size_t preferredSegment)
{
    auto iBucket = Hash(blockId, shardIdx);
    strategy_->BucketLock(iBucket);
    if (!ExistAt(iBucket, blockId, shardIdx)) {
        size_t pos = Alloc(blockId, shardIdx, iBucket, allowReserved, preferredSegment);
        Release(pos);
    }
    strategy_->BucketUnlock(iBucket);
}

bool TransBuffer::Exist(const Detail::BlockId& blockId, size_t shardIdx)
{
    auto iBucket = Hash(blockId, shardIdx);
    strategy_->BucketLock(iBucket);
    auto exist = ExistAt(iBucket, blockId, shardIdx);
    strategy_->BucketUnlock(iBucket);
    return exist;
}

bool TransBuffer::ExistAt(size_t iBucket, const Detail::BlockId& blockId, size_t shardIdx)
{
    auto iNode = strategy_->FirstAt(iBucket);
    while (iNode != invalidIndex) {
        auto meta = strategy_->MetaAt(iNode);
        if (meta->block == blockId && meta->shard == shardIdx) { return true; }
        iNode = meta->next;
    }
    return false;
}

size_t TransBuffer::FindAt(size_t iBucket, const Detail::BlockId& blockId, size_t shardIdx,
                           bool& owner)
{
    auto iNode = strategy_->FirstAt(iBucket);
    while (iNode != invalidIndex) {
        auto meta = strategy_->MetaAt(iNode);
        if (meta->block == blockId && meta->shard == shardIdx) {
            strategy_->NodeLock(iNode);
            owner = meta->reference == 0;
            if (owner && meta->state.load(std::memory_order_relaxed) == State::FAILED) {
                meta->state.store(State::LOADING, std::memory_order_relaxed);
                meta->errorCode.store(Status::OK().Underlying(), std::memory_order_relaxed);
            }
            ++meta->reference;
            strategy_->MarkAccessed(iNode);
            strategy_->NodeUnlock(iNode);
            break;
        }
        iNode = meta->next;
    }
    return iNode;
}

size_t TransBuffer::Alloc(const Detail::BlockId& blockId, size_t shardIdx, size_t iBucket,
                          bool allowReserved, size_t preferredSegment)
{
    size_t attempt = 0;
    for (;;) {
        auto iNode = strategy_->FetchNode(allowReserved, preferredSegment, attempt++);
        auto meta = strategy_->MetaAt(iNode);
        strategy_->NodeLock(iNode);
        if (meta->reference > 0) {
            strategy_->NodeUnlock(iNode);
            continue;
        }
        const auto oldBucket = meta->hash;
        if (oldBucket != iBucket) {
            if (oldBucket != invalidIndex) {
                if (!strategy_->BucketTryLock(oldBucket)) {
                    strategy_->NodeUnlock(iNode);
                    continue;
                }
                Remove(oldBucket, iNode);
                strategy_->BucketUnlock(oldBucket);
            }
            MoveTo(iBucket, iNode);
        }
        ++meta->reference;
        strategy_->MarkAccessed(iNode);
        meta->block = blockId;
        meta->shard = shardIdx;
        meta->state.store(State::LOADING, std::memory_order_relaxed);
        meta->errorCode.store(Status::OK().Underlying(), std::memory_order_relaxed);
        strategy_->NodeUnlock(iNode);
        return iNode;
    }
}

void TransBuffer::MoveTo(size_t iBucket, size_t iNode)
{
    auto meta = strategy_->MetaAt(iNode);
    auto& head = strategy_->FirstAt(iBucket);
    auto n = head;
    meta->next = n;
    if (n != invalidIndex) {
        auto next = strategy_->MetaAt(n);
        strategy_->NodeLock(n);
        next->prev = iNode;
        strategy_->NodeUnlock(n);
    }
    meta->hash = iBucket;
    head = iNode;
}

void TransBuffer::Remove(size_t iBucket, size_t iNode)
{
    auto meta = strategy_->MetaAt(iNode);
    auto p = meta->prev;
    if (p != invalidIndex) {
        auto prev = strategy_->MetaAt(p);
        strategy_->NodeLock(p);
        prev->next = meta->next;
        strategy_->NodeUnlock(p);
    }
    auto n = meta->next;
    if (n != invalidIndex) {
        auto next = strategy_->MetaAt(n);
        strategy_->NodeLock(n);
        next->prev = meta->prev;
        strategy_->NodeUnlock(n);
    }
    if (strategy_->FirstAt(iBucket) == iNode) { strategy_->FirstAt(iBucket) = n; }
    meta->prev = meta->next = invalidIndex;
    meta->hash = invalidIndex;
}

void* TransBuffer::DataAt(Index pos) { return strategy_->DataAt(pos); }

void* TransBuffer::DeviceDataAt(Index pos) { return strategy_->DeviceDataAt(pos); }

size_t TransBuffer::SegmentAt(Index pos) const { return strategy_->SegmentAt(pos); }

void TransBuffer::Acquire(Index pos)
{
    strategy_->NodeLock(pos);
    ++strategy_->MetaAt(pos)->reference;
    strategy_->NodeUnlock(pos);
}

void TransBuffer::Release(Index pos)
{
    strategy_->NodeLock(pos);
    --strategy_->MetaAt(pos)->reference;
    strategy_->NodeUnlock(pos);
}

bool TransBuffer::Ready(Index pos) { return GetState(pos) == State::READY; }

TransBuffer::State TransBuffer::GetState(Index pos)
{ return strategy_->MetaAt(pos)->state.load(std::memory_order_acquire); }

Status TransBuffer::FailureStatus(Index pos)
{
    auto errorCode = strategy_->MetaAt(pos)->errorCode.load(std::memory_order_acquire);
    if (errorCode == Status::OK().Underlying()) {
        return Status::Error("shared buffer failed without an error status");
    }
    return Status{errorCode, {}};
}

void TransBuffer::MarkReady(Index pos)
{ strategy_->MetaAt(pos)->state.store(State::READY, std::memory_order_release); }

void TransBuffer::MarkFailed(Index pos, const Status& status)
{
    auto meta = strategy_->MetaAt(pos);
    meta->errorCode.store(status.Underlying(), std::memory_order_release);
    meta->state.store(State::FAILED, std::memory_order_release);
}

void TransBuffer::MarkNotReady(Index pos)
{
    auto meta = strategy_->MetaAt(pos);
    meta->errorCode.store(Status::OK().Underlying(), std::memory_order_release);
    meta->state.store(State::LOADING, std::memory_order_release);
}

}  // namespace UC::CacheStore
