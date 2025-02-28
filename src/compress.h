//
// Created by linuxlite on 2/7/25.
//

#ifndef COMPRESS_H
#define COMPRESS_H
#include <iostream>

#include "sastam.h"
#include <mutex>
#include <ostream>
#include <statistics.h>
#include <stdexcept>
#include <thread>
#include <valkeymodule.h>
#include <vector>
#include <zstd.h>
#include <zdict.h>

#include "zstd.h"
#include "zstd.h"

enum
{
    page_size = 4096,
    reserved_address_base = 10000000,
    enable_compression = 1,
    auto_vac = 0,
    auto_vac_workers = 8,
    auto_vac_heap_threshold = 60,
};

struct compressed_address
{
    compressed_address() = default;
    compressed_address(const compressed_address&) = default;
    compressed_address& operator=(const compressed_address&) = default;

    explicit compressed_address(size_t index) : index(index)
    {
    }

    compressed_address(size_t p, size_t o)
    {
        from_page_offset(p, o);
    }

    compressed_address& operator =(nullptr_t)
    {
        index = 0;
        return *this;
    }

    [[nodiscard]] bool null() const
    {
        return index == 0;
    }

    bool operator==(const compressed_address& other) const
    {
        return index == other.index;
    }

    bool operator!=(const compressed_address& other) const
    {
        return index != other.index;
    }

    bool operator<(const compressed_address& other) const
    {
        return index < other.index;
    }

    [[nodiscard]] bool is_null_base() const
    {
        return (page() % reserved_address_base) == 0;
    }

    void clear()
    {
        index = 0;
    }

    void from_page_index(size_t p)
    {
        index = p * page_size;
    }

    void from_page_offset(size_t p, size_t offset)
    {
        index = p * page_size + offset;
    }

    [[nodiscard]] size_t offset() const
    {
        return index % page_size;
    }

    [[nodiscard]] size_t page() const
    {
        return index / page_size;
    }

    [[nodiscard]] size_t address() const
    {
        return index;
    }

    void from_address(size_t a)
    {
        index = a;
    }

    bool operator==(uint32_t other) const
    {
        return index == other;
    }

private:
    uint32_t index = 0;
};

struct training_entry
{
    training_entry(const uint8_t* data, size_t size) : data(data), size(size)
    {
    }

    const uint8_t* data;
    size_t size;
};

struct storage
{
    storage() : compressed(0), decompressed(0)
    {
    }

    storage(storage&& other) noexcept : compressed(0), decompressed(0)
    {
        *this = std::move(other);
    }

    storage(const storage& other) : compressed(0), decompressed(0)
    {
        *this = other;
    }

    storage& operator=(const storage& other)
    {
        if (&other == this) return *this;
        compressed = other.compressed;
        decompressed = other.decompressed;
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        return *this;
    }

    void clear()
    {
        compressed.release();
        decompressed.release();
        write_position = 0;
        modifications = 0;
        size = 0;
    }

    storage& operator=(storage&& other) noexcept
    {
        if (&other == this) return *this;
        compressed = std::move(other.compressed);
        decompressed = std::move(other.decompressed);
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        other.clear();
        return *this;
    }

    [[nodiscard]] bool empty() const
    {
        return size == 0 && write_position == 0 && compressed.empty() && decompressed.empty();
    }

    heap::buffer<uint8_t> compressed;
    heap::buffer<uint8_t> decompressed;
    uint16_t write_position = 0;
    uint16_t size = 0;
    uint16_t modifications = 0;
};

struct compress
{
    enum
    {
        min_training_size = 1024 * 120,
        compression_level = 1
    };

    compress() = default;
    compress(const compress&) = delete;

    ~compress()
    {
        tvac_exit = true;

        ZSTD_freeCCtx(cctx);
    }

private:
    std::thread tdict{};
    bool tvac_exit = false;
    unsigned size_in_training = 0;
    unsigned size_to_train = 0;
    heap::buffer<uint8_t> training_data{0};
    std::vector<training_entry, heap::allocator<training_entry>> trainables{};
    std::vector<training_entry, heap::allocator<training_entry>> intraining{};
    ZSTD_CDict* dict{nullptr};
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    size_t trained_size = 0;
    mutable std::mutex mutex{};
    /// TODO: NB! the arena may reallocate while compression worker threads are running and cause instability
    heap::vector<storage> arena{};
    heap::vector<size_t> free_pages{};
    size_t last_page_allocated{0};
    compressed_address arena_head{0};
    compressed_address highest_reserve_address{0};
    uint64_t last_heap_bytes = 0;
    uint64_t release_counter = 0;
    std::chrono::time_point<std::chrono::system_clock> last_vacuum_millis = std::chrono::high_resolution_clock::now();;
    compress& operator=(const compress& t)
    {
        if (this == &t) return *this;
        return *this;
    };

    void train()
    {
        if (size_in_training < min_training_size)
        {
            return;
        }
        unsigned sampleCount = intraining.size();
        auto samplesBuffer = heap::buffer<uint8_t>(size_in_training);
        training_data = heap::buffer<uint8_t>(size_in_training);
        auto sampleSizes = heap::buffer<size_t>(sampleCount);

        auto samplesBufferPtr = samplesBuffer.begin();
        auto sampleSizesPtr = sampleSizes.begin();
        for (auto [data,size] : intraining)
        {
            *sampleSizesPtr++ = size;
            memcpy(samplesBufferPtr, data, size);
            heap::free(const_cast<uint8_t*>(data), size);
            samplesBufferPtr += size;
        }
        if (sampleSizesPtr - sampleSizes.begin() != sampleCount)
        {
            throw std::runtime_error("training size mismatch");
        }
        if (samplesBufferPtr - samplesBuffer.begin() != size_in_training)
        {
            throw std::runtime_error("training size mismatch");
        }
        ZSTD_CDict* can_haz = nullptr;
        size_t trainedSize = ZDICT_trainFromBuffer(training_data.begin(),
                                                   size_in_training, samplesBuffer.begin(),
                                                   sampleSizes.cbegin(), sampleCount);
        if (ZDICT_isError(trainedSize))
        {
            auto zde = ZDICT_getErrorName(trainedSize);
            std::cerr << zde << std::endl;
            // some issue - need to start again
        }
        else
        {
            trained_size = trainedSize;
            can_haz = ZSTD_createCDict(training_data.begin(), trainedSize, compression_level);
        }


        size_in_training = 0;
        intraining.clear();
        if (can_haz)
        {
            dict = can_haz;

        }
    }

    bool add_training_entry(const uint8_t* data, size_t size)
    {
        if (dict || size_in_training != 0 || size_to_train > min_training_size)
        {
            return false;
        }
        heap::buffer<uint8_t> sample(size);
        sample.emplace(size, data);
        training_entry t = {sample.move(), size};
        trainables.push_back(t);
        size_to_train += size;

        if (size_to_train > min_training_size)
        {
            if (!intraining.empty() || size_in_training != 0)
                abort();
            intraining.swap(trainables);
            size_in_training = size_to_train;
            size_to_train = 0;
            if (tdict.joinable())
                tdict.join();
            tdict = std::thread(&compress::train, this);
            //train();
        }
        return true;
    }


    static heap::buffer<uint8_t> compress_2_buffer(ZSTD_CDict* localDict, ZSTD_CCtx* cctx, const uint8_t* data,
                                                   size_t size)
    {
        if (!localDict) return heap::buffer<uint8_t>(0);
        size_t compressed_max_size = ZSTD_compressBound(size);
        auto compressed_data_temp = heap::buffer<uint8_t>(compressed_max_size);
        size_t compressed = ZSTD_compress_usingCDict(cctx, compressed_data_temp.begin(),
                                                     compressed_max_size, data, size, localDict);
        if (ZDICT_isError(compressed))
        {
            abort();
        }
        auto compressed_data = heap::buffer<uint8_t>(compressed);
        compressed_data.emplace(compressed, compressed_data_temp.begin());
        return compressed_data;
    }

    heap::buffer<uint8_t> decompress_buffer(size_t known_decompressed_size, const heap::buffer<uint8_t>& compressed)
    {
        heap::buffer<uint8_t> decompressed = heap::buffer<uint8_t>(known_decompressed_size);
        size_t decompressed_size = ZSTD_decompress_usingDict(dctx,
                                                             decompressed.begin(), decompressed.size(),
                                                             compressed.begin(), compressed.size(),
                                                             training_data.begin(), trained_size);
        if (ZDICT_isError(decompressed_size))
        {
            auto msg = ZDICT_getErrorName(decompressed_size);
            std::cerr << msg << std::endl;
            abort();
        }
        return decompressed;
    }
    ;

    [[nodiscard]] bool decompress(storage& todo)
    {
        if (todo.decompressed.empty() && !todo.compressed.empty())
        {
            todo.decompressed = decompress_buffer(page_size, todo.compressed);
            statistics::page_bytes_uncompressed += todo.decompressed.byte_size();
            ++statistics::pages_uncompressed;

            if (todo.decompressed.size() != page_size)
            {
                abort();
            }
            return true;
        }
        return false;
    }

    [[nodiscard]] static bool is_null_base(const compressed_address& at)
    {
        return is_null_base(at.page());
    }

    [[nodiscard]] static bool is_null_base(size_t at)
    {
        return (at % reserved_address_base) == 0;
    }

    [[nodiscard]] size_t last_block() const
    {
        if (arena.empty()) return 0;
        return arena.size() - 1;
    }

    std::pair<size_t, storage&> create_if_required(size_t size)
    {
        if (is_null_base(last_block()))
        {
            arena.emplace_back();
            arena.back().write_position = page_size;
        }
        if (size > page_size)
        {
            arena.emplace_back();
            arena.back().decompressed = std::move(heap::buffer<uint8_t>(size));
            statistics::page_bytes_uncompressed += arena.back().decompressed.byte_size();
            ++statistics::pages_uncompressed;

            return {arena.size() - 1, arena.back()};
        }

        auto& last = arena.at(last_page_allocated);

        if (last.write_position + size >= page_size)
        {
            if (!dict)
            {
                add_training_entry(last.decompressed.begin(), last.write_position);
            }
            else if (!last.decompressed.empty() && auto_vac != 0)
            {
                // if (last.modifications > 0)
                   // to schedule a buffer release/compress
            }
            if (!free_pages.empty())
            {
                size_t fp = free_pages.back();
                free_pages.pop_back();
                auto& p = arena.at(fp);
                if (p.empty())
                {
                    p.decompressed = std::move(heap::buffer<uint8_t>(page_size));
                    statistics::page_bytes_uncompressed += p.decompressed.byte_size();
                    ++statistics::pages_uncompressed;

                    memset(p.decompressed.begin(), 0, page_size);
                    return {fp, p};
                }
            }
            arena.emplace_back();
            arena.back().decompressed = std::move(heap::buffer<uint8_t>(page_size));
            statistics::page_bytes_uncompressed += arena.back().decompressed.byte_size();
            ++statistics::pages_uncompressed;
            memset(arena.back().decompressed.begin(), 0, page_size);
            return {arena.size() - 1, arena.back()};
        }
        if (last.empty())
        {
            last.decompressed = std::move(heap::buffer<uint8_t>(page_size));
            statistics::page_bytes_uncompressed += last.decompressed.byte_size();
            ++statistics::pages_uncompressed;
            memset(last.decompressed.begin(), 0, page_size);
            return {last_page_allocated, last};
        }
        return {last_page_allocated, last};
    }

    // compression can happen in any single thread - even a worker thread
    size_t release_decompressed(ZSTD_CCtx* cctx, size_t at)
    {
        if (is_null_base(at)) return 0;
        size_t r = 0;
        uint16_t mods = 0;
        heap::buffer<uint8_t> compressed;
        heap::buffer<uint8_t> decompressed;
        heap::buffer<uint8_t> torelease;


        auto& t = arena[at];

        if (t.modifications > 0)
        {   decompressed = t.decompressed;
            mods = t.modifications;
        }

        if (enable_compression != 0 && mods > 0 && !t.decompressed.empty())
            compressed = compress_2_buffer(dict, cctx, decompressed.begin(), page_size);

        if (!decompressed.empty() && (compressed.empty() || mods == t.modifications))
        {
            if(!t.compressed.empty())
            {
                statistics::page_bytes_compressed -= t.compressed.byte_size();
                --statistics::pages_compressed;
            }
            statistics::page_bytes_compressed += compressed.byte_size();
            ++statistics::pages_compressed;
            t.compressed = std::move(compressed);
            t.modifications = 0;
        }
        if (t.compressed.empty() && t.size > 0 && t.modifications == 0)
        {
            abort();
        }
        if (!t.decompressed.empty() && t.modifications == 0)
        {
            r = t.decompressed.byte_size();

            torelease = std::move(t.decompressed);
            statistics::page_bytes_uncompressed -= torelease.byte_size();
            --statistics::pages_uncompressed;
        }

        return r;
    }


    uint8_t* basic_resolve(compressed_address at, bool modify = false)
    {
        if (at.null()) return nullptr;
        invalid(at);
        auto p = at.page();
        auto& t = arena.at(p);
        if (t.decompressed.empty() && !t.compressed.empty())
        {
            if (decompress(t))
            {   // we might signal this as decompressed
                if(modify && !t.compressed.empty())
                {
                    statistics::page_bytes_compressed -= t.compressed.byte_size();
                    --statistics::pages_compressed;
                    t.compressed.release();
                }
            }
        }
        if (t.decompressed.empty())
        {
            abort();
        }
        if (modify)
        {
            t.modifications++;
        }

        return t.decompressed.begin() + at.offset();
    }

    void invalid(compressed_address at) const
    {
        if (!valid(at))
        {
            abort();
        }
    }

    [[nodiscard]] bool valid(compressed_address at) const
    {
        if (at == 0) return true;
        if (at.page() >= arena.size())
        {
            return false;
        }
        const auto& t = arena.at(at.page());
        return t.size > 0 && t.write_position > at.offset();
    }

    size_t inner_vacuum()
    {
        std::atomic<size_t> r = 0;
        std::thread workers[auto_vac_workers];
        for (unsigned ivac = 0; ivac < auto_vac_workers; ivac++)
        {
            workers[ivac] = std::thread([this,ivac,&r]()
            {
                ZSTD_CCtx* cctx = ZSTD_createCCtx();
                for (size_t t = 1; t < arena.size(); t++)
                {
                    if(t % auto_vac_workers == ivac)
                        r +=release_decompressed(cctx,t);
                }

                ZSTD_freeCCtx(cctx);
            });
        }
        for (auto& worker : workers) worker.join();
        ++statistics::vacuums_performed;
        return r;
    }

public:
    void free(compressed_address at, size_t size)
    {
        std::lock_guard guard(mutex);
        if (at.address() == 0 || size == 0)
        {
            abort();
        }
        auto& t = arena.at(at.page());
        if (t.size == 0)
        {
            abort();
        }
        if (t.size == 1)
        {
            statistics::page_bytes_uncompressed -= t.decompressed.byte_size();
            if(!t.compressed.empty()) --statistics::pages_compressed;
            if(!t.decompressed.empty())--statistics::pages_uncompressed;
            t.clear();

            free_pages.push_back(at.page());
        }
        else
        {
            t.size--;
            t.modifications++;
        }
    }


    template <typename T>
    T* resolve(compressed_address at)
    {
        if (at.null()) return nullptr;
        std::lock_guard guard(mutex);
        return (T*)basic_resolve(at);
    }

    template <typename T>
    T* resolve_modified(compressed_address at)
    {
        if (at.null()) return nullptr;
        std::lock_guard guard(mutex);
        return (T*)basic_resolve(at, true);
    }

    compressed_address new_address(size_t size)
    {
        std::lock_guard guard(mutex);
        auto at = create_if_required(size);
        if (at.second.decompressed.empty() && !at.second.compressed.empty())
        {
            if (decompress(at.second))
            {
                statistics::page_bytes_compressed -= at.second.compressed.byte_size();
                --statistics::pages_compressed;
                at.second.compressed.release();
                // we might signal this as decompressed
            };
        }
        if (at.second.write_position + size >= page_size || at.second.decompressed.empty())
        {
            abort();
        }
        last_page_allocated = at.first;
        compressed_address ca(at.first, at.second.write_position);
        at.second.write_position += size;
        at.second.size++;
        at.second.modifications++;
        invalid(ca);
        return ca;
    }

    void release_context()
    {
        if (statistics::page_bytes_compressed > heap::allocated)
        {
            abort();
        }
        if (statistics::node_bytes_alloc < statistics::interior_bytes_alloc)
        {
            abort();
        }
        std::lock_guard guard(mutex);
        if (dict == nullptr) return;

        statistics::max_page_bytes_uncompressed = std::max<uint64_t>(statistics::page_bytes_uncompressed, statistics::max_page_bytes_uncompressed);

        auto t = std::chrono::high_resolution_clock::now();
        auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - last_vacuum_millis);
        double ratio = heap::get_physical_memory_ratio();
        uint64_t total_heap = heap::allocated;
        if ( ratio > 0.85 && d.count() > 200 )
        {
            if (total_heap < statistics::page_bytes_compressed)
            {
                abort();
            }
            auto start_vac = std::chrono::high_resolution_clock::now();
            inner_vacuum();
            auto end_vac = std::chrono::high_resolution_clock::now();
            auto dvac = std::chrono::duration_cast<std::chrono::milliseconds>(end_vac - start_vac);
            statistics::last_vacuum_time = dvac.count();
            last_heap_bytes = statistics::page_bytes_uncompressed;
            last_vacuum_millis = std::chrono::high_resolution_clock::now();

          }
        if(!last_heap_bytes) last_heap_bytes = statistics::page_bytes_compressed;


    }
    void enter_context()
    {
    }
    size_t vacuum()
    {
        std::lock_guard guard(mutex); // this should reduce wait time to the minimum
        double ratio = heap::get_physical_memory_ratio();
        int r = inner_vacuum();
        double ratio_after = heap::get_physical_memory_ratio();
        if (ratio_after > ratio)
        {
            abort();
        }
        return r;
    }


};

#endif //COMPRESS_H
