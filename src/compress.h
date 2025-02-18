//
// Created by linuxlite on 2/7/25.
//

#ifndef COMPRESS_H
#define COMPRESS_H
#include <iostream>

#include "sastam.h"
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <thread>
#include <vector>
#include <zstd.h>
#include <zdict.h>
enum
{
    page_size = 1024,
    max_buffered_el_size = 256,
    reserved_address = 10000000
};
struct compressed_address
{
    compressed_address() =default;
    compressed_address(const compressed_address &) = default;
    compressed_address& operator=(const compressed_address &) = default;

    explicit compressed_address(size_t index) : index(index) {}
    compressed_address(size_t p, size_t o)
    {
        from_page_offset(p, o);
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
        return (page() % reserved_address) == 0;
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
        return index ;
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
    training_entry(const uint8_t* data, size_t size) : data(data), size(size) {}
    const uint8_t * data;
    size_t size;
};
struct storage
{
    storage() : compressed(0), decompressed(0)
    {

    }

    storage(storage&& other)  noexcept : compressed(0), decompressed(0)
    {
        *this = std::move(other);
    }
#if 0
#endif
    storage(const storage& other) : compressed(0), decompressed(0)
    {
        *this = other;
    }
    storage& operator=(const storage& other)
    {
        if(&other == this) return *this;
        compressed = other.compressed;
        decompressed = other.decompressed;
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        return *this;
    }
    void clear()
    {
        compressed.clear();
        decompressed.clear();
        write_position = 0;
        modifications = 0;
        size = 0;
    }

    storage& operator=(storage&& other) noexcept
    {
        if(&other == this) return *this;
        compressed = std::move(other.compressed);
        decompressed = std::move(other.decompressed);
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        other.clear();
        return *this;
    }
#if 0
#endif
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
        min_training_size = 1024*1024,
        compression_level = 1
    };
    compress()= default;
    compress(const compress&) = delete;
    std::thread tdict {};
    unsigned size_in_training = 0;
    unsigned size_to_train = 0;
    heap::buffer<uint8_t> training_data {0};
    std::vector<training_entry, heap::allocator<training_entry>> trainables {};
    std::vector<training_entry, heap::allocator<training_entry>> intraining {};
    ZSTD_CDict* dict {nullptr};
    ZSTD_CCtx*  cctx = ZSTD_createCCtx();
    ZSTD_DCtx*  dctx = ZSTD_createDCtx();
    size_t trained_size = 0;
    std::mutex mutex {};
    heap::vector<storage> arena{};
    heap::vector<compressed_address> decompressed {};
    compressed_address arena_head{0};
    compressed_address highest_reserve_address{ 0};
    compress& operator=(const compress& ) {
        return *this;
    };
    ~compress()
    {
        ZSTD_freeCCtx(cctx);
    }
    void train()
    {
        if(size_in_training < min_training_size)
        {
            return;
        }
        unsigned sampleCount = intraining.size();
        auto samplesBuffer = heap::buffer<uint8_t>(size_in_training);
        training_data = heap::buffer<uint8_t>(size_in_training);
        auto sampleSizes = heap::buffer<size_t>(sampleCount);

        auto samplesBufferPtr = samplesBuffer.begin();
        auto sampleSizesPtr = sampleSizes.begin();
        for(auto [data,size]: intraining)
        {
            *sampleSizesPtr++ = size;
            memcpy(samplesBufferPtr, data, size);
            heap::free(const_cast<uint8_t*>(data), size);
            samplesBufferPtr += size;
        }
        if(sampleSizesPtr - sampleSizes.begin() != sampleCount)
        {
            throw std::runtime_error("training size mismatch");
        }
        if(samplesBufferPtr - samplesBuffer.begin() != size_in_training)
        {
            throw std::runtime_error("training size mismatch");
        }
        ZSTD_CDict* can_haz = nullptr;
        size_t trainedSize = ZDICT_trainFromBuffer(training_data.begin(),
            size_in_training, samplesBuffer.begin(),
            sampleSizes.cbegin(), sampleCount);
        if (ZDICT_isError(trainedSize))
        {
            auto zde =ZDICT_getErrorName(trainedSize) ;
            std::cerr << zde << std::endl;
            // some issue - need to start again
        }else
        {   trained_size = trainedSize;
            can_haz = ZSTD_createCDict(training_data.begin(), trainedSize, compression_level);
        }

        //std::lock_guard guard(mutex);
        size_in_training = 0;
        intraining.clear();
        if(can_haz)
            dict = can_haz;

    }

    bool add_training_entry(const uint8_t* data, size_t size )
    {
        //std::lock_guard guard(mutex);
        if (dict || size_in_training != 0 ||size_to_train > min_training_size)
        {
              return false;
        }
        heap::buffer<uint8_t> sample(size);
        sample.emplace(size, data);
        training_entry t = {sample.move(),size};
        trainables.push_back(t);
        size_to_train += size;

        if(size_to_train > min_training_size)
        {
            if(!intraining.empty() || size_in_training != 0)
                abort();
            intraining.swap(trainables);
            size_in_training = size_to_train;
            size_to_train = 0;
            if(tdict.joinable())
                tdict.join();
            //tdict = std::thread(&compress::train, this);
            train();
        }
        return true;
    }


    void flush(compressed_address address)
    {
        if(address.page() >= arena.size()) return;

        auto &s = arena.at(address.page());

        if(s.modifications)
        {
            if (dict && cctx && trained_size)
            {
                s.compressed = compress_2_buffer(s.decompressed.begin(),s.write_position);

                s.modifications = 0;
            }else
            {
                s.modifications = 0;
            }

        }
        if(!s.compressed.empty() && !s.modifications)
            s.decompressed.release();
    }
    void flush()
    {
        flush(arena_head);
    }
    heap::buffer<uint8_t> compress_2_buffer(const uint8_t* data, size_t size) const
    {

        if(!dict) return heap::buffer<uint8_t>(0);
        size_t compressed_max_size = ZSTD_compressBound(size);
        auto compressed_data_temp = heap::buffer<uint8_t>(compressed_max_size);
        size_t compressed = ZSTD_compress_usingCDict(cctx, compressed_data_temp.begin(),
            compressed_max_size, data, size, dict);
        if (ZDICT_isError(compressed))
        {
            abort();
        }
        auto compressed_data = heap::buffer<uint8_t>(compressed);
        compressed_data.emplace( compressed, compressed_data_temp.begin());
        return compressed_data;
    }

    void decompress(compressed_address t)
    {
        if(arena.empty()) return;

        auto &s = arena.at(t.page());
        decompress(s);
        //if(!s.modifications)
            //decompressed.push_back(t);
    }
    heap::buffer<uint8_t> decompress_buffer(size_t known_decompressed_size,const heap::buffer<uint8_t>& compressed)
    {
        heap::buffer<uint8_t> decompressed = heap::buffer<uint8_t>(known_decompressed_size);
        size_t decompressed_size = ZSTD_decompress_usingDict(dctx,
            decompressed.begin(),decompressed.size(),
            compressed.begin(),compressed.size(),
            training_data.begin(),trained_size);
        if(ZDICT_isError(decompressed_size))
        {
            auto msg = ZDICT_getErrorName(decompressed_size);
            std::cerr << msg << std::endl;
            abort();
        }
        return decompressed;
    }
    void decompress(storage & todo)
    {
        if(!dict)
            abort();
        if(!dctx)
            abort();
        if (todo.empty()) {
            abort();
        }
        if (todo.compressed.empty())
        {
            abort();
        }
        if (!todo.write_position)
        {
            abort();
        }
        if (todo.modifications && todo.decompressed.empty())
        {
            abort();
        }
        if(!todo.decompressed.empty() && todo.decompressed.size() != todo.write_position)
        {
            abort();
        }
        if(!todo.decompressed.empty())
        {
            return;
        }
        todo.decompressed = decompress_buffer(todo.write_position,todo.compressed);
    }
    [[nodiscard]] static bool is_null_base(const compressed_address& at)
    {
        return is_null_base(at.page());
    }
    [[nodiscard]] static bool is_null_base(size_t at)
    {
        return (at % reserved_address) == 0;
    }
    void allocate_arena()
    {
        arena.emplace_back();

        if(is_null_base(arena.size()-1))
        {   highest_reserve_address.from_page_index(arena.size()-1);
            arena.emplace_back();// reserve and address
        }

        arena_head.from_page_index(arena.size()-1);

    }
    void release(const compressed_address& at)
    {
        if(is_null_base(at))
        {
            abort();
        }
        if(at.page() >= arena.size())
        {
            abort();
        }
        auto& t = arena.at(at.page());
        if(t.size > 0)
        {
            t.size--;// TODO: modifications ??
            if(t.size == 0)
            {
                t.compressed.release();
                t.decompressed.release();
                t.write_position = 0;
            }
        }
    }
    heap::vector<const uint8_t*> mapper{};
    const uint8_t* resolve(compressed_address at)
    {
        return mapper.at(at.address());
#if 0
        if (at.null())
        {
            return nullptr;
        }
        if (is_null_base(at))
        {
            return nullptr;
        }
        auto& s = arena.at(at.page());
        auto offset = at.offset();
        if (s.empty())
        {
            return nullptr;
        }
        if(s.decompressed.empty())
        {
            decompress(at);
        }
        if (offset >= s.write_position)
        {
            abort();
        }
        return s.decompressed.begin() + offset;
#endif
    }

    compressed_address new_address(const uint8_t* data, size_t)
    {
        if(mapper.empty())
        {
            mapper.push_back(nullptr);
        }
        //uint8_t *d = heap::allocate<uint8_t>(size);
        //memcpy(d, data, size);
        mapper.push_back(data);
        return compressed_address(mapper.size()-1);
#if 0
        if (arena.empty())
        {   allocate_arena();
        }

        if (size > page_size)
        {
            storage s;
            s.decompressed = heap::buffer<uint8_t>(size);
            s.decompressed.emplace(size, data);
            s.write_position = size;
            s.compressed = compress_2_buffer(data, size);
            s.size++;
            arena.emplace_back(s); // preserve the head
            compressed_address address { (arena.size()-1) * page_size} ;
            //decompressed.emplace_back(address);
            return address;
        }

        if (arena.at(arena_head.page()).write_position + size> page_size)
        {
            flush();
            allocate_arena();
        }

        auto& current = arena.at(arena_head.page());

        if (current.decompressed.empty())
        {
            if(current.compressed.empty())
            {
                current.decompressed = heap::buffer<uint8_t>(page_size);
            }else
            {
                decompress(arena_head);
            }
        }
        if(size == 0)
        {
            abort();
        }

        //add_training_entry(data, size);
        current.decompressed.emplace(current.write_position, size, data);
        compressed_address address {arena_head.page(),current.write_position };
        current.write_position += size;
        current.modifications++;
        current.size++;
        if(current.modifications > current.size)
        {
            abort();
        }

        if (memcmp(resolve(address), data, size) != 0)
        {
            abort();
        }
        return address;
#endif
    }
    size_t release_decompressed(compressed_address at)
    {
        if(arena.empty()) return 0;
        auto& t = arena.at(at.page());
        if (t.modifications) return 0; // TODO: maybe compress it ?
        if (t.decompressed.empty()) return 0;
        if (t.empty()) return 0;
        if (t.compressed.empty())
        {
            return 0;
        }// weird state
        size_t r = t.decompressed.size();
        t.decompressed.release();
        return r;
    }
    size_t release_decompressed()
    {   size_t r = 0;
        for (auto at : decompressed)
        {
            r += release_decompressed(at);
        }
        decompressed.clear();
        return r;
    }
};

#endif //COMPRESS_H
