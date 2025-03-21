//
// Created by linuxlite on 2/7/25.
//

#ifndef COMPRESS_H
#define COMPRESS_H
#include <iostream>
#include "sastam.h"
#include <mutex>
#include <shared_mutex>
#include <ostream>
#include <statistics.h>
#include <stdexcept>
#include <thread>
#include <valkeymodule.h>
#include <vector>
#include <zstd.h>
#include <zdict.h>
#include <functional>
#include <unordered_set>
#include <list>
#include "configuration.h"
enum
{
    page_size = 4096,
    reserved_address_base = 12000000,
    //opt_enable_compression = 1,
    auto_vac = 0,
    auto_vac_workers = 8,
    test_memory = 0,
    allocation_padding = 0,
    coalesce_fragments = 0
};
typedef uint16_t PageSizeType;
typedef std::list<size_t, heap::allocator<size_t>> lru_list;
struct compressed_address
{
    typedef uint64_t AddressIntType;
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

    [[nodiscard]] static bool is_null_base(size_t page)
    {
        return (page % reserved_address_base) == 0;
    }

    [[nodiscard]] bool is_null_base() const
    {
        return is_null_base(page());
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

    bool operator==(AddressIntType other) const
    {
        return index == other;
    }
    bool operator!=(AddressIntType other) const
    {
        return index != other;
    }
    explicit operator size_t() const
    {
        return index;
    }
private:
    AddressIntType index = 0;
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
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        return *this;
    }

    void clear()
    {
        compressed.release();
        decompressed.release();
        write_position = 0;
        modifications = 0;
        size = 0;
        ticker = 0;
        fragmentation = 0;
        lru = lru_list::iterator();
    }

    storage& operator=(storage&& other) noexcept
    {
        if (&other == this) return *this;
        compressed = std::move(other.compressed);
        decompressed = std::move(other.decompressed);
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        other.clear();
        return *this;
    }

    [[nodiscard]] bool empty() const
    {
        return size == 0 && write_position == 0 && compressed.empty() && decompressed.empty();
    }

    heap::buffer<uint8_t> compressed;
    heap::buffer<uint8_t> decompressed;
    unsigned write_position = 0;
    unsigned size = 0;
    unsigned modifications = 0;
    lru_list::iterator lru{};
    uint64_t ticker = 0;
    uint64_t fragmentation = 0;
};
struct size_offset
{
    PageSizeType size{};
    PageSizeType offset{};
};
struct free_page
{
    free_page()= default;
    explicit free_page(compressed_address p) : page(p.page()){};
    heap::vector<PageSizeType> offsets{}; // within page
    uint64_t page{0};
    [[nodiscard]] bool empty() const
    {
        return offsets.empty();
    }
    compressed_address pop()
    {
        if (empty())
        {
            return compressed_address{0};
        }
        PageSizeType r = offsets.back();
        if (r >= page_size)
        {
            abort();
        }
        offsets.pop_back();
        return {page,r};
    }

    void clear()
    {
        offsets.clear();
    }

    void push(PageSizeType offset)
    {
        offsets.push_back(offset);
    }

    [[nodiscard]] size_t size() const
    {
        return offsets.size();
    }
};
struct free_bin
{
    free_bin() = default;
    explicit free_bin(unsigned size) : size(size)
    {
    }
    unsigned size = 0;
    heap::vector<uint32_t> page_index{};
    heap::vector<free_page> pages{};
    struct page_max_t {size_t page{}; size_t size{};};
    [[nodiscard]] page_max_t page_max() const
    {
        size_t sz = 0, pmax = 0;
        for(auto& p: pages)
        {
            if (p.size() > sz)
            {
                pmax = p.page;
                sz = p.size();
            }
        }
        return {pmax, sz};
    }

    [[nodiscard]] bool empty() const
    {
        if (!pages.empty())
        {
            return pages.back().empty();
        }
        return true;
    }

    void get(size_t page, const std::function<void(free_page& p)> &f)
    {
        if (page < page_index.size())
        {
            size_t at = page_index.at(page);
            if (at != 0)
            {
                auto & p = pages.at(at-1);
                f(p);
            }
        }
    }

    [[nodiscard]] bool available() const
    {
        return !empty();
    }
    heap::vector<size_t> get_addresses(size_t page)
    {
        heap::vector<size_t> r;
        if (page < page_index.size()) {
            size_t at = page_index.at(page);
            if (at != 0)
            {
                auto & p = pages.at(at-1);
                for (auto o : p.offsets)
                {
                    compressed_address ad{page, o};
                    r.push_back(ad.address());
                }
            }
        }
        return r;
    }
    unsigned erase(size_t page)
    {   unsigned r = 0;
        get(page,[&](free_page& p) -> void
        {
            if (p.page != page)
            {
                abort();
            }
            r = p.offsets.size() * size;
            p.clear();
            page_index[p.page] = 0;

        });
        return r;
    }
    void add(compressed_address address, unsigned s)
    {
        if (s != size)
        {
            abort();
        }
        if (page_index.size() <= address.page())
        {
            page_index.resize(address.page()+1);
        }
        size_t addr_page = address.page();
        if (page_index[addr_page] == 0)
        {
            pages.emplace_back();
            pages.back().page = address.page();
            page_index[address.page()] = pages.size();
        }
        size_t page = page_index[addr_page] - 1;

        pages[page].push(address.offset());

    }

    compressed_address pop(unsigned s)
    {
        if (s != size)
        {
            abort();
        }
        if (pages.empty())
        {
            return compressed_address(0);
        }
        auto & p = pages.back();
        if (p.empty())
        {
            page_index[p.page] = 0;
            pages.pop_back();
            return compressed_address(0);
        }
        if (page_index[p.page] == 0)
        {
            return compressed_address(0);
        }

        compressed_address address = p.pop();
        if (p.empty())
        {
            page_index[p.page] = 0;
            pages.pop_back();
        }
        return address;

    }
};
struct free_list
{
    size_t added = 0;
    size_t min_bin = page_size;
    size_t max_bin = 0;
    std::unordered_set<size_t> addresses{};
    heap::vector<free_bin> free_bins{};

    free_list()
    {
        for (unsigned i = 0; i < page_size; ++i)
        {
            free_bins.emplace_back(i);
        }
    }
    void inner_add(compressed_address address, unsigned size)
    {
        if (size >= free_bins.size())
        {
            return;
        }
        if (address.is_null_base())
        {
            return;
        }
        if ( test_memory == 1 )
        {
            if (addresses.count(address.address()) > 0)
            {
                abort();
            }
            addresses.insert(address.address());

        }
        free_bins[size].add(address, size);
        min_bin = std::min(min_bin, (size_t)size);
        max_bin = std::max(max_bin, (size_t)size);

        added += size;

    }
    void add(compressed_address address, unsigned size)
    {
        inner_add(address, size);
    }
    void erase(size_t page)
    {

        for (size_t b = min_bin; b <= max_bin; ++b)
        {
            auto &f = free_bins[b];
            auto tobe = f.get_addresses(page);
            if ( test_memory == 1 )
            {
                for (auto o : tobe)
                {
                    if(addresses.count(o) == 0)
                    {
                        abort();
                    }
                    addresses.erase(o);
                }
            }
            unsigned r = f.erase(page);

            if (added >= r)
            {
                added -= r;
            } else
            {
                abort();
            }
        }

    }
    compressed_address get(unsigned size)
    {
        if (size >= free_bins.size())
        {
            return compressed_address{0};
        }
        if (!added) return compressed_address{0};

        compressed_address r = free_bins[size].pop(size);
        if (!r.null())
        {
            if (added < size)
            {
                abort();
            }
            added -= size;
            if ( test_memory == 1 )
            {
                if (addresses.count(r.address()) == 0)
                {
                    abort();
                }
                addresses.erase(r.address());
            }
        }


        return r;
    }
};

struct vector_arena
{
private:
    std::vector<storage> hidden_arena{};
    std::unordered_set<size_t> freed_pages{};
public:
    // arena virtualization functions
    void expand_address_space()
    {
        hidden_arena.emplace_back();
    }
    [[nodiscard]] size_t page_count() const
    {
        return hidden_arena.size();
    }
    [[nodiscard]] size_t max_logical_address() const
    {
        if (hidden_arena.empty()) return 0;
        return hidden_arena.size() - 1;
    }
    void free_page(size_t at)
    {
        if (freed_pages.count(at))
        {
            throw std::runtime_error("page already free");
        };
        freed_pages.insert(at);
    }
    bool is_free(size_t at) const
    {
        return freed_pages.count(at) > 0;
    }
    bool has_free() const
    {
        return !freed_pages.empty();
    }
    size_t emancipate()
    {
        if(!has_free())
        {
            throw std::runtime_error("no free pages available");
        }
        size_t to = *freed_pages.begin();
        recover_free(to);
        return to;
    }
    void recover_free(size_t at)
    {
        if(!is_free(at))
        {
            throw std::runtime_error("page not free");
        }
        if (at <= max_logical_address())
        {
            throw std::runtime_error("invalid address");
        }
        freed_pages.erase(at);
    }
    storage& retrieve_page(size_t at)
    {
        if (at >= hidden_arena.size())
        {
            throw std::runtime_error("invalid page");
        }
        return hidden_arena.at(at);
    }
    [[nodiscard]] const storage& retrieve_page(size_t at) const
    {
        if (at >= hidden_arena.size())
        {
            throw std::runtime_error("invalid page");
        }
        return hidden_arena.at(at);
    }
};

struct hash_arena
{
    enum
    {
        cache_size = 1024
    };
private:
    std::unordered_map<size_t,storage> hidden_arena{};
    heap::vector<size_t> free_address_list {};
    size_t top = 10000000;
    size_t free_pages = top;
    size_t last_allocated = 0;
    void recover_free(size_t at)
    {
        if(!is_free(at))
        {
            throw std::runtime_error("page not free");
        }
        if (at > max_logical_address())
        {
            free_pages += at - max_logical_address();
            top = at;
        }
        if (!has_free())
        {
            throw std::runtime_error("no free pages available");
        }
        hidden_arena[at] = storage{};
        --free_pages;
    }
public:
    hash_arena() = default;
    // arena virtualization functions
    void expand_address_space()
    {
        if (hidden_arena.empty() && top == 0)
        {
            hidden_arena[top] = storage{};
        }else
        {
            hidden_arena[++top] = storage{};
        }
    }

    size_t page_count() const
    {
        return hidden_arena.size();
    }
    size_t max_logical_address() const
    {
        return top;
    }
    void free_page(size_t at)
    {
        if (hidden_arena.empty())
        {
            throw std::runtime_error("no pages left to free");
        }
        auto pi = hidden_arena.find(at);
        if (pi == hidden_arena.end())
        {
            throw std::runtime_error("page already free");
        }
        hidden_arena.erase(pi);
        ++free_pages;
        free_address_list.emplace_back(at);
    }
    bool is_free(size_t at) const
    {
        return hidden_arena.count(at) == 0;
    }

    bool has_free() const
    {
        return free_pages > 1;
    }
    size_t allocate()
    {
        if(!has_free())
        {
            throw std::runtime_error("no free pages available");
        }
        if (!free_address_list.empty())
        {
            size_t at = free_address_list.back();
            free_address_list.pop_back();
            recover_free(at);
            return at;
        }
        if (last_allocated > 0)
        {
            if (is_free(last_allocated+1))
            {
               ++last_allocated;
                recover_free(last_allocated);
                return last_allocated;
            }
        }
        for(size_t to = 1; to < top; ++to)
        {
            if (is_free(to) && !compressed_address::is_null_base(to))
            {
                last_allocated = to;
                recover_free(to);
                return to;
            }
        }
        throw std::runtime_error("no free pages found");
    }

    bool has_page(size_t at) const
    {
        return hidden_arena.count(at) != 0;
    }

    storage& retrieve_page(size_t at)
    {
        if (at > top)
        {
            throw std::runtime_error("invalid page");
        }
        auto pi = hidden_arena.find(at);
        if (pi==hidden_arena.end())
        {
            throw std::runtime_error("missing page");
        }
        return pi->second;
    }
    const storage& retrieve_page(size_t at) const
    {
        if (at > top)
        {
            throw std::runtime_error("missing page");
        }
        auto pi = hidden_arena.find(at);
        if (pi==hidden_arena.end())
        {
            throw std::runtime_error("invalid page");
        }
        return pi->second;
    }
    void iterate_arena(const std::function<bool(size_t, storage&)>& iter)
    {
        for(auto&[at,str] : hidden_arena)
        {
            if(!iter(at, str))
            {
                return;
            }

        }
    }

    void iterate_arena(const std::function<void(size_t, storage&)>& iter)
    {
        for(auto&[at,str] : hidden_arena)
        {
            iter(at, str);
        }
    }};

struct compress : hash_arena
{
    enum
    {
        min_training_size = 1024 * 120,
        compression_level = 1
    };

    compress() = default;
    explicit compress(bool opt_enable_compression, bool opt_enable_lru, std::string name): opt_enable_compression(opt_enable_compression), opt_enable_lru(opt_enable_lru),name(std::move(name)){};
    compress(const compress&) = delete;

    ~compress()
    {
        threads_exit = true;
        if(tpoll.joinable())
            tpoll.join();
        ZSTD_freeCCtx(cctx);
    }
    static uint32_t flush_ticker;
    static std::shared_mutex vacuum_scope;

private:
    bool opt_enable_compression = false;
    bool opt_enable_lru = false;
    bool opt_validate_addresses = true;
    std::thread tdict{};
    std::thread tpoll{};
    bool threads_exit = false;
    unsigned size_in_training = 0;
    unsigned size_to_train = 0;
    heap::buffer<uint8_t> training_data{0};
    std::vector<training_entry, heap::allocator<training_entry>> trainables{};
    std::vector<training_entry, heap::allocator<training_entry>> intraining{};
    ZSTD_CDict* dict{nullptr};
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    size_t trained_size = 0;
    static std::mutex mutex;
    /// prevents other threads from allocating memory while vacuum is taking place
    /// it must be entered and left before the allocation mutex to prevent deadlocks
    heap::vector<size_t> decompressed_pages{};
    size_t last_page_allocated{0};
    compressed_address arena_head{0};
    compressed_address highest_reserve_address{0};
    uint64_t last_heap_bytes = 0;
    uint64_t release_counter = 0;
    std::chrono::time_point<std::chrono::system_clock> last_vacuum_millis = std::chrono::high_resolution_clock::now();;
    free_list emancipated{};
    lru_list lru{};
    uint64_t ticker = 1;
    uint64_t allocated = 0;
    size_t fragmentation = 0;
    std::unordered_set<size_t> fragmented{};
    std::unordered_set<size_t> erased{}; // for runtime use after free tests
    std::string name;
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
            tpoll = std::thread([&]()
            {
                while (!threads_exit)
                {
                    if(heap::get_physical_memory_ratio() > 0.95 || heap::allocated > art::get_max_module_memory())
                        full_vacuum();
                    std::this_thread::sleep_for(std::chrono::milliseconds(45));
                }
            });
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

    [[nodiscard]] bool decompress(std::pair<size_t,storage&>& param)
    {
        storage& todo = param.second;
        if (todo.decompressed.empty() && !todo.compressed.empty())
        {
            todo.decompressed = decompress_buffer(page_size, todo.compressed);
            statistics::page_bytes_uncompressed += todo.decompressed.byte_size();
            ++statistics::pages_uncompressed;

            if (todo.decompressed.size() != page_size)
            {
                abort();
            }
            decompressed_pages.push_back(param.first);
            return true;
        }
        return false;
    }

    [[nodiscard]] static bool is_null_base(const compressed_address& at)
    {
        return at.is_null_base();
    }

    [[nodiscard]] static bool is_null_base(size_t at)
    {
        return compressed_address::is_null_base(at);
    }

    [[nodiscard]] size_t last_block() const
    {
        return max_logical_address();
    }
    void add_to_lru(size_t at, storage& page)
    {
        if(opt_enable_lru)
        {
            lru.emplace_back(at);
            page.lru = --lru.end();
            page.ticker = ++ticker;;
        }
    }
    void update_lru(size_t at, storage& page)
    {
        if (opt_enable_lru)
        {
            if (page.lru == lru_list::iterator())
            {
                lru.emplace_back(at);
                page.lru = --lru.end();
            }else
            {
                if(*page.lru != at)
                {
                    abort();
                }
                lru.splice(lru.begin(), lru, page.lru);
            }
        }
    }
    std::pair<size_t, storage&> allocate_page_at(size_t at, size_t ps = page_size)
    {
        auto &page = retrieve_page(at);
        add_to_lru(at, page);
        page.decompressed = std::move(heap::buffer<uint8_t>(ps));
        statistics::page_bytes_uncompressed += page.decompressed.byte_size();
        ++statistics::pages_uncompressed;
        memset(page.decompressed.begin(), 0, ps);
        decompressed_pages.push_back(at);
        return {at,page};
    }
    std::pair<size_t, storage&>  expand_over_null_base(size_t ps = page_size)
    {
        auto at = allocate();
        last_page_allocated = at;
        return allocate_page_at(at,ps);

    }
    std::pair<size_t, storage&> create_if_required(size_t size)
    {

        if (size > page_size)
        {
            return expand_over_null_base(size);
        }
        if (last_page_allocated == 0)
        {
            return expand_over_null_base();
        }

        auto& last = retrieve_page(last_page_allocated);
        if (last.empty())
        {
            abort();
        }
        if (last.write_position + size >= page_size)
        {
            if (!dict && !last.decompressed.empty())
            {
                add_training_entry(last.decompressed.begin(), last.write_position);
            }
            else if (!last.decompressed.empty() && auto_vac != 0)
            {
                // if (last.modifications > 0)
                   // to schedule a buffer release/compress
            }

            return expand_over_null_base();
        }

        return {last_page_allocated, last};
    }

    // compression can happen in any single thread - even a worker thread
    size_t release_decompressed(ZSTD_CCtx* cctx, size_t at)
    {
        if (is_null_base(at)) return 0;
        size_t r = 0;
        heap::buffer<uint8_t> torelease;

        auto& t = retrieve_page(at);

        if (t.modifications && !t.compressed.empty())
        {
            if(t.decompressed.empty())
            {
                abort();
            }
            statistics::page_bytes_compressed -= t.compressed.size();
            --statistics::pages_compressed;
            t.compressed.release();

        }

        if (t.compressed.empty() && !t.decompressed.empty())
        {
            t.compressed = compress_2_buffer(dict, cctx, t.decompressed.begin(), page_size);
            if (t.compressed.empty())
            {
                abort();
            }
            statistics::page_bytes_compressed += t.compressed.size();
            ++statistics::pages_compressed;
            t.modifications = 0;
        }

        if (!t.decompressed.empty() && !t.compressed.empty() && t.modifications == 0)
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
        if (test_memory)
        {
            if (erased.count(at.address()))
            {
                std::cerr << "allocator '" << name << "' is accessing memory after free ( at: " << at.address() << ")" << std::endl;
                throw std::runtime_error("memory erased");
            }
        }
        auto p = at.page();
        auto& t = retrieve_page(p);
        if (t.decompressed.empty() && !t.compressed.empty())
        {
            std::pair<size_t, storage&> dec = {p,t};
            if (decompress(dec))
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
            throw std::runtime_error("empty page");
        }
        if (modify)
        {
            t.modifications++;
        }
        update_lru(at.page(), t);

        return t.decompressed.begin() + at.offset();
    }

    void invalid(compressed_address at) const
    {
        valid(at);

    }

    void valid(compressed_address at) const
    {
        if(!opt_validate_addresses) return;

        if (at == 0) return ;
        auto pg = at.page();
        if (pg > max_logical_address())
        {
            throw std::runtime_error("invalid page");
        }
        if (is_free(pg))
        {
            throw std::runtime_error("deleted page");
        }
        const auto& t = retrieve_page(at.page());
        if (t.size == 0)
        {
            throw std::runtime_error("invalid page size");
        }
        if (t.write_position <= at.offset())
        {
            throw std::runtime_error("invalid page write position");
        }
        //return true;
    }

    // TODO: put a time limit on this function because it can take long
    size_t inner_vacuum()
    {
        std::atomic<size_t> r = 0;
        std::thread workers[auto_vac_workers];
        size_t arena_size = max_logical_address();
        ++flush_ticker;
        for (unsigned ivac = 0; ivac < auto_vac_workers; ivac++)
        {
            workers[ivac] = std::thread([this,arena_size,ivac,&r]()
            {
                ZSTD_CCtx* cctx = ZSTD_createCCtx();
                iterate_arena([&](size_t p,storage&) -> void
                {
                    if(p % auto_vac_workers == ivac)
                        r += release_decompressed(cctx,p);
                });

                ZSTD_freeCCtx(cctx);
            });
        }
        for (auto& worker : workers) worker.join();

        decompressed_pages.clear();
        ++statistics::vacuums_performed;
        return r;
    }
    std::pair<heap::buffer<uint8_t>,size_t> get_page_buffer_inner(size_t at)
    {
        if(is_null_base(at)) return {};
        if(!has_page(at)) return {};
        auto& t = retrieve_page(at);
        if (t.empty()) return {};
        valid({at,0});
        std::pair<size_t, storage&> dec = {at,t};
        if (decompress(dec))
        {   // we might signal this as decompressed
            if(!t.compressed.empty())
            {
                statistics::page_bytes_compressed -= t.compressed.byte_size();
                --statistics::pages_compressed;
                t.compressed.release();
            }
        }
        if (t.decompressed.empty())
        {
            return {heap::buffer<uint8_t>(),0};
        }
        return {t.decompressed,t.write_position};

    }

public:
    void set_opt_enable_compression(bool opt_compression)
    {
        this->opt_enable_compression = opt_compression;
    }
    void free(compressed_address at, size_t sz)
    {
        size_t size = sz + test_memory + allocation_padding;
        std::lock_guard guard(mutex);

        uint8_t * d1 = (test_memory == 1) ? basic_resolve(at) : nullptr;
        if (allocated < size)
        {
            abort();
        }
        allocated -= size;
        if (at.address() == 0 || size == 0)
        {
            abort();
        }
        if (test_memory == 1 && d1[sz] != at.address() % 255)
        {
            abort();
        }
        if (test_memory == 1)
            d1[sz] = 0;
        auto& t = retrieve_page(at.page());
        if (t.size == 0)
        {
            abort();
        }
        if (test_memory == 1)
        {
            memset(d1,0, size);
        }
        if (test_memory)
        {
            if (erased.count(at.address()))
            {
                throw std::runtime_error("memory erased");
            }
            erased.insert(at.address());
        }

        if (t.size == 1)
        {
            auto tp = at.page();

            if (is_free(tp))
            {
                abort();
            }
            if (last_page_allocated == tp)
            {
                last_page_allocated = 0;
            }
            t.size = 0;
            t.modifications = 0;
            if (fragmentation < t.fragmentation)
            {
                abort();
            }
            fragmentation -= t.fragmentation;
            //t.write_position = 0;


            if(!t.compressed.empty())
            {
                --statistics::pages_compressed;
                statistics::page_bytes_compressed -= t.compressed.byte_size();
                t.compressed.release();
            }

            if(!t.decompressed.empty())
            {
                --statistics::pages_uncompressed;
                statistics::page_bytes_uncompressed -= t.decompressed.byte_size();
            }

            if(!lru.empty())
                lru.erase(t.lru);
            t.clear();
            emancipated.erase(at.page());
            free_page(at.page());

            fragmented.erase(at.page());
            //free_pages.push_back(at.page());

            if (fragmentation < t.fragmentation)
            {
                abort();
            }
        }
        else
        {
            emancipated.add(at, size); // add a free allocation for later re-use
            t.size--;
            t.modifications++;
            if (t.fragmentation + size > t.write_position)
            {
                abort();
            }
            t.fragmentation += size;
            fragmentation += size;
            fragmented.insert(at.page());
        }
    }
    float fragmentation_ratio() const
    {
        std::lock_guard guard(mutex);
        return (float)fragmentation/(float(allocated)+0.0001f);
    }
    // TODO: this function may cause to much latency when the arena is large
    // maybe just dont iterate through everything - it doesnt need to get
    // every page
    heap::vector<size_t> create_fragmentation_list(size_t max_pages) const
    {
        std::lock_guard guard(mutex);
        heap::vector<size_t> pages;
        if (fragmented.empty()) return {};
        for(auto page : fragmented)
        {
            pages.push_back(page);
            if (pages.size() >= max_pages)
            {
                return pages;
            }
        }
        return pages;
    }
    lru_list create_lru_list()
    {
        std::lock_guard guard(mutex);
        return lru;
    }
    std::pair<heap::buffer<uint8_t>,size_t> get_page_buffer(size_t at)
    {
        std::lock_guard guard(mutex);
        return get_page_buffer_inner(at);

    }
    std::pair<heap::buffer<uint8_t>,size_t> get_lru_page()
    {
        std::lock_guard guard(mutex);
        if (opt_enable_lru)
        {
            if(!lru.empty())
            {
                return get_page_buffer_inner(*(--lru.end()));
            }
        }
        return {heap::buffer<uint8_t>(), 0};
    }

    template <typename T>
    T* read(compressed_address at)
    {
        if (at.null()) return nullptr;
        std::lock_guard guard(mutex);
        const uint8_t * d = basic_resolve(at);
        return (T*)d;
    }

    template <typename T>
    T* modify(compressed_address at)
    {
        if (at.null()) return nullptr;
        std::lock_guard guard(mutex);
        uint8_t * d = basic_resolve(at,true);
        return (T*)d;
    }

    compressed_address new_address(size_t sz)
    {
        size_t size = sz + test_memory + allocation_padding;
        std::lock_guard guard(mutex);
        compressed_address r = emancipated.get(size);
        if(!r.null() && r.page() <= max_logical_address() && !retrieve_page(r.page()).empty())
        {   auto& pcheck = retrieve_page(r.page());

            PageSizeType w = r.offset();
            if (w + size > pcheck.write_position)
            {
                pcheck.write_position = w + size;
            }
            std::pair<size_t,storage&> at = {r.page(),pcheck};
            if (decompress(at))
            {
                statistics::page_bytes_compressed -= at.second.compressed.byte_size();
                --statistics::pages_compressed;
                at.second.compressed.release();
                // we might signal this as decompressed
            };
            // last_page_allocated should not be set here
            at.second.size++;
            at.second.modifications++;
            at.second.fragmentation -= size;

            invalid(r);
            auto* data = test_memory == 1 ? basic_resolve(r) : nullptr;
            if (test_memory == 1 && data[sz] != 0)
            {
                abort();
            }
            if(test_memory == 1)
                data[sz] = r.address() % 255;
            if (at.second.decompressed.empty())
            {
                abort();
            }
            memset(at.second.decompressed.begin() + r.offset(), 0, sz);
            allocated += size;
            return r;
        }
        auto at = create_if_required(size);
        if (is_null_base(at.first))
        {
            abort();
        }
        if (at.second.decompressed.empty() && !at.second.compressed.empty())
        {
            if (decompress(at))
            {
                statistics::page_bytes_compressed -= at.second.compressed.byte_size();
                --statistics::pages_compressed;
                at.second.compressed.release();
                // we might signal this as decompressed
            };
        }
        if (at.second.write_position + size > at.second.decompressed.size() || at.second.decompressed.empty())
        {
            abort();
        }
        last_page_allocated = at.first;
        compressed_address ca(at.first, at.second.write_position);
        at.second.write_position += size;
        at.second.size++;
        at.second.modifications++;
        invalid(ca);
        if (test_memory)
        {
            erased.erase(ca.address());
        }
        auto* data = (test_memory == 1 ) ?  basic_resolve(ca) : nullptr;
        if (test_memory == 1 && data[sz] != 0)
        {
            abort();
        }
        if (test_memory == 1)
            data[sz] = ca.address() % 255;
        allocated += size;

        return ca;
    }

    void release_context()
    {   vacuum_scope.unlock_shared();

        if (statistics::page_bytes_compressed > heap::allocated)
        {
            abort();
        }
        if (statistics::addressable_bytes_alloc < statistics::interior_bytes_alloc)
        {
            abort();
        }
        context_vacuum();
    }
    size_t full_vacuum()
    {
        std::lock_guard guard(mutex);
        statistics::max_page_bytes_uncompressed = std::max<uint64_t>(statistics::page_bytes_uncompressed, statistics::max_page_bytes_uncompressed);

        auto t = std::chrono::high_resolution_clock::now();
        auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - last_vacuum_millis);
        uint64_t total_heap = heap::allocated;
        size_t result = 0;
        if ( d.count() > 40 )
        {
            if (total_heap < statistics::page_bytes_compressed)
            {
                abort();
            }
            auto start_vac = std::chrono::high_resolution_clock::now();
            result = inner_vacuum();
            auto end_vac = std::chrono::high_resolution_clock::now();
            auto dvac = std::chrono::duration_cast<std::chrono::milliseconds>(end_vac - start_vac);
            statistics::last_vacuum_time = dvac.count();
            last_heap_bytes = statistics::page_bytes_uncompressed;
            last_vacuum_millis = std::chrono::high_resolution_clock::now();

        }
        if(!last_heap_bytes) last_heap_bytes = statistics::page_bytes_compressed;

        return result;

    }
    size_t context_vacuum()
    {
        std::unique_lock scope(vacuum_scope);
        std::lock_guard guard(mutex);
        if (!opt_enable_compression) return 0;
        double ratio = heap::get_physical_memory_ratio();
        bool heap_overflow = heap::allocated > art::get_max_module_memory();
        if ((heap_overflow || ratio > 0.99) && !decompressed_pages.empty())
        {
            //++flush_ticker;
            //size_t at = decompressed_pages.back();
            //decompressed_pages.pop_back();
            return 0; //release_decompressed(cctx,at);

        }

        return 0;


    }

    void enter_context()
    {
        vacuum_scope.lock_shared();
    }
    size_t vacuum()
    {
        vacuum_scope.unlock_shared();
        size_t r = full_vacuum();
        vacuum_scope.lock_shared();
        return r;
    }


};

#endif //COMPRESS_H
