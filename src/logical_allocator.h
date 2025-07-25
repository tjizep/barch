//
// Created by linuxlite on 2/7/25.
//

#ifndef COMPRESS_H
#define COMPRESS_H
#include <iostream>
#include <fstream>
#include "sastam.h"
#include <mutex>
#include <shared_mutex>
#include <ostream>
#include <statistics.h>
#include <stdexcept>
#include <thread>
#include <valkeymodule.h>
#include <vector>
//#include <zstd.h>
//#include <zdict.h>
#include <functional>
#include <list>
#include <logger.h>
#include <unordered_set>

#include "ioutil.h"
#include "configuration.h"
#include "logical_address.h"
#include "constants.h"
#include "storage.h"
#include "hash_arena.h"
#include "page_modifications.h"

typedef uint32_t PageSizeType;
typedef heap::set<size_t> address_set;
enum {
    LPageSize = page_size - sizeof(storage)

};
struct alloc_pair;
struct training_entry {
    training_entry(const uint8_t *data, size_t size) : data(data), size(size) {
    }

    const uint8_t *data;
    size_t size;
};


struct size_offset {
    PageSizeType size{};
    PageSizeType offset{};
};

struct free_page {
    free_page() = delete;
    free_page& operator=(const free_page&) = default;
    free_page(const free_page&) = default;
    explicit free_page(logical_address p) : ap(&p.get_ap<alloc_pair>()), page(p.page()) {
    };
    alloc_pair* ap{nullptr};
    heap::vector<PageSizeType> offsets{}; // within page
    uint64_t page{0};

    [[nodiscard]] bool empty() const {
        return offsets.empty();
    }

    logical_address pop() {
        if (empty()) {
            return logical_address{0,ap};
        }
        PageSizeType r = offsets.back();
        if (r >= LPageSize) {
            abort_with("invalid offset");
        }
        offsets.pop_back();
        return {page, r, ap};
    }

    void clear() {
        offsets.clear();
    }

    void push(PageSizeType offset) {
        offsets.push_back(offset);
    }

    [[nodiscard]] size_t size() const {
        return offsets.size();
    }
};
struct free_bin {

    free_bin() = default;

    free_bin(unsigned size, alloc_pair* alloc) : alloc(alloc), size(size) {
    }
    free_bin(const free_bin& other) = default; //: alloc(other.alloc),size(other.size) {};
    free_bin& operator=(const free_bin& other) = default;

    alloc_pair * alloc = nullptr;
    unsigned size = 0;
    heap::vector<uint32_t> page_index{};
    heap::vector<free_page> pages{};

    struct page_max_t {
        size_t page{};
        size_t size{};
    };

    [[nodiscard]] page_max_t page_max() const {
        size_t sz = 0, pmax = 0;
        for (auto &p: pages) {
            if (p.size() > sz) {
                pmax = p.page;
                sz = p.size();
            }
        }
        return {pmax, sz};
    }

    [[nodiscard]] bool empty() const {
        if (!pages.empty()) {
            return pages.back().empty();
        }
        return true;
    }

    void get(size_t page, const std::function<void(free_page &p)> &f) {
        if (page < page_index.size()) {
            size_t at = page_index.at(page);
            if (at != 0) {
                auto &p = pages.at(at - 1);
                f(p);
            }
        }
    }

    [[nodiscard]] bool available() const {
        return !empty();
    }

    heap::vector<size_t> get_addresses(size_t page) {
        heap::vector<size_t> r;
        if (page < page_index.size()) {
            size_t at = page_index.at(page);
            if (at != 0) {
                auto &p = pages.at(at - 1);
                for (auto o: p.offsets) {
                    logical_address ad{page, o, alloc};
                    r.push_back(ad.address());
                }
            }
        }
        return r;
    }

    unsigned erase(size_t page) {
        unsigned r = 0;
        get(page, [&](free_page &p) -> void {
            if (p.page != page) {
                abort_with("page does not match");
            }
            r = p.offsets.size() * size;
            p.clear();
            page_index[p.page] = 0;
        });
        return r;
    }

    void add(logical_address address, unsigned s) {
        if (s != size) {
            abort_with("invalid size match");
        }
        if (page_index.size() <= address.page()) {
            page_index.resize(address.page() + 1);
        }
        size_t addr_page = address.page();
        if (page_index[addr_page] == 0) {
            pages.emplace_back(alloc);
            pages.back().page = address.page();
            page_index[address.page()] = pages.size();
        }
        size_t page = page_index[addr_page] - 1;

        pages[page].push(address.offset());
    }

    logical_address pop(unsigned s) {
        if (s != size) {
            abort_with("invalid free list size");
        }
        if (pages.empty()) {
            return logical_address(0,alloc);
        }
        auto &p = pages.back();
        if (p.empty()) {
            page_index[p.page] = 0;
            pages.pop_back();
            return logical_address(0,alloc);
        }
        if (page_index[p.page] == 0) {
            return logical_address(0,alloc);
        }

        logical_address address = p.pop();
        if (p.empty()) {
            page_index[p.page] = 0;
            pages.pop_back();
        }
        return address;
    }
};
struct free_list {
    alloc_pair * alloc = nullptr;
    size_t added = 0;
    size_t min_bin = LPageSize;
    size_t max_bin = 0;
    address_set addresses{};
    heap::std_vector<free_bin> free_bins{};
    //heap::vector<free_bin> free_bins{};
    free_list(const free_list& other) = default;
    free_list& operator=(const free_list& other) {
        if (this == &other) return *this;
        alloc = other.alloc;
        added = other.added;
        min_bin = other.min_bin;
        max_bin = other.max_bin;
        addresses = other.addresses;
        free_bins = other.free_bins;
        return *this;
    }
    free_list(alloc_pair* alloc): alloc(alloc) {

    }
    void clear() {
        added = 0;
        addresses.clear();
        max_bin = 0;
        min_bin = LPageSize;
        free_bins.clear();
    }

    void inner_add(logical_address address, unsigned size) {

        if (LPageSize < size) {
            abort_with("invalid allocation size");
        }

        if (size >= free_bins.size()) {
            size_t s = free_bins.size();
            for (unsigned i = s; i < size + 1; ++i) {
                free_bins.emplace_back(i, alloc);
            }
        }
        if (size >= free_bins.size()) {
            abort_with("bin not initialized");
        }

        if (address.is_null_base()) {
            return;
        }

        if (test_memory == 1) {
            if (addresses.count(address.address()) > 0) {
                abort_with("address already freed");
            }
            addresses.insert(address.address());
        }
        free_bins[size].add(address, size);
        min_bin = std::min(min_bin, (size_t) size);
        max_bin = std::max(max_bin, (size_t) size);

        added += size;
    }

    void add(logical_address address, unsigned size) {
        inner_add(address, size);
    }

    void erase(size_t page) {
        for (size_t b = min_bin; b <= max_bin; ++b) {
            auto &f = free_bins[b];
            auto tobe = f.get_addresses(page);
            if (test_memory == 1) {
                for (auto o: tobe) {
                    if (addresses.count(o) == 0) {
                        abort();
                    }
                    addresses.erase(o);
                }
            }
            unsigned r = f.erase(page);

            if (added >= r) {
                added -= r;
            } else {
                abort_with("unbalanced pointer size");
            }
        }
    }

    logical_address get(unsigned size) {
        if (size >= free_bins.size()) {
            return logical_address{0,alloc};
        }
        if (!added) return logical_address{0,alloc};

        logical_address r = free_bins[size].pop(size);
        if (!r.null()) {
            if (added < size) {
                abort_with("trying to free too many bytes");
            }
            if (test_memory == 1) {
                auto& tap = r.get_ap<alloc_pair>();
                if (&tap != this->alloc) {
                    abort_with("allocation pair from different tree");
                }
                if (addresses.count(r.address()) == 0) {
                    abort_with("memory test failed: no such free address");
                }

                addresses.erase(r.address());
            }
            added -= size;

        }


        return r;
    }
};


struct logical_allocator {

    logical_allocator(alloc_pair* ap,std::string name): ap(ap), main(std::move(name)), emancipated(ap) {}

    explicit
    logical_allocator(bool opt_enable_lru, alloc_pair* ap, std::string name): ap(ap), main(std::move(name)), opt_enable_lru(opt_enable_lru) {
        opt_page_trace = art::get_log_page_access_trace();
    };

    logical_allocator(const logical_allocator &) = delete;

    ~logical_allocator() = default;


private:

    mutable alloc_pair * ap = nullptr;
    arena::hash_arena main;
    bool opt_page_trace = false;
    bool opt_enable_lru = false;
    bool opt_enable_lfu = false;
    bool opt_validate_addresses = false;
    bool opt_move_decompressed_pages = false;
    unsigned opt_iterate_workers = 1;

    size_t last_page_allocated{0};
    logical_address highest_reserve_address{0,ap};
    uint64_t last_heap_bytes = 0;
    uint64_t ticker = 1;
    uint64_t allocated = 0;
    size_t fragmentation = 0;

    std::chrono::time_point<std::chrono::system_clock> last_vacuum_millis = std::chrono::high_resolution_clock::now();;
    free_list emancipated{nullptr};
    lru_list lru{};

    address_set fragmented{};
    address_set erased{}; // for runtime use after free tests
    size_t last_created_page{};
    uint8_t *last_page_ptr{};
    logical_allocator &operator=(const logical_allocator &t) {
        if (this == &t) return *this;
        return *this;
    };


    // arena virtualization start
    storage &retrieve_page(size_t page, bool modify = false) {

        if (modify) {
            //main.modify(page);
            page_modifications::inc_ticker(page);
            return *(storage*)main.get_page_data({page,LPageSize,ap}, modify); //main.modify(page);
        }
        return *(storage*)main.get_page_data({page,LPageSize,ap}, modify);
    }

    [[nodiscard]] const storage &retrieve_page(size_t page) const {
        return *(const storage*)main.get_page_data({page, LPageSize, ap}, false);//main.read(page);
    }

    [[nodiscard]] size_t max_logical_address() const {
        return main.max_logical_address();
    }

    size_t allocate() {
        return main.allocate();
    }

    [[nodiscard]] bool has_free() const {
        return main.has_free();
    }

    [[nodiscard]] bool is_free(size_t page) const {
        return main.is_free(page);
    }

    void free_page(size_t page) {
        main.free_page(page);
    }

    bool has_page(size_t page) {
        return main.has_page(page);
    }

    void iterate_arena(const std::function<void(size_t, size_t &)> &iter) {
        main.iterate_arena(iter);
    }

    void iterate_arena(const std::function<void(size_t, const size_t &)> &iter) const {
        main.iterate_arena(iter);
    }
    uint8_t* get_page_data(logical_address at) {
        if (at.offset() > LPageSize) {
            abort_with("offset too large");
        }
        return main.get_page_data({at.page(),at.offset(),ap},true);
    }
    const uint8_t* get_page_data(logical_address at) const {
        if (at.offset() > LPageSize) {
            abort_with("offset too large");
        }

        return main.get_page_data({at.page(),at.offset(),ap},true);
    }
    uint8_t * get_alloc_page_data(logical_address at, size_t size) {
        if (size > LPageSize) {
            abort_with("allocation too large");
        }
        if (at.offset() == 0) {
            return main.get_alloc_page_data({at.page(),0,ap},size);
        }
        return main.get_alloc_page_data({at.page(),at.offset(), ap},size);
    }
    // arena virtualization end


    [[nodiscard]] static bool is_null_base(const logical_address &at) {
        return at.is_null_base();
    }

    [[nodiscard]] static bool is_null_base(size_t at) {
        return logical_address::is_null_base(at);
    }

    [[nodiscard]] size_t last_block() const {
        return main.max_logical_address();
    }

    void add_to_lru(size_t at, storage &page) {
        if (opt_enable_lru) {
            lru.emplace_back(at);
            page.lru = --lru.end();
        }
        if (opt_enable_lfu||opt_enable_lru) {
            page.ticker = ++ticker;
        }
    }

    void update_lru(size_t at, storage &page) {
        if (opt_enable_lru) {
            if (page.lru == lru_list::iterator()) {
                lru.emplace_back(at);
                page.lru = --lru.end();
            } else {
                if (*page.lru != at) {
                    abort();
                }
                lru.splice(lru.begin(), lru, page.lru);
            }
        }
    }

    std::pair<size_t, storage &> allocate_page_at(size_t at, size_t) //unused(ps = SS)
    {
        auto &page = retrieve_page(at);
        add_to_lru(at, page);
        if (initialize_memory == 1) {
        }

        return {at, page};
    }

    std::pair<size_t, storage &> expand_over_null_base(size_t ps = LPageSize) {
        auto at = allocate();
        if (is_null_base(at)) {
            at = allocate();
        }
        last_page_allocated = at;
        static_assert((size_t)LPageSize<(size_t)page_size);
        static_assert((size_t)LPageSize==(size_t)(page_size - sizeof(storage)));
        main.get_alloc_page_data({at,0,ap}, page_size);
        return allocate_page_at(at, ps);
    }

    std::pair<size_t, storage &> create_if_required(size_t size) {
        if (size > LPageSize) {
            return expand_over_null_base(size);
        }
        if (last_page_allocated == 0) {
            return expand_over_null_base();
        }

        auto &last = retrieve_page(last_page_allocated, false);
        if (last.empty()) {
            abort();
        }
        if (last.write_position + size >= LPageSize) {
            return expand_over_null_base();
        }

        return {last_page_allocated, last};
    }

    size_t release_decompressed(size_t at) {
        if (is_null_base(at)) return 0;
        size_t r = 0;
        heap::buffer<uint8_t> torelease;

        page_modifications::inc_ticker(at);

        return r;
    }

    uint8_t *basic_resolve(logical_address at, bool modify = false) {
        if (opt_page_trace) {
            art::std_log("page trace [", main.name, "]:", at.address(), at.page(), at.offset(), modify);
        }
        if (at.null()) return nullptr;
        if (opt_enable_lru) {
            auto p = at.page();
            auto &t = retrieve_page(p, modify);
            update_lru(at.page(), t);
        }


        return get_page_data(at);
    }

    void invalid(logical_address at) const {
        if (!opt_validate_addresses) return;
        valid(at);
    }

    void valid(logical_address at) const {
        if (!opt_validate_addresses) return;

        if (at == 0) return;
        auto pg = at.page();
        if (pg > max_logical_address()) {
            throw std::runtime_error("invalid page");
        }
        if (is_free(pg)) {
            throw std::runtime_error("deleted page");
        }
        const auto &t = retrieve_page(at.page());
        if (t.size == 0) {
            throw std::runtime_error("invalid page size");
        }
        if (t.write_position <= at.offset()) {
            throw std::runtime_error("invalid page write position");
        }
    }


    std::pair<heap::buffer<uint8_t>, size_t> get_page_buffer_inner(size_t at) {
        if (is_null_base(at)) return {};
        if (!has_page(at)) return {};
        auto &t = retrieve_page(at);
        if (t.empty()) return {};
        valid({at, 0, ap});

        return {heap::buffer{get_page_data({at,0, ap}), t.write_position}, t.write_position};
    }

public:
    const std::string& get_name() const { return main.name; }
    void set_opt_trace_page(bool value) {
        opt_page_trace = value;
    }


    [[nodiscard]] uint64_t get_allocated() const {
        return allocated;
    }

    void free(logical_address at, size_t sz) {
        size_t size = sz + test_memory + allocation_padding;

        uint8_t *d1 = (test_memory == 1) ? basic_resolve(at) : nullptr;
        if (allocated < size) {
            abort_with("invalid allocation data");
        }
        allocated -= size;
        if (at.address() == 0 || size == 0) {
            abort();
        }
        if (test_memory == 1 && d1[sz] != at.address() % 255) {
            abort();
        }
        if (test_memory == 1)
            d1[sz] = 0;
        page_modifications::inc_ticker(at.page());
        auto &t = retrieve_page(at.page());
        if (t.size == 0) {
            abort();
        }
        if (initialize_memory == 1) {
            //memset(d1, 0, size);
        }
        if (test_memory) {
            if (erased.count(at.address())) {
                throw std::runtime_error("memory erased");
            }
            erased.insert(at.address());
        }
        statistics::logical_allocated -= size;
        if (t.size == 1) {
            auto tp = at.page();

            if (is_free(tp)) {
                abort_with("freeing a free page");
            }
            if (last_page_allocated == tp) {
                last_page_allocated = 0;
            }
            t.size = 0;
            //t.modifications = 0;
            if (fragmentation < t.fragmentation) {
                abort_with("invalid fragmentation");
            }
            fragmentation -= t.fragmentation;
            if (!lru.empty())
                lru.erase(t.lru);
            t.clear();
            emancipated.erase(at.page());
            free_page(at.page());

            fragmented.erase(at.page());
            //free_pages.push_back(at.page());

            if (fragmentation < t.fragmentation) {
                abort_with("invalid fragmentation");
            }
        } else {
            emancipated.add(at, size); // add a free allocation for later re-use
            t.size--;
            if (t.fragmentation + size > t.write_position) {
                abort();
            }
            t.fragmentation += size;
            fragmentation += size;
            fragmented.insert(at.page());
        }
    }

    float fragmentation_ratio() const {
        return (float) fragmentation / (float(allocated) + 0.0001f);
    }

    // TODO: this function may cause to much latency when the arena is large
    // maybe just dont iterate through everything - it doesnt need to get
    // every page
    heap::vector<size_t> create_fragmentation_list(size_t max_pages) const {
        heap::vector<size_t> pages;
        if (fragmented.empty()) return {};
        for (auto page: fragmented) {
            pages.push_back(page);
            if (pages.size() >= max_pages) {
                return pages;
            }
        }
        return pages;
    }

    lru_list create_lru_list() {
        return lru;
    }

    std::pair<heap::buffer<uint8_t>, size_t> get_page_buffer(size_t at) {
        return get_page_buffer_inner(at);
    }

    std::pair<heap::buffer<uint8_t>, size_t> get_lru_page() {
        if (opt_enable_lru) {
            if (!lru.empty()) {
                return get_page_buffer_inner(*(--lru.end()));
            }
        }
        return {{}, 0};
    }

    template<typename T>
    T *read(logical_address at) const {
        if (at.null()) return nullptr;
        //std::lock_guard guard(mutex);
        static_assert(sizeof(T) < LPageSize);
        const uint8_t *d = const_cast<logical_allocator*>(this)->basic_resolve(at);
        return (T *) d;
    }

    template<typename T>
    T *modify(logical_address at) {
        if (at.null()) return nullptr;
        //std::lock_guard guard(mutex);
        static_assert(sizeof(T) < LPageSize);
        uint8_t *d = basic_resolve(at, true);
        return (T *) d;
    }

    logical_address new_address(size_t sz) {
        logical_address r{ap};
        new_address(r, sz);
        return r;
    }

    uint8_t *new_address(logical_address &r, size_t sz) {
        size_t size = sz + test_memory + allocation_padding;
        //std::lock_guard guard(mutex);
        r = emancipated.get(size);
        if (!r.null() && r.page() <= max_logical_address() && !retrieve_page(r.page()).empty()) {
            if (test_memory) {
                erased.erase(r.address());
            }
            auto &pcheck = retrieve_page(r.page());

            PageSizeType w = r.offset();
            if (w + size > pcheck.write_position) {
                pcheck.write_position = w + size;
            }
            std::pair<size_t, storage &> at = {r.page(), pcheck};

            // last_page_allocated should not be set here
            at.second.size++;
            //at.second.modifications++;
            if (at.second.fragmentation < size) {
                abort_with(" no fragmentation?");
            }
            at.second.fragmentation -= size;
            invalid(r);
            auto *data = test_memory == 1 ? basic_resolve(r) : nullptr;
            if (test_memory == 1 && data[sz] != 0) {
                abort();
            }
            if (test_memory == 1)
                data[sz] = r.address() % 255;

            allocated += size;
            statistics::logical_allocated += size;
            uint8_t *pd = get_alloc_page_data(r, sz);
            if (initialize_memory == 1) {
                memset(pd, 0, sz);
            }


            return pd;
        }
        auto at = create_if_required(size);
        if (is_null_base(at.first)) {
            abort();
        }
        if (at.second.write_position + size > LPageSize) {
            abort();
        }
        last_page_allocated = at.first;
        logical_address ca(at.first, at.second.write_position, ap);
        at.second.write_position += size;
        at.second.size++;

        if (test_memory) {
            invalid(ca);
            erased.erase(ca.address());
        }
        uint8_t *rd = get_alloc_page_data(ca, size);

        if (initialize_memory == 1) {
            memset(rd, 0, sz);
        }

        if (test_memory == 1)
            rd[sz] = ca.address() % 255;

        auto *data = (test_memory == 1) ? basic_resolve(ca) : nullptr;
        if (test_memory == 1 && data[sz] != ca.address() % 255) {
            abort();
        }

        allocated += size;
        statistics::logical_allocated += size;
        r = ca;

        return rd;
    }


    size_t full_vacuum() {

        if (!last_heap_bytes) last_heap_bytes = statistics::page_bytes_compressed;

        return 0;
    }

    void context_vacuum() {
    }


    size_t vacuum() const {

        return 0;
    }

    void iterate_pages(std::shared_mutex& latch, const std::function<bool(size_t, size_t, const heap::buffer<uint8_t> &)> &found_page) {
        opt_iterate_workers = art::get_iteration_worker_count();
        std::thread workers[opt_iterate_workers];
        std::atomic<bool> stop = false;
        for (unsigned iwork = 0; iwork < opt_iterate_workers; iwork++) {
            workers[iwork] = std::thread([this,&latch,iwork,&found_page,&stop]() {
                iterate_arena([&](size_t page, size_t &) -> void {
                    if (stop) return;
                    if (is_null_base(page)) return;
                    if (page % opt_iterate_workers == iwork) {
                        unsigned wp = 0;
                        heap::buffer<uint8_t> pdata; {
                            // copy under lock
                            std::lock_guard guard(latch);
                            if (!is_free(page)) {
                                wp = retrieve_page(page).write_position;
                                pdata = std::move(heap::buffer{get_page_data({page,0,this->ap}), wp});
                            }
                        }
                        if (wp) {
                            stop = !found_page(wp, page, pdata);
                            return;
                        }

                        auto pb = get_page_buffer(page);
                        stop = !found_page(pb.second, page, pb.first);
                    }
                });
            });
        }
        for (auto &worker: workers) worker.join();
    }

public:
    bool save_extra(const arena::hash_arena &copy, const std::string &filename,
                    const std::function<void(std::ostream &of)> &extra1) const {
        auto writer = [&](std::ostream &of) -> void {
            long ts = 0;
            writep(of, ts);

            writep(of, opt_enable_lru);
            writep(of, opt_validate_addresses);
            writep(of, opt_move_decompressed_pages);
            writep(of, opt_iterate_workers);

            writep(of, last_page_allocated);
            writep(of, highest_reserve_address);
            writep(of, last_heap_bytes);
            writep(of, ticker);
            writep(of, allocated);
            writep(of, fragmentation);
            extra1(of);
        };

        return copy.save(copy.name+filename, writer);
    }
    bool self_save_extra(const std::string &filename,
                    const std::function<void(std::ostream &of)> &extra1) const {

        return save_extra(main, filename, extra1);
    }

    bool send_extra(const arena::hash_arena &copy, std::ostream &out,
                    const std::function<void(std::ostream &of)> &extra1) const {
        auto writer = [&](std::ostream &of) -> void {
            long ts = 0;
            writep(of, ts);

            writep(of, opt_enable_lru);
            writep(of, opt_validate_addresses);
            writep(of, opt_move_decompressed_pages);
            writep(of, opt_iterate_workers);

            writep(of, last_page_allocated);
            writep(of, highest_reserve_address);
            writep(of, last_heap_bytes);
            writep(of, ticker);
            writep(of, allocated);
            writep(of, fragmentation);
            extra1(of);
        };

        return copy.send(out, writer);
    }

    [[nodiscard]] const arena::hash_arena &get_main() const {
        return main;
    }
    [[nodiscard]] arena::hash_arena &get_main() {
        return main;
    }

    bool load_extra(const std::string &filenname, const std::function<void(std::istream &of)> &extra1) {
        auto reader = [&](std::istream &in) -> void {
            long ts = 0;
            readp(in, ts);

            readp(in, opt_enable_lru);
            readp(in, opt_validate_addresses);
            readp(in, opt_move_decompressed_pages);
            readp(in, opt_iterate_workers);

            readp(in, last_page_allocated);
            readp(in, highest_reserve_address);
            readp(in, last_heap_bytes);
            readp(in, ticker);
            readp(in, allocated);
            readp(in, fragmentation);
            last_page_allocated = 0;
            extra1(in);
        };
        try {
            emancipated.clear();
            return main.load(main.name+filenname, reader);
        } catch (std::exception &e) {
            art::log(e,__FILE__,__LINE__);
            ++statistics::exceptions_raised;
        }

        return false;
    }

    void begin() {
        main.begin();
    }

    bool receive_extra(std::istream& in, const std::function<void(std::istream &of)> &extra1) {
        auto reader = [&](std::istream &in) -> void {
            long ts = 0;
            readp(in, ts);

            readp(in, opt_enable_lru);
            readp(in, opt_validate_addresses);
            readp(in, opt_move_decompressed_pages);
            readp(in, opt_iterate_workers);

            readp(in, last_page_allocated);
            readp(in, highest_reserve_address);
            readp(in, last_heap_bytes);
            readp(in, ticker);
            readp(in, allocated);
            readp(in, fragmentation);
            last_page_allocated = 0;
            extra1(in);
        };
        try {
            emancipated.clear();
            return main.receive(in, reader);
        } catch (std::exception &e) {
            art::log(e,__FILE__,__LINE__);
            ++statistics::exceptions_raised;
        }

        return false;
    }

    void commit() {
        main.commit();
    }

    void rollback() {
        main.rollback();
    }

    void clear() {
        main.rollback();
        main = arena::hash_arena{main.name};
        last_page_allocated = {0};
        highest_reserve_address = {nullptr};
        last_heap_bytes = 0;
        ticker = 1;
        allocated = 0;
        fragmentation = 0;

        last_vacuum_millis = std::chrono::high_resolution_clock::now();;
        emancipated = {ap};
        lru = {};
        fragmented = {};
        erased = {}; // for runtime use after free tests
        last_created_page = {};
        last_page_ptr = {};
    }

    void set_opt_enable_lru(bool enable_lru) {
        opt_enable_lru = enable_lru;
    }

    void set_opt_enable_lfu(bool enable_lfu) {
        opt_enable_lfu = enable_lfu;
    }

    void set_opt_use_vmm(bool use_vmm_mem) {
        main.set_opt_use_vmm(use_vmm_mem);
    }

    [[nodiscard]] size_t get_bytes_allocated() const {
        return main.get_bytes_allocated();
    }
};

struct alloc_pair {

    int sentinel = 1<<24;
    size_t shard{};
    std::shared_mutex latch{};

    logical_allocator nodes{this,"nodes"};
    logical_allocator leaves{this,"leaves"};
    explicit alloc_pair(size_t shard) : shard(shard), nodes(this,"nodes_"+std::to_string(shard)),leaves(this,"leaves_"+std::to_string(shard)) {}
    logical_allocator& get_nodes() {
        return nodes;
    };
    logical_allocator& get_leaves() {
        return leaves;
    }
    const logical_allocator& get_nodes() const {
        return nodes;
    };
    const logical_allocator& get_leaves() const {
        return leaves;
    }

};

#endif //COMPRESS_H
