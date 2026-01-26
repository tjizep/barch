//
// Created by linuxlite on 2/7/25.
//

#ifndef COMPRESS_H
#define COMPRESS_H
#include <iostream>
#include "sastam.h"
#include <ostream>
#include <statistics.h>
#include <stdexcept>
#include <thread>
#include <vector>
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
typedef heap::unordered_set<size_t> address_set;
enum {
    LPageSize = page_size - sizeof(storage)

};
struct alloc_pair;

struct size_offset {
    PageSizeType size{};
    PageSizeType offset{};
};

struct free_page {
    free_page() = delete;
    free_page& operator=(const free_page&) = default;
    free_page(const free_page&) = default;
    explicit free_page(logical_address p) : ap(&p.get_ap<abstract_alloc_pair>()), page(p.page()) {
    };
    abstract_alloc_pair *ap{nullptr};
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

    free_bin(unsigned size, abstract_leaf_pair* alloc) : alloc(alloc), size(size) {
    }
    free_bin(const free_bin& other) = default; //: alloc(other.alloc),size(other.size) {};
    free_bin& operator=(const free_bin& other) = default;

    abstract_leaf_pair * alloc = nullptr;
    unsigned size = 0;
    size_t bytes = 0;
    heap::vector<uint32_t> page_index{};
    heap::vector<free_page> pages{};

    struct page_max_t {
        size_t page{};
        size_t size{};
    };
    ~free_bin() {

    }
    void clear() {
        heap::vector<free_page> e{};
        pages.swap(e);
        heap::vector<uint32_t> pi;
        page_index.swap(pi);
    }
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

        return bytes == 0;
    }

    template<typename  TF>
    void get(size_t page, const TF &&f) {
        if (page < page_index.size()) {
            size_t at = page_index.at(page);
            if (at != 0) {
                auto &p = pages.at(at - 1);
                f(p);
            }
        }
    }
    template<typename  TF>
    void cget(size_t page, const TF &&f) const {
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
    void each(const std::function<void (size_t, uint32_t, uint32_t)>& f) const {
        for (auto& page: pages) {
            if (!page.empty()) {
                for (auto o: page.offsets) {
                    f(page.page, size, o);
                }
            }
        }
    }
    void each(size_t page, const std::function<void (size_t, uint32_t, uint32_t)>& f) const {
        cget(page, [&](const free_page &p) -> void {
            if (p.page != page) {
                abort_with("page does not match");
            }
            for (auto o: p.offsets) {
                f(p.page, size, o);
            }
        });

    }
    void get_addresses(heap::vector<size_t>& r,size_t page) {
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
        if (bytes < r) {
            abort_with("invalid free size");
        }

        bytes -= r;
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
        bytes += s;

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
        if (bytes < s) {
            abort_with("invalid free size");
        }
        bytes -= s;

        return address;
    }
};
struct free_list {
    abstract_leaf_pair * alloc = nullptr;
    uint64_t added_ = 0;
    size_t min_bin = LPageSize;
    size_t max_bin = 0;
    address_set addresses{};
    heap::std_vector<free_bin> free_bins{};
    heap::vector<size_t> tobe{};
    heap::unordered_set<size_t> available_bins{};
    //heap::vector<free_bin> free_bins{};
    free_list(free_list&& other) = delete;
    free_list &operator=(free_list&& other) = delete;
    free_list(const free_list& other) = default;
    free_list& operator=(const free_list& other) {
        if (this == &other) return *this;
        alloc = other.alloc;
        added_ = other.added_;
        min_bin = other.min_bin;
        max_bin = other.max_bin;
        available_bins = other.available_bins;
        addresses = other.addresses;
        free_bins = other.free_bins;
        return *this;
    }
    free_list(abstract_leaf_pair* alloc): alloc(alloc) {

    }
    [[nodiscard]] uint64_t get_added() const {
        return added_;
    }

    void remove_added(uint64_t by) {
        if (by > added_) {
            abort_with("invalid allocation size");
        }
        added_ -= by;
        statistics::bytes_in_free_lists -= by;
    }
    void add_added(uint64_t by) {
        added_ += by;
        statistics::bytes_in_free_lists += by;
    }
    void clear() {
        remove_added(get_added());
        addresses.clear();
        max_bin = 0;
        min_bin = LPageSize;
        available_bins.clear();
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

        if (fl_test_memory == 1) {
            if (addresses.count(address.address()) > 0) {
                abort_with("address already freed");
            }
            addresses.insert(address.address());
        }
        available_bins.insert(size);
        free_bins[size].add(address, size);
        min_bin = std::min(min_bin, (size_t) size);
        max_bin = std::max(max_bin, (size_t) size);

        add_added(size);
    }

    void add(logical_address address, unsigned size) {
        inner_add(address, size);
    }


    void erase(size_t page) {
        heap::vector<size_t> unavailer{};
        unavailer.clear();
        //for (size_t b = min_bin; b <= max_bin; ++b) {
        for (size_t b: available_bins){
            auto &f = free_bins[b];

            if (fl_test_memory == 1) {
                tobe.clear();
                f.get_addresses(tobe, page);
                for (auto o: tobe) {
                    if (addresses.count(o) == 0) {
                        abort_with("cannot erase pages which are allocated");
                    }
                    addresses.erase(o);
                }
            }
            unsigned r = f.erase(page);

            remove_added(r);

            if (f.empty()) {
                // opportunistic
                f.clear();
                unavailer.push_back(b);
            }
        }
        for (auto b: unavailer) {
            available_bins.erase(b);
        }
    }

    logical_address get(unsigned size) {
        if (size >= free_bins.size()) {
            return logical_address{0,alloc};
        }
        if (!get_added()) return logical_address{0,alloc};

        logical_address r = free_bins[size].pop(size);
        if (free_bins[size].empty()) {
            free_bins[size].clear();
            available_bins.erase(size);
        }
        if (!r.null()) {
            if (fl_test_memory == 1) {
                auto& tap = r.get_ap<abstract_leaf_pair>();
                if (&tap != this->alloc) {
                    abort_with("allocation pair from different tree");
                }
                if (addresses.count(r.address()) == 0) {
                    abort_with("memory test failed: no such free address");
                }

                addresses.erase(r.address());
            }
            remove_added(size);
        }

        return r;
    }
    void each(const std::function<void (size_t, uint32_t, uint32_t)>& f) const {
        for (size_t b: available_bins) {
            free_bins[b].each(f);
        }
    }
    void each(size_t page, const std::function<void (size_t, uint32_t, uint32_t)>& f) const {
        for (size_t b: available_bins) {
            free_bins[b].each(page, f);
        }
    }
};

// the clock allocator that logically skips every null-base pages
// it uses an accounting scheme to transact clock allocations without
// paying for them.
// further it handles fragmentation information and other allocation meta data for
// optimization space use.
// it also emits page modification notifications
struct logical_allocator {

    logical_allocator(abstract_leaf_pair* ap,std::string name): ap(ap), main(std::move(name)), emancipated(ap) {}

    logical_allocator(const logical_allocator &) = delete;

    ~logical_allocator() = default;

private:

    mutable abstract_leaf_pair * ap = nullptr;
    arena::hash_arena main;
    bool opt_page_trace = barch::get_log_page_access_trace();
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

    address_set fragmented{};
    address_set erased{}; // for runtime use after free tests
    size_t last_created_page{};
    uint8_t *last_page_ptr{};
    logical_allocator &operator=(const logical_allocator &t) = delete;

    // arena virtualization start
    storage &retrieve_page(size_t page, bool modify = false) {

        if (modify) {
            page_modifications::inc_ticker(page); // inform virtual pointers things changed here
            return *(storage*)main.get_page_data({page,LPageSize,ap}, modify);
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

    bool has_page(size_t page) const {
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


    std::pair<size_t, storage &> allocate_page_at(size_t at, size_t) {
        auto &page = retrieve_page(at);
        return {at, page};
    }
    // this function looks like it should be in another class - but it handles the allocation clock
    std::pair<size_t, storage &> alloc_with_clock(size_t ps) {
        auto at = allocate(); // tell the accountants you want budget to get a page
        if (is_null_base(at)) { // skip the null bases and make another allocation (that we don't pay for physically)
            at = allocate();
        }
        last_page_allocated = at;
        main.get_alloc_page_data({at,0,ap}, page_size); // tell the engineers to make a page at the specified position
        return allocate_page_at(at, ps);
    }

    std::pair<size_t, storage &> create_if_required(size_t size) {
        static_assert((size_t)LPageSize<(size_t)page_size);
        static_assert((size_t)LPageSize==(size_t)(page_size - sizeof(storage)));

        if (size > LPageSize) {
            return alloc_with_clock(size);
        }
        if (last_page_allocated == 0) {
            return alloc_with_clock(LPageSize);
        }

        auto &last = retrieve_page(last_page_allocated, true);
        if (last.empty()) {
            abort();
        }
        if (last.write_position + size >= LPageSize) {
            return alloc_with_clock(LPageSize);
        }
        return {last_page_allocated, last};
    }

    uint8_t *basic_resolve(logical_address at, bool modify = false) {
        if (opt_page_trace) {
            barch::std_log("page trace [", main.name, "]:", at.address(), at.page(), at.offset(), "for",modify ? "write":"read");
        }
        if (test_memory == 1) {
            if (!allocated) {
                barch::std_err("failure for",main.name,at.address(), at.page(), at.offset());
                abort_with("use after free - no data allocated ");
            }
            if (erased.contains(at.address())) {
                barch::std_err("failure for",main.name,at.address(), at.page(), at.offset());
                abort_with("use after free");
            }
        }
        if (at.null()) return nullptr;

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


    std::pair<heap::buffer<uint8_t>, size_t> get_page_buffer_inner(size_t at) const {
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
    void shrinkLast() {
        main.shrinkLast();
    }
    void free(logical_address at, size_t sz) {
        sz = pad(sz);
        size_t size = sz + test_memory;
        uint8_t *d1 = (test_memory == 1) ? basic_resolve(at) : nullptr;
        if (allocated < size) {
            barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
            abort_with("invalid allocation data");
        }
        if (this->opt_page_trace) {
            barch::std_log("freeing from [",main.name,"] size", size,"at",at.address(),"allocated",allocated);
        }

        allocated -= size;
        if (at.address() == 0 || size == 0) {
            barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
            abort_with("invalid address");
        }
        if (test_memory == 1 && d1[sz] != at.address() % 255) {
            barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
            abort_with("memory address check failure");
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
                barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
                abort_with("memory erased (double free)");
            }
            erased.insert(at.address());
        }
        statistics::logical_allocated -= size;
        if (t.size == 1) {
            auto tp = at.page();

            if (is_free(tp)) {
                barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
                abort_with("freeing a free page");
            }
            if (last_page_allocated == tp) {
                last_page_allocated = 0;
            }
            t.size = 0;
            //t.modifications = 0;
            if (fragmentation < t.fragmentation) {
                barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
                abort_with("invalid fragmentation");
            }
            fragmentation -= t.fragmentation;
            t.clear();
            if (test_memory == 1) {
                emancipated.each(at.page(),[&](size_t p, uint32_t unused(s), uint32_t o) {
                    logical_address la(p,o,ap);
                    erased.erase(la.address());
                });
            }
            emancipated.erase(at.page());
            free_page(at.page());

            fragmented.erase(at.page());
            //free_pages.push_back(at.page());

            if (fragmentation < t.fragmentation) {
                barch::std_log("failure for",main.name,at.address(), at.page(), at.offset());
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
        return (float) emancipated.get_added() / (float(allocated) + 0.0001f);
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
    float page_fragmentation() const {
        return main.fragmentation();
    }

    size_t get_page_count() const {
        return main.page_count();
    }

    size_t max_allocated_page_num() const {
        return main.max_allocated_page_num();
    }
    size_t get_max_accessible_page() const {
        return main.get_max_accessible_page();
    }
    bool is_page_allocated(size_t page) {
        return !main.is_free(page);
    }
    std::pair<heap::buffer<uint8_t>, size_t> get_page_buffer(size_t at) const {
        return get_page_buffer_inner(at);
    }

    std::pair<heap::buffer<uint8_t>, size_t> get_lru_page() {
        return {{}, 0};
    }

    template<typename T>
    T *read(logical_address at) const {
        if (at.null()) return nullptr;
        static_assert(sizeof(T) < LPageSize);
        const uint8_t *d = const_cast<logical_allocator*>(this)->basic_resolve(at);
        return (T *) d;
    }

    template<typename T>
    T *modify(logical_address at) {
        if (at.null()) return nullptr;
        static_assert(sizeof(T) < LPageSize);
        uint8_t *d = basic_resolve(at, true);
        return (T *) d;
    }

    logical_address new_address(size_t sz) {
        logical_address r{ap};
        new_address(r, sz);
        return r;
    }
    // this reduces the total amount pf different allocations sizes possible
    size_t pad(size_t size) const {

        return alloc_pad(size);
    }
    uint8_t *new_address(logical_address &r, size_t sz) {
        sz = pad(sz);
        size_t size = sz + test_memory;

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
            if (at.second.fragmentation < size) {barch::std_log("failure for",main.name,r.address(), r.page(), r.offset());
                abort_with(" no fragmentation?");
            }
            at.second.fragmentation -= size;
            invalid(r);

            auto *data = test_memory == 1 ? basic_resolve(r) : nullptr;
            if (test_memory == 1 && data[sz] != 0) {
                barch::std_log("failure for",main.name,r.address(), r.page(), r.offset());
                abort_with("address check failed");
            }
            if (test_memory == 1)
                data[sz] = r.address() % 255;

            allocated += size;
            statistics::logical_allocated += size;
            uint8_t *pd = get_alloc_page_data(r, sz);
            if (initialize_memory == 1) {
                memset(pd, 0, sz);
            }

            if (this->opt_page_trace) {
                barch::std_log("allocate size [",main.name,"]", size,"at",r.address(),"from freelist","allocated",allocated);
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

        allocated += size;
        auto *data = (test_memory == 1) ? basic_resolve(ca) : nullptr;
        if (test_memory == 1 && data[sz] != ca.address() % 255) {
            barch::std_log("failure for",main.name,r.address(), r.page(), r.offset());
            abort_with("memory address check failure");
        }


        statistics::logical_allocated += size;
        r = ca;
        if (this->opt_page_trace) {
            barch::std_log("allocate size [",main.name,"]", size,"at",r.address(),"as new","allocated",allocated);
        }

        return rd;
    }


    size_t full_vacuum() {

        if (!last_heap_bytes) last_heap_bytes = statistics::value_bytes_compressed;

        return 0;
    }

    void context_vacuum() {
    }


    size_t vacuum() const {

        return 0;
    }
    void iterate_pages(const std::function<void(size_t, size_t, const heap::buffer<uint8_t> &)> &found_page) const {
        heap::buffer<uint8_t> pdata;
        iterate_arena([&](size_t page, const size_t &) -> void {
            if (is_null_base(page)) return;
            size_t wp = 0;
            if (!is_free(page)) {
                wp = retrieve_page(page).write_position;
                pdata = heap::buffer{get_page_data({page,0,this->ap}), wp};
            }
            auto pb = get_page_buffer(page);
            found_page(pb.second, page, pb.first);
        });
    }
    void iterate_pages(heap::shared_mutex& latch, const std::function<bool(size_t, size_t, const heap::buffer<uint8_t> &)> &found_page) {
        opt_iterate_workers = barch::get_iteration_worker_count();
        std::vector<std::thread> workers{opt_iterate_workers};
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
                                pdata = heap::buffer{get_page_data({page,0,this->ap}), wp};
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
            bool opt_enable_lru = false;
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
            write_emancipated(of);
            extra1(of);
        };

        return copy.save(copy.name+filename, writer);
    }
    bool self_save_extra(const std::string &filename,
                    const std::function<void(std::ostream &of)> &extra1) const {

        return save_extra(main, filename, extra1);
    }
    bool delete_files(const std::string &filename) {
        std::string fname = main.name+filename;
        return std::remove(fname.c_str())==0;
    }
    void write_emancipated(std::ostream& of) const {
        uint64_t written = 0, skipped = 0;
        address_set duplicates;
        emancipated.each([&]( size_t page, uint32_t size, uint32_t offset ) {
            logical_address at{page,offset,emancipated.alloc};
            if (!duplicates.contains(at.address())) { // it seems some items are added multiple times
                writep(of, page);
                writep(of, offset);
                writep(of, size);
                ++written;
                duplicates.insert(at.address());
            }else {
                ++skipped;
            }
        });
        writep(of, (size_t)0);
        writep(of, (uint32_t)0);
        writep(of, (uint32_t)0);
        writep(of, written);
        if (skipped)
            barch::std_log("skipped duplicate entries",skipped);
    }
    void read_emancipated(std::istream& in) {
        size_t page;
        uint32_t sz;
        uint32_t o;
        uint64_t read = 0,written = 0;
        if (!erased.empty()) {
            barch::std_log("erased should be empty");
        }
        do {
            readp(in, page);
            readp(in, o);
            readp(in, sz);
            if (sz != 0) {
                ++read;
                logical_address at{page,o,emancipated.alloc};
                emancipated.add(at,sz);
                if (test_memory == 1) {
                    erased.insert(at);
                }
            }
        }while (sz != 0);
        readp(in, written);
        if (written != read) {
            abort_with("invalid data file");
        }
    }
    bool send_extra(const arena::hash_arena &copy, std::ostream &out,
                    const std::function<void(std::ostream &of)> &extra1) const {
        auto writer = [&](std::ostream &of) -> void {
            long ts = 0;
            writep(of, ts);
            bool opt_enable_lru = false;
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
            write_emancipated(of);
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
            bool opt_enable_lru = false;
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
            read_emancipated(in);
            extra1(in);
        };
        try {
            emancipated.clear();
            return main.load(main.name+filenname, reader);
        } catch (std::exception &e) {
            barch::log(e,__FILE__,__LINE__);
            ++statistics::exceptions_raised;
        }

        return false;
    }

    void begin() {
        main.begin();
    }

    bool receive_extra(std::istream& in, const std::function<void(std::istream &of)> &extra1) {
        auto reader = [&](std::istream &in1) -> void {
            long ts = 0;
            readp(in1, ts);
            bool opt_enable_lru = false;
            readp(in1, opt_enable_lru);
            readp(in1, opt_validate_addresses);
            readp(in1, opt_move_decompressed_pages);
            readp(in1, opt_iterate_workers);

            readp(in1, last_page_allocated);
            readp(in1, highest_reserve_address);
            readp(in1, last_heap_bytes);
            readp(in1, ticker);
            readp(in1, allocated);
            readp(in1, fragmentation);
            last_page_allocated = 0;
            extra1(in1);
        };
        try {
            emancipated.clear();
            return main.receive(in, reader);
        } catch (std::exception &e) {
            barch::log(e,__FILE__,__LINE__);
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
        highest_reserve_address = nullptr;
        last_heap_bytes = 0;
        ticker = 1;
        allocated = 0;
        fragmentation = 0;

        last_vacuum_millis = std::chrono::high_resolution_clock::now();;
        emancipated.clear();

        fragmented = {};
        erased = {}; // for runtime use after free tests
        last_created_page = {};
        last_page_ptr = {};
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
    [[nodiscard]] size_t get_bytes_in_free_list() const {
        return emancipated.get_added();
    }
};

struct alloc_pair : public abstract_leaf_pair{


    size_t shard_number{};
    heap::shared_mutex latch{};
    bool is_debug = false;
    std::string name{};

    logical_allocator nodes{this,"nodes"};
    logical_allocator leaves{this,"leaves"};
    explicit alloc_pair(size_t shard_number) : shard_number(shard_number), nodes(this,"nodes_"+std::to_string(shard_number)),leaves(this,"leaves_"+std::to_string(shard_number)) {}
    alloc_pair(size_t shard_number,const std::string& name) : shard_number(shard_number), name(name), nodes(this,"nodes_"+name+std::to_string(shard_number)),leaves(this,"leaves_"+name+std::to_string(shard_number)) {}
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

    void shrink() {
        leaves.shrinkLast();
        nodes.shrinkLast();
    }

    virtual ~alloc_pair() = default;

};

#endif //COMPRESS_H
