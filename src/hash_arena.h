//
// Created by linuxlite on 3/27/25.
//

#ifndef HASH_ARENA_H
#define HASH_ARENA_H
#include "storage.h"
#include "logical_address.h"
#include <fstream>
#include <page_modifications.h>
#include <ankerl/unordered_dense.h>
#include <sys/mman.h>
#include "configuration.h"
#include "logger.h"

namespace arena {
    struct base_hash_arena {
        bool opt_use_vmmap = art::get_use_vmm_memory();

    protected:
        typedef heap::allocator<std::pair<size_t, storage> > allocator_type;

        typedef ankerl::unordered_dense::map<
            size_t
            , storage
            , ankerl::unordered_dense::hash<size_t>
            , std::equal_to<size_t>
            , allocator_type> hash_type;

        hash_type hidden_arena{};
        heap::std_vector<size_t> free_address_list{};
        heap::std_vector<size_t> buffered_free{};
        size_t top = 100000000;
        size_t free_pages = top;
        size_t last_allocated = 0;
        size_t max_address_accessed = 0;
        base_hash_arena *source = nullptr;
        uint8_t *page_data{nullptr};
        size_t page_data_size{0};


        void recover_free(size_t at) {
            if (!is_free(at)) {
                throw std::runtime_error("page not free");
            }
            if (at > max_logical_address()) {
                free_pages += at - max_logical_address();
                top = at;
            }
            if (!has_free()) {
                throw std::runtime_error("no free pages available");
            }
            hidden_arena[at] = storage{};
            max_address_accessed = std::max(max_address_accessed, at);
            --free_pages;
        }

    public:
        base_hash_arena(const base_hash_arena &other) {
            *this = other;
        };

        base_hash_arena(base_hash_arena &&other) noexcept {
            *this = std::move(other);
        };

        base_hash_arena &operator=(base_hash_arena &&other) {
            if (this != &other) {
                this->clear();
                opt_use_vmmap = other.opt_use_vmmap;
                hidden_arena = std::move(other.hidden_arena);
                free_address_list = std::move(other.free_address_list);
                buffered_free = std::move(other.buffered_free);
                top = other.top;
                free_pages = other.free_pages;
                last_allocated = other.last_allocated;
                max_address_accessed = other.max_address_accessed;
                source = other.source;
                page_data = other.page_data;
                page_data_size = other.page_data_size;
                other.page_data = nullptr;
                other.page_data_size = 0;
                other.clear();
            }
            return *this;
        };

        base_hash_arena &operator=(const base_hash_arena &other) {
            if (this != &other) {
                this->clear();
                //opt_use_vmmap = other.opt_use_vmmap;
                alloc_page_data(other.page_data_size);
                if (other.page_data_size)
                    memcpy(page_data, other.page_data,
                           std::min(other.page_data_size, (other.get_max_address_accessed() + 1) * page_size));
                top = other.top;
                free_pages = other.free_pages;
                max_address_accessed = other.max_address_accessed;
                source = other.source;
                hidden_arena = other.hidden_arena;
            }
            return *this;
        };

        ~base_hash_arena() {
            if (page_data != nullptr) {
                if (art::get_use_vmm_memory()) {
                    munmap(page_data, page_data_size);
                } else {
                    free(page_data);
                    heap::allocated -= page_data_size;
                }
            }
        }

        void reallocate(bool use_vmm) {
            if (use_vmm == opt_use_vmmap) {
                return;
            }
            if (use_vmm) {
                opt_use_vmmap = true;
                if (page_data) {
                    size_t new_page_data_size = (max_address_accessed + 1) * page_size;
                    auto old_data = page_data;
                    auto old_page_data_size = page_data_size;
                    page_data = nullptr;
                    page_data_size = 0;
                    alloc_page_data(new_page_data_size);
                    memcpy(page_data, old_data, old_page_data_size);
                    free(old_data);
                    heap::allocated -= old_page_data_size;
                    art::std_log("reallocating [", old_page_data_size, "] physical page data, to [", page_data_size,
                                 "] virtual memory");
                }
            } else {
                if (page_data) {
                    size_t old_page_data_size = page_data_size;
                    size_t new_page_data_size = (max_address_accessed + 1) * page_size;
                    auto npd = (uint8_t *) realloc(nullptr, new_page_data_size);
                    memcpy(npd, page_data, new_page_data_size);
                    heap::allocated += new_page_data_size;

                    munmap(page_data, page_data_size);
                    page_data_size = new_page_data_size;
                    page_data = npd;
                    page_modifications::inc_all_tickers();
                    art::std_log("reallocating [", old_page_data_size, "] vmm page data, to [", new_page_data_size,
                                 "] physical memory");
                }
            }
        }

        void clear() {
            hidden_arena = hash_type{};
            free_address_list = heap::std_vector<size_t>{};
            buffered_free = heap::std_vector<size_t>{};
            top = 10000000;
            free_pages = top;
            last_allocated = 0;
            max_address_accessed = 0;
            source = nullptr;
            if (page_data != nullptr) {
                if (opt_use_vmmap) {
                    munmap(page_data, page_data_size);
                } else {
                    free(page_data);
                    heap::allocated -= page_data_size;
                }
            }

            page_data = nullptr;
            page_data_size = 0;
        }

        base_hash_arena() = default;

        // arena virtualization functions
        [[nodiscard]] size_t page_count_no_source() const {
            return hidden_arena.size();
        }

        [[nodiscard]] size_t page_count() const {
            if (source) {
                return source->page_count_no_source() + page_count_no_source();
            }
            return page_count_no_source();
        }

        [[nodiscard]] size_t max_logical_address() const {
            return top;
        }

        void free_page(size_t at) {
            if (hidden_arena.empty()) {
                throw std::runtime_error("no pages left to free");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                throw std::runtime_error("page already free");
            }

            hidden_arena.erase(pi);
            max_address_accessed = std::max(max_address_accessed, at);
            ++free_pages;
            free_address_list.emplace_back(at);
            if (source) {
                buffered_free.emplace_back(at);
            }
        }

        [[nodiscard]] bool is_free_no_source(size_t at) const {
            return !hidden_arena.contains(at);
        }

        [[nodiscard]] bool is_free(size_t at) const {
            return is_free_no_source(at);
        }

        [[nodiscard]] bool has_free_no_source() const {
            return free_pages > 1;
        }

        [[nodiscard]] bool has_free() const {
            //if (source && source->has_free_no_source()) return true;
            return has_free_no_source();
        }

        size_t allocate() {
            if (!has_free()) {
                throw std::runtime_error("no free pages available");
            }
            if (!free_address_list.empty()) {
                size_t at = free_address_list.back();
                free_address_list.pop_back();
                recover_free(at);
                return at;
            }
            if (last_allocated > 0) {
                if (is_free(last_allocated + 1)) {
                    ++last_allocated;
                    recover_free(last_allocated);
                    return last_allocated;
                }
            }
            for (size_t to = 1; to < top; ++to) {
                if (is_free(to) && !logical_address::is_null_base(to)) {
                    last_allocated = to;
                    recover_free(to);
                    return to;
                }
            }
            throw std::runtime_error("no free pages found");
        }

        [[nodiscard]] bool has_page_no_source(size_t at) const {
            return hidden_arena.contains(at);
        }

        [[nodiscard]] bool has_page(size_t at) const {
            if (source && source->has_page_no_source(at)) return true;
            return has_page_no_source(at);
        }

        storage &modify(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                if (source) {
                    page_modifications::inc_ticker(at);
                    hidden_arena[at] = source->read(at);
                    return hidden_arena[at];
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }

        [[nodiscard]] const storage &read(size_t at) const {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }

            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                if (source) {
                    return source->read(at);
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }

        [[nodiscard]] storage &read(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                if (source) {
                    return source->read_no_source(at);
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }

        [[nodiscard]] storage &read_no_source(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }


        void iterate_arena(const std::function<bool(size_t, storage &)> &iter) {
            if (source) {
                for (auto &[at,str]: source->hidden_arena) {
                    if (has_page_no_source(at)) continue;
                    if (!iter(at, str)) {
                        return;
                    }
                }
            }
            for (auto &[at,str]: hidden_arena) {
                if (!iter(at, str)) {
                    return;
                }
            }
        }

        void iterate_arena(const std::function<void(size_t, storage &)> &iter) {
            if (source) {
                source->iterate_arena([&](size_t page, storage &s) {
                    if (!has_page_no_source(page)) iter(page, s);
                });
            }
            for (auto &[at,str]: hidden_arena) {
                iter(at, str);
            }
        }

        void iterate_arena(const std::function<void(size_t, const storage &)> &iter) const {
            if (source) {
                source->iterate_arena([&](size_t page, const storage &s) {
                    if (!has_page_no_source(page)) iter(page, s);
                });
            }
            for (auto &[at,str]: hidden_arena) {
                iter(at, str);
            }
        }

        [[nodiscard]] size_t get_max_address_accessed() const {
            return max_address_accessed;
        }

        void set_source(base_hash_arena *src) {
            source = src;
            /**
                        free_address_list.clear();
                        top = 10000000;
                        free_pages = top;
                        last_allocated = 0;
                        max_address_accessed = 0;
            */
            buffered_free.clear();
            max_address_accessed = src->max_address_accessed;
            top = src->top;
            free_address_list = src->free_address_list;
            free_pages = src->free_pages;
            last_allocated = src->last_allocated;
        }

        [[nodiscard]] bool has_source() const {
            return source != nullptr;
        }

        void move_to_source() {
            if (source) {
                for (auto f: buffered_free) {
                    source->hidden_arena.erase(f);
                }
                for (auto &[at,str]: hidden_arena) {
                    page_modifications::inc_ticker(at);
                    source->hidden_arena[at] = std::move(str);
                }
                source->free_address_list = free_address_list;
                source->top = top;
                source->free_pages = free_pages;
                source->last_allocated = last_allocated;
                source->max_address_accessed = max_address_accessed;
                clear();
            }
        }

        void alloc_page_data(size_t new_size) {
            if (opt_use_vmmap) {
                if (page_data_size > 0) {
                    clear();
                    abort_with("out of virtual memory");
                }
                uint64_t toalloc = heap::get_physical_memory_bytes() * vmm_physical_factor / 100;
                page_data_size = toalloc;

                if (new_size > page_data_size) {
                    throw std::runtime_error("out of virtual memory");
                }
                page_data = (uint8_t *) mmap(nullptr, page_data_size, PROT_READ | PROT_WRITE,
                                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (page_data == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }
                page_modifications::inc_all_tickers();
                art::std_log("allocated", page_data_size, "virtual memory as page data");
            } else {
                heap::allocated -= page_data_size;
                auto old = page_data;
                page_data = (uint8_t *) realloc(page_data, new_size);
                if (!page_data) {
                    abort_with("out of memory");
                }
                page_data_size = new_size;

                if (old != page_data) {
                    page_modifications::inc_all_tickers();
                }
                heap::allocated += page_data_size;
            }
        }

        uint8_t *get_alloc_page_data(logical_address r, size_t size) {
            // page size must be a power of two
            size_t page_pos = r.page() * page_size;
            size_t offset = r.offset();
            if (page_data_size <= page_pos + offset + size) {
                alloc_page_data((r.page() + 1024) * page_size + size);
            }
            if (page_data_size < page_pos + offset + size) {
                abort_with("position not allocated");
            }
            return page_data + page_pos + r.offset();
        }

        [[nodiscard]] uint8_t *get_page_data(logical_address r) const {
            size_t page_pos = r.page() * page_size;
            size_t offset = r.offset();
            if (page_pos + offset > page_data_size) {
                abort_with("invalid page address");
            }
            return page_data + page_pos + offset;
        }

        bool save(const std::string &filename, const std::function<void(std::ofstream &)> &extra) const;

        bool load(const std::string &filename, const std::function<void(std::ifstream &)> &extra);

        static bool arena_read(base_hash_arena &arena, const std::function<void(std::ifstream &)> &extra,
                               const std::string &filename);
    };

    struct hash_arena {
        base_hash_arena main{};
        // arena virtualization functions
        [[nodiscard]] size_t page_count() const {
            return main.page_count();
        }

        [[nodiscard]] size_t max_logical_address() const {
            return main.max_logical_address();
        }

        void free_page(size_t at) {
            main.free_page(at);
        }

        [[nodiscard]] bool is_free(size_t at) const {
            return main.is_free(at);
        }

        [[nodiscard]] bool has_free() const {
            return main.has_free();
        }

        size_t allocate() {
            return main.allocate();
        }

        [[nodiscard]] bool has_page(size_t at) const {
            return main.has_page(at);
        }

        storage &modify(size_t at) {
            return main.modify(at);
        }

        [[nodiscard]] const storage &read(size_t at) const {
            return main.read(at);
        }

        [[nodiscard]] storage &read(size_t at) {
            return main.read(at);
        }

        [[nodiscard]] const storage &retrieve_page(size_t at) const {
            return main.read(at);
        }

        void iterate_arena(const std::function<bool(size_t, storage &)> &iter) {
            main.iterate_arena(iter);
        }

        void iterate_arena(const std::function<void(size_t, storage &)> &iter) {
            main.iterate_arena(iter);
        }

        void iterate_arena(const std::function<void(size_t, const storage &)> &iter) const {
            main.iterate_arena(iter);
        }

        [[nodiscard]] size_t get_max_address_accessed() const {
            return main.get_max_address_accessed();
        }

        void begin() {
        }

        void commit() {
        }

        void rollback() {
        }

        bool save(const std::string &filename, const std::function<void(std::ofstream &)> &extra) const {
            return main.save(filename, extra);
        };

        bool load(const std::string &filename, const std::function<void(std::ifstream &)> &extra) {
            return main.load(filename, extra);
        };

        uint8_t *get_alloc_page_data(logical_address r, size_t size) {
            return main.get_alloc_page_data(r, size);
        }

        [[nodiscard]] uint8_t *get_page_data(logical_address r) const {
            return main.get_page_data(r);
        }

        void set_opt_use_vmm(bool use_vmm) {
            main.reallocate(use_vmm);
        }

        [[nodiscard]] size_t get_bytes_allocated() const {
            if (main.opt_use_vmmap) {
                return (main.get_max_address_accessed() + 1) * page_size;
            }
            return 0;
        }
    };
}

#endif //HASH_ARENA_H
