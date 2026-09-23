//
// Created by linuxlite on 3/27/25.
//

#ifndef HASH_ARENA_H
#define HASH_ARENA_H
#include "logical_address.h"
#include <fstream>
#include <stdexcept>
#include <utility>
#include <page_modifications.h>
#include <unordered_set>
#include <ankerl/unordered_dense.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <atomic>
#include <memory>
#include <mutex>
#include "configuration.h"
#include "lzr_log.h"
#include "sastam.h"

namespace arena {
    /**
     * How many arenas mapped their pages back instead of loading them - TODO 262.
     * Counted rather than logged per arena: a default server is 694 of them, and a
     * line each is 694 lines into whatever start-up's stdout happens to be. When
     * that is a pipe nobody is draining yet, the writer blocks and the server never
     * finishes starting, which is exactly what happened.
     */
    std::atomic<uint64_t>& mapped_count();

    typedef std::unordered_set<size_t> address_set;
    typedef heap::allocator<std::pair<size_t, size_t> > allocator_type;
    typedef ankerl::unordered_dense::map<
            size_t
            , size_t
            , ankerl::unordered_dense::hash<size_t>
            , std::equal_to<size_t>
            , allocator_type> hash_type;
    /*
     * Page data is always mapped - TODO 347.
     *
     * There used to be a second mode where `page_data` came from malloc and
     * realloc, chosen by `use_vmm_memory`. It is gone, along with the setting:
     * the two modes shared every release path, so `clear()` had to know which
     * kind of block it held, `reallocate()` existed only to convert between
     * them, and both conversions called `free()` or `realloc()` on a pointer
     * that could have come from `mmap` once arena files arrived. Three of the
     * counter bugs in TODO 345 and 346 were in those branches, and none of them
     * was reachable in the mode anybody runs.
     */
    /**
     * Which pages the CoW map holds a copy of - TODO 416.
     *
     * It was a std::vector<bool>, written from the read path under a shared
     * latch. Two readers touching pages whose bits share a word lost each
     * other's stores, and the bit was set before the page was copied, so a
     * second reader could see it and read a page that was still zeros. Each
     * page gets its own atomic now, set only once the copy is done, and the
     * copy itself runs under `copying` so two first touches of one page
     * don't both write it.
     *
     * Resizing is only done by a writer - begin, or an allocation that grows
     * the CoW map - and a writer holds the shard latch exclusively, so no
     * reader is looking at the array while it is replaced.
     */
    struct cow_flags {
        cow_flags() = default;
        cow_flags(const cow_flags &other) { *this = other; }
        cow_flags(cow_flags &&other) noexcept { *this = std::move(other); }
        cow_flags &operator=(const cow_flags &other) {
            if (this == &other) return *this;
            clear();
            resize(other.n);
            for (size_t i = 0; i < n; ++i)
                if (other.test(i)) set(i);
            return *this;
        }
        cow_flags &operator=(cow_flags &&other) noexcept {
            if (this == &other) return *this;
            flags = std::move(other.flags);
            n = other.n;
            other.n = 0;
            return *this;
        }
        [[nodiscard]] size_t size() const { return n; }
        [[nodiscard]] bool test(size_t i) const {
            return flags[i].load(std::memory_order_acquire) != 0;
        }
        void set(size_t i) const { flags[i].store(1, std::memory_order_release); }
        /** keeps what is already set */
        void resize(size_t to) {
            if (to <= n) return;
            std::unique_ptr<std::atomic<uint8_t>[]> grown(new std::atomic<uint8_t>[to]);
            for (size_t i = 0; i < to; ++i)
                grown[i].store(i < n ? flags[i].load(std::memory_order_relaxed) : 0,
                               std::memory_order_relaxed);
            flags = std::move(grown);
            n = to;
        }
        void clear() {
            flags.reset();
            n = 0;
        }
        std::mutex &copying() const { return copy_mutex; }
    private:
        std::unique_ptr<std::atomic<uint8_t>[]> flags{};
        size_t n{0};
        mutable std::mutex copy_mutex{};
    };

    struct base_hash_arena {
    protected:

        hash_type hidden_arena{};
        address_set free_address_list{};
        heap::std_vector<size_t> buffered_free{};
        size_t top = max_top;
        size_t free_pages = top;
        size_t max_allocated_page = 0;
        size_t last_allocated = 0;
        uint8_t *page_data{nullptr};
        /*
         * Whether `page_data` is the arena file's mapping - TODO 345.
         *
         * `heap::named_vmm_allocated` has to move with the mapping it describes,
         * and `is_file_backed()` cannot decide that: it asks whether a backing
         * path exists, while `wants_backing()` re-reads `arena_map` from the live
         * configuration on every allocation. `CONFIG SET arena_map off` is
         * allowed at runtime, so an arena can hold a file mapping and then take
         * an anonymous branch, or the reverse. This says what was actually
         * mapped, set where the mapping is made and cleared where it is given up.
         */
        bool page_data_named{false};

        /**
         * Every change to what this arena holds goes through here - TODO 346.
         *
         * The three counters move together or they drift apart, and they had:
         * the realloc branch of `alloc_main` decremented `heap::allocated`
         * alone and then incremented both it and `heap::vmm_allocated`, so vmm
         * gained the old size on every grow, and `clear()` gave a malloc'd
         * block back to `allocated` only, so vmm kept it. One place to change
         * all three is the only way that stays true as paths are added.
         *
         * `delta` is signed because half the callers are giving memory back.
         * The named subtotal follows `page_data_named`, so set that flag before
         * calling when a mapping becomes the file's and clear it before calling
         * when it stops being - the overload taking `named` is for the CoW
         * mappings, which are always anonymous whatever `page_data` happens to
         * be.
         *
         * Worth knowing: `vmm_allocated` counts malloc'd page data too, because
         * the realloc path has always added it there. That is not changed here -
         * it is only made symmetric, so what goes in comes back out.
         */
        void update_usage_stats(int64_t delta, bool named) {
            if (delta >= 0) {
                const auto d = (uint64_t) delta;
                heap::allocated += d;
                heap::vmm_allocated += d;
                if (named)
                    heap::named_vmm_allocated += d;
            } else {
                const auto d = (uint64_t) -delta;
                heap::allocated -= d;
                heap::vmm_allocated -= d;
                if (named)
                    heap::named_vmm_allocated -= d;
            }
        }
        void update_usage_stats(int64_t delta) {
            update_usage_stats(delta, page_data_named);
        }

        size_t cow_size{0};
        mutable uint8_t *cow{nullptr};
        size_t page_data_size{0};
        mutable cow_flags modified{};
        /*
         * The page table as it stood at begin, put back by rollback - TODO 417.
         * Pages allocated or freed inside a transaction change these, and a
         * rollback that only dropped the CoW map left them changed: a page freed
         * in the transaction stayed free while the restored tree still used it.
         * page_data needs nothing, since nothing writes it until commit.
         */
        struct table_state {
            hash_type hidden_arena{};
            address_set free_address_list{};
            heap::std_vector<size_t> buffered_free{};
            size_t top{0};
            size_t free_pages{0};
            size_t max_allocated_page{0};
            size_t last_allocated{0};
        };
        std::unique_ptr<table_state> at_begin{};
        mutable size_t cow_alllocated{};
        bool borrowed{false};
        bool opt_check_mem = true;
        /*
         * Backing this arena's pages with a named file rather than anonymous memory
         * - TODO 239. `backing_name` is what the file is called; it is configuration
         * and survives a clear, so re-allocating maps the same file again. The fd is
         * state and does not: it is closed with the mapping.
         *
         * The file is where the pages live while running and is not read back
         * afterwards - the shard file is what an arena is rebuilt from, and it is
         * the one with a version, a completion stamp and an atomic rename behind it.
         * TODO 262 is what it would take to trust this one.
         */
        std::string backing_name{};
        std::string backing_path{};
        /** decorated space name, so this arena can ask what its own space wants */
        std::string backing_space{};

        void reconcile_free_list() {
            free_address_list.clear();
            if (empty()) return;
            for (size_t p = 1; p <= max_accessible_page(); ++p) {
                if (!hidden_arena.contains(p)) {
                    free_address_list.insert(p);
                }
            }
        }
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
            hidden_arena[at] = {};
            max_allocated_page = std::max<size_t>(at, max_allocated_page);
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
                hidden_arena = std::move(other.hidden_arena);
                free_address_list = std::move(other.free_address_list);
                buffered_free = std::move(other.buffered_free);
                top = other.top;
                free_pages = other.free_pages;
                max_allocated_page = other.max_allocated_page;
                last_allocated = other.last_allocated;
                page_data = other.page_data;
                page_data_size = other.page_data_size;
                page_data_named = other.page_data_named;
                modified = std::move(other.modified);
                cow = other.cow;
                cow_size = other.cow_size;
                cow_alllocated = other.cow_alllocated;
                backing_name = std::move(other.backing_name);
                backing_path = std::move(other.backing_path);
                other.backing_name.clear();
                other.backing_path.clear();
                other.page_data = nullptr;
                other.page_data_size = 0;
                other.page_data_named = false;

                other.clear();

            }
            return *this;
        };

        base_hash_arena &operator=(const base_hash_arena &other) {
            if (this != &other) {
                this->clear();
                alloc_page_data(other.page_data_size);
                if (other.page_data_size)
                    memcpy(page_data, other.page_data,other.page_data_size);
                if (other.cow_size) {
                    alloc_cow(other.cow_size);
                    memcpy(cow, other.cow, other.cow_size);
                    modified = other.modified;
                    cow_alllocated = other.cow_alllocated;
                }
                top = other.top;
                free_pages = other.free_pages;
                max_allocated_page = other.max_allocated_page;
                hidden_arena = other.hidden_arena;
                free_address_list = other.free_address_list;
            }
            return *this;
        };

        ~base_hash_arena() {
            clear();
        }
        size_t reusable_free_pages() const {
            return free_address_list.size();
        }
        float fragmentation_ratio() const {
            size_t hasize = hidden_arena.size();
            if (!hasize) return free_address_list.size();
            return (float) free_address_list.size() / (float)hasize;
        }
        bool empty() const {
            return page_data == nullptr;
        }
        size_t max_accessible_page() const {
            if (page_data_size % page_size != 0) {
                abort_with("page data size mismatch");
            }
            return (page_data_size / page_size) - 1;
        }
        // function attempts to pop the last page if possible
        heap::vector<size_t> pop_last() {

            heap::vector<size_t> r;

            if (!page_data_size) {
                return r;
            }

            size_t last_page = max_accessible_page();
            if (hidden_arena.contains(last_page)) {
                return r;
            }
            --last_page;
            if (free_address_list.contains(last_page)) {
                if (last_page * page_size > page_data_size) {
                    abort_with("invalid max address accessed");
                }
                auto new_size = page_data_size - physical_page_size;
                if (!new_size) {
                    return r;
                }

                page_data = (uint8_t*) mremap(page_data, page_data_size, new_size, MREMAP_MAYMOVE);
                if (page_data == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }

                update_usage_stats(-(int64_t) physical_page_size);
                page_data_size = new_size;
                page_modifications::inc_all_tickers();
                r.push_back(last_page);
                free_address_list.erase(last_page);
                last_page = max_accessible_page();
                ++statistics::vmm_pages_popped;
            }
            return r;
        }
        void clear() {
            drop_cow();
            at_begin.reset();
            hidden_arena = hash_type{};
            free_address_list = address_set{};
            buffered_free = heap::std_vector<size_t>{};
            top = max_top;
            free_pages = top;
            last_allocated = 0;
            if (!borrowed) {
                if (page_data != nullptr) {
                    munmap(page_data, page_data_size);
                    update_usage_stats(-(int64_t) page_data_size);
                }
            }
            borrowed = false;
            page_data = nullptr;
            page_data_size = 0;
            page_data_named = false;
            close_backing();
        }
        void borrow(base_hash_arena &other) {
            rollback();
            if (!other.cow) {
                abort_with("transaction not started. cow is missing");
            }
            hidden_arena = other.hidden_arena;
            free_address_list = other.free_address_list;
            buffered_free = other.buffered_free;
            top = other.top;
            free_pages = other.free_pages;
            last_allocated = other.last_allocated;
            page_data = other.page_data;
            page_data_size = other.page_data_size;
            // the lender keeps the accounting: clear() skips a borrowed mapping
            page_data_named = false;
            borrowed = true;
        }
        base_hash_arena() = default;

        /** what the mapping covers, resident or not */
        [[nodiscard]] size_t mapped_bytes() const {
            return page_data_size;
        }

        /** true once this arena's pages come from a file rather than anonymous memory */
        [[nodiscard]] bool is_file_backed() const {
            return !backing_path.empty();
        }

        [[nodiscard]] const std::string& backing_file() const {
            return backing_path;
        }

        /**
         * How much of this arena the kernel actually has in RAM, asked with
         * mincore() - TODO 340.
         *
         * For an anonymous arena this is nearly all of mapped_bytes(), and the
         * number is dull. For a file backed one the two come apart, which is the
         * whole point of mapping from a file: the kernel writes pages out and drops
         * them, so the arena is bounded by the device and this says how much of it
         * is costing RAM right now. That is the number a per space budget would have
         * to be chosen against, since the kernel offers no per mapping limit of its
         * own - RLIMIT_RSS has been a no-op since 2.4, no mmap flag or madvise op is
         * a cap, and a memory cgroup cannot be narrower than a thread group.
         *
         * What "resident" means here, because it is not the process's Rss and the
         * difference is the interesting part. mincore reports a page as resident if
         * referencing it would not cause a *disk* access, so for a shared file
         * mapping it counts pages sitting in the page cache even when they are not
         * mapped into this process. `/proc/<pid>/smaps` counts only the mapped ones,
         * so this number is the larger of the two. Measured: a 51MB file backed
         * space read 17.9MB here against 10.3MB of smaps Rss, and after the arena
         * files were fsync'd and dropped with FADV_DONTNEED both read 10.3MB - the
         * 7.6MB gap was exactly the cached-but-unmapped part.
         *
         * That makes this the right number for a budget. It is the RAM the arena
         * actually costs, cache the kernel holds on its behalf included, and the gap
         * to smaps Rss is how much of it can be given back for a minor fault instead
         * of a major one.
         *
         * Two things about the cost. mincore() answers a byte per system page, so it
         * is asked in chunks instead of with one buffer the size of the arena over
         * the page size. And it walks page tables, so this belongs in an explicit
         * report and not on a request path or in anything polled per second.
         *
         * Returns 0 when there is nothing to ask about, and also for an arena whose
         * pages came from realloc rather than mmap - that memory is mapped, but not
         * at a page boundary, and mincore refuses a misaligned address.
         */
        [[nodiscard]] size_t resident_bytes() const {
            if (page_data == nullptr || page_data_size == 0)
                return 0;
            const auto sys_page = (size_t) ::sysconf(_SC_PAGESIZE);
            if (sys_page == 0)
                return 0;
            if ((uintptr_t) page_data % sys_page != 0)
                return 0;
            unsigned char seen[4096];
            const size_t chunk = sizeof(seen) * sys_page;
            size_t resident = 0;
            for (size_t off = 0; off < page_data_size; off += chunk) {
                const size_t len = std::min(chunk, page_data_size - off);
                if (::mincore(page_data + off, len, seen) != 0)
                    return resident;            // say what was counted, not nothing
                const size_t pages = (len + sys_page - 1) / sys_page;
                for (size_t i = 0; i < pages; ++i) {
                    if (seen[i] & 1u)
                        resident += sys_page;
                }
            }
            return resident;
        }

        // arena virtualization functions
        [[nodiscard]] size_t page_count_no_source() const {
            return hidden_arena.size();
        }
        [[nodiscard]] size_t max_allocated_page_num() const {
            return max_allocated_page;
        }
        [[nodiscard]] size_t find_max_allocated_page_num() const {
            size_t pmax = 0;
            for (auto &[at,str]: hidden_arena) {
                pmax = std::max(pmax, at);
            }
            return pmax;
        }
        [[nodiscard]] size_t page_count() const {
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
            if (at == max_allocated_page) {
                max_allocated_page = find_max_allocated_page_num();
            }
            ++free_pages;
            free_address_list.insert(at);
        }

        [[nodiscard]] bool is_free_no_source(size_t at) const {
            bool contained = hidden_arena.contains(at);
            return !contained;
        }

        [[nodiscard]] bool is_free(size_t at) const {
            return is_free_no_source(at);
        }

        [[nodiscard]] bool has_free_no_source() const {
            return free_pages > 1;
        }

        [[nodiscard]] bool has_free() const {
            return has_free_no_source();
        }
        // creates allocation index entry but does not physically allocate space
        size_t allocate() {
            if (!has_free()) {
                throw std::runtime_error("no free pages available");
            }
            if (!free_address_list.empty()) {
                size_t at = *free_address_list.begin();
                free_address_list.erase(at);
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
            return has_page_no_source(at);
        }

        size_t &modify(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                barch::err({"page [",at,"] is missing"});
                abort_with("missing page");
            }

            return pi->second;
        }

        [[nodiscard]] const size_t &read(size_t at) const {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }

            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                barch::err({"page [",at,"] is missing"});
                abort_with("page not found");

            }
            return pi->second;
        }

        [[nodiscard]] size_t &read(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                barch::err({"page [",at,"] is missing"});
                abort_with("page not found");
            }
            return pi->second;
        }

        [[nodiscard]] size_t &read_no_source(size_t at) {
            if (at > top) {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end()) {
                barch::err({"page [",at,"] is missing"});
                abort_with("page not found");
            }
            return pi->second;
        }


        void iterate_arena(const std::function<bool(size_t, size_t &)> &iter) {
            for (auto &[at,str]: hidden_arena) {
                if (!iter(at, str)) {
                    return;
                }
            }
        }

        void iterate_arena(const std::function<void(size_t, size_t &)> &iter) {
            for (auto &[at,str]: hidden_arena) {
                iter(at, str);
            }
        }

        void iterate_arena(const hash_type& arena, const std::function<void(size_t,  size_t)> &iter) const {
            for (auto &[at,str]: arena) {
                iter(at, str);
            }
        }

        void iterate_arena(const std::function<void(size_t, const size_t &)> &iter) const {
            for (auto &[at,str]: hidden_arena) {
                if (free_address_list.contains(at)) {
                    abort_with("trying to access free page");
                }
                iter(at, str);
            }
        }
        [[nodiscard]] const hash_type &get_arena() const {
            return hidden_arena;
        }
        [[nodiscard]] size_t get_bytes_allocated() const {
            return page_data_size;
        }

        [[nodiscard]] uint64_t get_max_accessible_page() const {
            return max_accessible_page();
        }

        size_t first_page() const {
            return next_page(0);
        }

        size_t next_page(size_t start) const {
            if (hidden_arena.empty()) return 0;
            for (size_t to = start+1; to <= max_allocated_page; ++to) {
                if (!is_free(to) && !logical_address::is_null_base(to)) {
                    return to;
                }
            }
            return 0;
        }

        void set_source(base_hash_arena *) {
        }

        [[nodiscard]] bool has_source() const {
            return false;
        }

        void move_to_source() {
        }
        /** the file this arena maps, when it has one to map */
        bool wants_backing() const {
            if (backing_name.empty() || barch::get_arena_dir(backing_space).empty())
                return false;
            // matched on the name an arena already has - `leaves_<space><shard>`
            const auto which = barch::get_arena_map(backing_space);
            if (which == "all")
                return true;
            if (which == "off")
                return false;
            return backing_name.compare(0, which.size(), which) == 0;
        }

        /**
         * Create the file once, and after that only name it.
         *
         * The descriptor is not kept: an mmap holds its own reference, so the file
         * stays alive without one, and a default server is 347 shards times two
         * allocators - 696 open files before a single client connects, which a
         * 1024 descriptor limit does not survive. It is opened again for the moment
         * it takes to grow the file and closed again straight away.
         */
        bool prepare_backing(bool truncate = true) {
            if (!backing_path.empty())
                return true;
            auto dir = barch::get_arena_dir(backing_space);
            if (dir.empty() || backing_name.empty())
                return false;
            ::mkdir(dir.c_str(), 0755);                 // already there is fine
            std::string path = dir + "/" + backing_name + ".arena";
            /*
             * Truncated on creation. The file holds the pages of the process that is
             * running, not of the one that ran before - nothing reads it back, so
             * starting from what a previous run left would only mean mapping bytes
             * that are about to be overwritten. TODO 262 is what changes that.
             */
            int fd = ::open(path.c_str(), O_RDWR | O_CREAT | (truncate ? O_TRUNC : 0), 0644);
            if (fd < 0) {
                barch::err({"could not open arena file", path, strerror(errno)});
                return false;
            }
            ::close(fd);
            backing_path = path;
            return true;
        }

        /** grow the file to `size`; false says why in the log */
        bool size_backing(size_t size) {
            int fd = ::open(backing_path.c_str(), O_RDWR);
            if (fd < 0) {
                barch::err({"could not open arena file", backing_path, strerror(errno)});
                return false;
            }
            bool ok = ::ftruncate(fd, (off_t) size) == 0;
            if (!ok)
                barch::err({"could not size the arena file", backing_path, strerror(errno)});
            ::close(fd);
            return ok;
        }

        void close_backing() {
            backing_path.clear();
        }

        void alloc_cow(size_t new_size) {
            if (new_size < physical_page_size) {
                new_size = physical_page_size;
            }
            if (cow != nullptr) {
                cow = (uint8_t*) mremap(cow, cow_size, new_size, MREMAP_MAYMOVE);
                if (cow == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }
                if (new_size > cow_size) {
                    //memset(cow + cow_size, 0, new_size - cow_size);
                }
                update_usage_stats((int64_t) new_size - (int64_t) cow_size, false);
                cow_size = new_size;
            } else {
                cow = (uint8_t *) mmap(nullptr, new_size, PROT_READ | PROT_WRITE,
                                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (cow == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }
                //memset(cow, 0, new_size);
                update_usage_stats((int64_t) new_size, false);
                cow_size = new_size;
                barch::log({"allocated ", cow_size, "virtual memory as CoW"});
            }
            size_t cow_pages = cow_size / physical_page_size + 1;
            modified.resize(cow_pages);
            page_modifications::inc_all_tickers();
        }

        bool alloc_main(size_t new_size) {
            if (new_size < physical_page_size) {
                new_size = physical_page_size;
            }
            /*
             * File backed, when one was asked for. MAP_SHARED so the kernel writes
             * dirty pages out to that file and can drop them again - which is the
             * whole point: the arena is bounded by the device rather than by RAM
             * plus swap, and it works where swap is off. The file has to be grown
             * before the mapping is, or the pages past the old end have nothing
             * behind them.
             */
            if (wants_backing() && (page_data == nullptr || !backing_path.empty())) {
                if (!prepare_backing()) {
                    backing_name.clear();               // said why; carry on anonymously
                } else if (!size_backing(new_size)) {
                    return false;
                } else {
                    uint8_t* mapped;
                    if (page_data_size > 0) {
                        mapped = (uint8_t*) mremap(page_data, page_data_size, new_size,
                                                   MREMAP_MAYMOVE);
                    } else {
                        int fd = ::open(backing_path.c_str(), O_RDWR);
                        if (fd < 0)
                            abort_with("failed to open the arena file");
                        mapped = (uint8_t*) mmap(nullptr, new_size, PROT_READ | PROT_WRITE,
                                                 MAP_SHARED, fd, 0);
                        ::close(fd);                    // the mapping holds the file
                    }
                    if (mapped == MAP_FAILED) {
                        abort_with("failed to map the arena file");
                    }
                    const auto grew = (int64_t) new_size - (int64_t) page_data_size;
                    page_data = mapped;
                    page_data_size = new_size;
                    page_data_named = true;      // set first: the subtotal follows it
                    update_usage_stats(grew);    // the one place named mappings grow
                    page_modifications::inc_all_tickers();
                    return true;
                }
            }
            if (page_data_size > 0) {
                page_data = (uint8_t*) mremap(page_data, page_data_size, new_size, MREMAP_MAYMOVE);
                if (page_data == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }
                if (new_size > page_data_size) {
                    //memset(page_data + page_data_size, 0, new_size - page_data_size);
                }
                // mremap on a file mapping leaves it file backed, and this
                // branch is reachable with one if arena_map was turned off
                // after the mapping was made - TODO 345
                update_usage_stats((int64_t) new_size - (int64_t) page_data_size);
                page_data_size = new_size;
                page_modifications::inc_all_tickers();
            } else {
                page_data = (uint8_t *) mmap(nullptr, new_size, PROT_READ | PROT_WRITE,
                                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (page_data == MAP_FAILED) {
                    abort_with("failed to allocate virtual page data");
                }
                //memset(page_data, 0, new_size);
                page_data_named = false;
                update_usage_stats((int64_t) new_size);
                page_data_size = new_size;
                page_modifications::inc_all_tickers();
                //art::log({"allocated", page_data_size, "virtual memory as page data"});

            }

            return false;
        }

        void alloc_page_data(size_t new_size) {
            if (new_size < page_data_size) {} // ????
            if (cow != nullptr) {
                alloc_cow(new_size);
                return;
            }
            alloc_main(new_size);

        }
        /**
         * A page inside a transaction - TODO 416.
         *
         * A write copies the page into the CoW map the first time and works on
         * the copy from then on. A read of a page nobody has written yet reads
         * `page_data` where it is, rather than copying it: copying on every read
         * doubled the memory of whatever got read during a transaction, and a
         * commit then copied all of it back unchanged. Once a page is in the
         * CoW map, reads go there too, and the ticker bump on the copy is what
         * sends a reader that cached the `page_data` pointer to the copy.
         */
        uint8_t * get_cow_page(size_t page, size_t offset, bool modify) const {
            size_t page_pos = page * physical_page_size;
            if (page_pos + offset > cow_size) {
                abort_with("invalid CoW page address");
            }
            if (modified.size() <= page) {
                abort_with("invalid modified page address");
            }
            if (!modified.test(page)) {
                if (!modify && page_pos + physical_page_size <= page_data_size) {
                    return page_data + page_pos + offset;
                }
                std::lock_guard<std::mutex> g(modified.copying());
                if (!modified.test(page)) {
                    // the whole page when it lies inside page_data, the part
                    // that does when it's the partial last one. `<` used to
                    // leave out a page ending exactly at page_data_size
                    if (page_pos < page_data_size) {
                        size_t n = std::min<size_t>(physical_page_size, page_data_size - page_pos);
                        memcpy(cow + page_pos, page_data + page_pos, n);
                    }
                    modified.set(page);
                    page_modifications::inc_ticker(page);
                }
            }

            return cow + page_pos + offset;
        }
        uint8_t *get_alloc_page_data(logical_address r, size_t size) {
            // page size must be a power of two

            size_t page_pos = r.page() * physical_page_size;
            size_t offset = r.offset();
            if (cow == nullptr && cow_size > 0) {
                abort_with("invalid CoW page data");
            }
            if (std::max(page_data_size, cow_size) <= page_pos + offset + size) {
                alloc_page_data((r.page() + page_extension_on_allocation) * physical_page_size + size);
            }
            if (std::max(page_data_size, cow_size) < page_pos + offset + size) {
                abort_with("position not allocated");
            }
            if (cow != nullptr) {
                return get_cow_page(r.page(), r.offset(), true);
            }


            return page_data + page_pos + r.offset();
        }
        [[nodiscard]] bool is_from(const uint8_t* add) const {
            return add > page_data && add < (page_data+page_data_size);
        }
        [[nodiscard]] uint8_t *get_page_data(logical_address r, bool modify) const {
            size_t page_pos = r.page() * physical_page_size;
            size_t offset = r.offset();
            // these two validate an address that can have come out of a file, so they
            // are reporting bad input rather than a broken invariant, and they throw
            // instead of aborting. shard::load already wraps _load in a try/catch that
            // logs "could not load", which is where a bad file is meant to end up;
            // abort() walked straight past it. Inside the valkey module it was worse
            // than a crash - several loader threads abort at once, valkey's
            // sigsegvHandler takes a global lock and then joins the main thread, which
            // is itself waiting for that lock, and the process hangs at zero cpu
            // instead of dying. See TODO 42
            if (cow == nullptr && cow_size > 0) {
                throw_exception<std::runtime_error>("invalid CoW page data");
            }
            if (page_pos + offset > std::max(cow_size, page_data_size)) {
                throw_exception<std::runtime_error>("invalid page address");
            }
            if (cow != nullptr) {
                return get_cow_page(r.page(), r.offset(), modify);
            }

            return page_data + page_pos + offset;
        }
        void begin() {
            drop_cow();
            alloc_cow(page_data_size);
            at_begin = std::make_unique<table_state>(table_state{
                hidden_arena, free_address_list, buffered_free,
                top, free_pages, max_allocated_page, last_allocated});
        }

        void commit() {
            at_begin.reset();
            if (cow == nullptr) {
                return;
            }
            alloc_main(cow_size);
            for (size_t i = 0; i < modified.size(); i++) {
                if (modified.test(i)) {
                    memcpy(page_data + physical_page_size * i,cow + physical_page_size * i, physical_page_size);
                    page_modifications::inc_ticker(i);
                }
            }
            drop_cow();
        }

        /** back to begin: the CoW pages go, and the page table is put back */
        void rollback() {
            drop_cow();
            if (!at_begin)
                return;
            hidden_arena = std::move(at_begin->hidden_arena);
            free_address_list = std::move(at_begin->free_address_list);
            buffered_free = std::move(at_begin->buffered_free);
            top = at_begin->top;
            free_pages = at_begin->free_pages;
            max_allocated_page = at_begin->max_allocated_page;
            last_allocated = at_begin->last_allocated;
            at_begin.reset();
        }

        void drop_cow() {
            modified.clear();
            if (cow) {
                munmap(cow, cow_size);
                update_usage_stats(-(int64_t) cow_size, false);
                // anything that cached a pointer into the CoW map has to look
                // again, or it reads memory that is no longer mapped. commit
                // bumped the pages it copied back, but a rollback copies
                // nothing and bumped nothing - TODO 416
                page_modifications::inc_all_tickers();
            }
            cow = nullptr;
            cow_size = 0;
        }
        /** name the file this arena maps, when `arena_dir` says to map one */
        void set_backing_name(const std::string& name) {
            backing_name = name;
        }
        /**
         * The space this arena belongs to, decorated - `node`, `auth`, `shop_`. Only
         * used to ask whether that space has its own `arena_dir`/`arena_map`; empty
         * means the global answer, which is what an arena with no space gets. TODO 268.
         */
        void set_backing_space(const std::string& space) {
            backing_space = space;
        }
        const std::string& get_backing_space() const {
            return backing_space;
        }
        const std::string& get_backing_name() const {
            return backing_name;
        }
        void close_backing_file() {
            close_backing();
        }
        const std::string& get_backing_path() const {
            return backing_path;
        }
        void set_check_mem(bool check) {
            opt_check_mem = check;
        }
        bool is_check_mem() const {
            return opt_check_mem;
        }
        bool save(const std::string &filename, const std::function<void(std::ostream &)> &extra) const;

        /**
         * The snapshot beside a mapped arena - TODO 262.
         *
         * The pages are already in the `.arena` file; what a load also needs is the
         * metadata that says which of them are live - `hidden_arena`, `top`,
         * `last_allocated` - and the allocator state the shard file carries in its
         * `extra` block. So this is the save file minus the page data, written to
         * `<arena>.arena.meta`.
         *
         * Written only during an orderly shutdown, because that is the only moment
         * it can be true: the mapping goes on changing after any other save, and a
         * snapshot describing a state the file has moved past is worse than none.
         * Read once and unlinked, so a crash after start-up leaves nothing to trust.
         */
        bool save_snapshot(const std::function<void(std::ostream &)> &extra) const;
        bool load_snapshot(const std::function<void(std::istream &)> &extra);

        bool load(const std::string &filename, const std::function<void(std::istream &)> &extra);

        bool retrieve(std::istream& in, const std::function<void(std::istream &)> &extra);

        bool send(std::ostream &out, const std::function<void(std::ostream &)> &extra, bool write_version) const ;

        /**
         * Everything send() writes except the page bytes - TODO 416. The version,
         * the arena header, `middle`, the page count, and each page's record header
         * (page, fragmentation, size, ticker, write position) in page order. A page
         * walk has the bytes, so the two together are the file.
         */
        void write_layout(std::ostream &out, const std::function<void(std::ostream &)> &middle) const;

        /** the arena's header fields, the ones after the version in send() */
        void write_header(std::ostream &out) const;

        /**
         * Read one arena of a streamed shard - TODO 418. The header, `extra` (the
         * allocator's state, and the shard's stats for the leaf arena), the page
         * count, then per page its storage fields and only the bytes up to its
         * write position. The page tail is rebuilt from the storage fields, which
         * the file format carries as well but reads back from the page instead.
         * Replaces this arena only when the whole section reads.
         */
        bool stream_load(std::istream &in, const std::function<void(std::istream &)> &extra);
        static bool stream_retrieve(base_hash_arena &arena, std::istream &in,
                                    const std::function<void(std::istream &)> &extra);

        static bool arena_read(base_hash_arena &arena, const std::function<void(std::istream &)> &extra,
                               const std::string &filename);
        static bool arena_retrieve(base_hash_arena &arena, std::istream& in, const std::function<void(std::istream &)> &exre);
        //static bool arena_send(base_hash_arena &arena, std::istream& in);
    };

    struct hash_arena {
        std::string name;
        base_hash_arena main{};
        hash_arena(const hash_arena &) = default;
        hash_arena& operator=(const hash_arena &) = default;
        explicit hash_arena(std::string name, std::string space = {}) : name(std::move(name)) {
            // the arena's own name is what its file is called, and it is already
            // unique per space and shard - `nodes_<space><shard>` - TODO 239
            main.set_backing_name(this->name);
            main.set_backing_space(space);
        }
        // arena virtualization functions

        bool save_snapshot(const std::function<void(std::ostream &)> &extra) const {
            return main.save_snapshot(extra);
        }
        bool load_snapshot(const std::function<void(std::istream &)> &extra) {
            return main.load_snapshot(extra);
        }
        [[nodiscard]] float fragmentation() const {
            return main.fragmentation_ratio();
        }
        [[nodiscard]] size_t page_count() const {
            return main.page_count();
        }
        /* address space and RAM this arena costs - see base_hash_arena, TODO 340 */
        [[nodiscard]] size_t mapped_bytes() const {
            return main.mapped_bytes();
        }
        [[nodiscard]] size_t resident_bytes() const {
            return main.resident_bytes();
        }
        [[nodiscard]] bool is_file_backed() const {
            return main.is_file_backed();
        }
        [[nodiscard]] size_t max_allocated_page_num() const {
            return main.max_allocated_page_num();
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
        size_t reusable_free_pages() const {
            return main.reusable_free_pages();
        }
        heap::vector<size_t> shrinkLast() {
            return main.pop_last();
        }
        size_t allocate() {
            return main.allocate();
        }

        [[nodiscard]] bool has_page(size_t at) const {
            return main.has_page(at);
        }

        size_t &modify(size_t at) {
            return main.modify(at);
        }

        [[nodiscard]] const size_t &read(size_t at) const {
            return main.read(at);
        }

        [[nodiscard]] size_t &read(size_t at) {
            return main.read(at);
        }

        [[nodiscard]] const size_t &retrieve_page(size_t at) const {
            return main.read(at);
        }

        void iterate_arena(const std::function<bool(size_t, size_t &)> &iter) {
            main.iterate_arena(iter);
        }

        void iterate_arena(const hash_type& arena, const std::function<void(size_t, size_t)> &iter) const {
            main.iterate_arena(arena, iter);
        }

        void iterate_arena(const std::function<void(size_t, size_t &)> &iter) {
            main.iterate_arena(iter);
        }
        const hash_type& get_arena() const {
            return main.get_arena();
        }

        void iterate_arena(const std::function<void(size_t, const size_t &)> &iter) const {
            main.iterate_arena(iter);
        }

        size_t first_page() const {
            return main.first_page();
        }

        size_t next_page(size_t start) const {
            return main.next_page(start);
        }

        [[nodiscard]] uint64_t get_max_accessible_page() const {
            return main.get_max_accessible_page();
        }

        void begin() {
            main.begin();
        }

        void commit() {
            main.commit();
        }

        void rollback() {
            main.rollback();
        }

        void borrow(hash_arena &other) {
            main.borrow(other.main);
        }

        bool save(const std::string &filename, const std::function<void(std::ostream &)> &extra) const {
            return main.save(filename, extra);
        };
        void write_layout(std::ostream& out, const std::function<void(std::ostream &)> &middle) const {
            main.write_layout(out, middle);
        }
        void write_header(std::ostream& out) const {
            main.write_header(out);
        }
        bool stream_load(std::istream& in, const std::function<void(std::istream &)> &extra) {
            return main.stream_load(in, extra);
        }
        bool send(std::ostream& out, const std::function<void(std::ostream &)> &extra) const {
            return main.send(out, extra, true);
        };

        bool load(const std::string &filename, const std::function<void(std::istream &)> &extra) {
            return main.load(filename, extra);
        };

        bool receive(std::istream& in, const std::function<void(std::istream &)> &extra) {
            return main.retrieve(in, extra);
        };

        uint8_t *get_alloc_page_data(logical_address r, size_t size) {
            return main.get_alloc_page_data(r, size);
        }

        [[nodiscard]] uint8_t *get_page_data(logical_address r, bool modify) const {
            return main.get_page_data(r, modify);
        }

        void set_check_mem(bool check) {
            main.set_check_mem(check);
        }
        [[nodiscard]] size_t get_bytes_allocated() const {
            return main.get_bytes_allocated();
        }
        bool empty() const {
            return main.empty();
        }
        [[nodiscard]] bool is_from(const uint8_t* add) const {
            return main.is_from(add);
        }
    };
}

#endif //HASH_ARENA_H
