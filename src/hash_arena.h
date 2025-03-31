//
// Created by linuxlite on 3/27/25.
//

#ifndef HASH_ARENA_H
#define HASH_ARENA_H
#include "storage.h"
#include "compressed_address.h"
#include <fstream>
#include <ankerl/unordered_dense.h>
namespace arena {
    struct base_hash_arena
    {
        enum
        {
            cache_size = 1024
        };

    protected:
        // ankerl hash seems to make a small difference
        typedef heap::allocator<std::pair<size_t,storage>> allocator_type;
        //typedef std::allocator<std::pair<size_t,storage>> allocator_type;

        typedef ankerl::unordered_dense::map<
            size_t
        ,   storage
        ,   ankerl::unordered_dense::hash<size_t>
        ,   std::equal_to<size_t>
        ,   allocator_type> hash_type ;

        //typedef std::unordered_map<size_t, storage> hash_type ;
        hash_type hidden_arena{};
        heap::vector<size_t> free_address_list{};
        size_t top = 10000000;
        size_t free_pages = top;
        size_t last_allocated = 0;
        size_t max_address_accessed = 0;
        base_hash_arena* source = nullptr;

        void recover_free(size_t at)
        {
            if (!is_free(at))
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
            max_address_accessed = std::max(max_address_accessed, at);
            --free_pages;
        }

    public:
        base_hash_arena(const arena::base_hash_arena&) = default;
        base_hash_arena& operator=(const arena::base_hash_arena&) = default;
        void clear() {
            hidden_arena.clear();
            free_address_list.clear();
            top = 10000000;
            free_pages = top;
            last_allocated = 0;
            max_address_accessed = 0;
            source = nullptr;

        }
        base_hash_arena() = default;
        // arena virtualization functions
        [[nodiscard]] size_t page_count_no_source() const
        {
            return hidden_arena.size();
        }
        [[nodiscard]] size_t page_count() const
        {
            if (source) {
                return source->page_count_no_source() + page_count_no_source();
            }
            return page_count_no_source();
        }

        [[nodiscard]] size_t max_logical_address() const
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
            max_address_accessed = std::max(max_address_accessed, at);
            ++free_pages;
            free_address_list.emplace_back(at);
        }
        [[nodiscard]] bool is_free_no_source(size_t at) const
        {
            return !hidden_arena.contains(at);
        }

        [[nodiscard]] bool is_free(size_t at) const
        {
           return is_free_no_source(at);
        }

        [[nodiscard]] bool has_free_no_source() const
        {
            return free_pages > 1;
        }
        [[nodiscard]] bool has_free() const
        {
            //if (source && source->has_free_no_source()) return true;
            return has_free_no_source();
        }

        size_t allocate()
        {
            if (!has_free())
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
                if (is_free(last_allocated + 1))
                {
                    ++last_allocated;
                    recover_free(last_allocated);
                    return last_allocated;
                }
            }
            for (size_t to = 1; to < top; ++to)
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
        [[nodiscard]] bool has_page_no_source(size_t at) const
        {
            return hidden_arena.contains(at);
        }

        [[nodiscard]] bool has_page(size_t at) const
        {
            if (source && source->has_page_no_source(at)) return true;
            return has_page_no_source(at);
        }

        storage& modify(size_t at)
        {
            if (at > top)
            {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end())
            {
                if (source) {
                    hidden_arena[at] = source->read(at);
                    return hidden_arena[at];
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }
        [[nodiscard]] const storage& read(size_t at) const
        {
            if (at > top)
            {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end())
            {
                if (source) {
                    return source->read(at);
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }
        [[nodiscard]] storage& read(size_t at)
        {
            if (at > top)
            {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end())
            {
                if (source) {
                    return source->read_no_source(at);
                }
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }
        [[nodiscard]] storage& read_no_source(size_t at)
        {
            if (at > top)
            {
                throw std::runtime_error("invalid page");
            }
            auto pi = hidden_arena.find(at);
            if (pi == hidden_arena.end())
            {
                throw std::runtime_error("missing page");
            }
            return pi->second;
        }


        void iterate_arena(const std::function<bool(size_t, storage&)>& iter)
        {
            if (source) {
                for (auto& [at,str] : source->hidden_arena)
                {
                    if (has_page_no_source(at)) continue;
                    if (!iter(at, str))
                    {
                        return;
                    }
                }
            }
            for (auto& [at,str] : hidden_arena)
            {
                if (!iter(at, str))
                {
                    return;
                }
            }

        }

        void iterate_arena(const std::function<void(size_t, storage&)>& iter)
        {
            if (source) {
                source->iterate_arena([&](size_t page, storage& s) {
                    if (!has_page_no_source(page)) iter(page, s);
                });
            }
            for (auto& [at,str] : hidden_arena)
            {
                iter(at, str);
            }
        }

        void iterate_arena(const std::function<void(size_t, const storage&) >& iter) const
        {
            if (source) {
                source->iterate_arena([&](size_t page, const storage& s) {
                    if (!has_page_no_source(page)) iter(page, s);
                });
            }
            for (auto& [at,str] : hidden_arena)
            {
                iter(at, str);
            }
        }

        [[nodiscard]] size_t get_max_address_accessed() const
        {
            return max_address_accessed;
        }
        void set_source(base_hash_arena* src) {
            source = src;
/**
            free_address_list.clear();
            top = 10000000;
            free_pages = top;
            last_allocated = 0;
            max_address_accessed = 0;
*/
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
                for (auto& [at,str] : hidden_arena)
                {
                    source->hidden_arena[at] = std::move(str);
                }
                source->free_address_list = free_address_list;
                source->top = top;
                source->free_pages = free_pages;
                source->last_allocated= last_allocated;
                source->max_address_accessed = max_address_accessed;
                clear();
            }

        }
        bool save(const std::string& filename, const std::function<void(std::ofstream&)>& extra) const;
        bool load(const std::string& filename, const std::function<void(std::ifstream&)>& extra);
        static bool arena_read(base_hash_arena& arena, const std::function<void(std::ifstream&)>& extra, const std::string& filename) ;
    };

    struct hash_arena {
        base_hash_arena buffer {};
        base_hash_arena main {};
        // arena virtualization functions
        [[nodiscard]] size_t page_count() const
        {
            if (buffer.has_source()) return buffer.page_count();
            return main.page_count();
        }

        [[nodiscard]] size_t max_logical_address() const
        {
            if (buffer.has_source()) return buffer.max_logical_address();
            return main.max_logical_address();
        }

        void free_page(size_t at)
        {
            if (buffer.has_source()) return buffer.free_page(at);
            main.free_page(at);
        }

        [[nodiscard]] bool is_free(size_t at) const
        {
            if (buffer.has_source()) return buffer.is_free(at);
            return main.is_free(at);
        }

        [[nodiscard]] bool has_free() const
        {
            if (buffer.has_source()) return buffer.has_free();
            return main.has_free();
        }

        size_t allocate()
        {
            if (buffer.has_source()) return buffer.allocate();
            return main.allocate();
        }

        [[nodiscard]] bool has_page(size_t at) const
        {
            if (buffer.has_source()) return buffer.has_page(at);
            return main.has_page(at);
        }
        storage& modify(size_t at)
        {
            if (buffer.has_source()) return buffer.modify(at);
            return main.modify(at);
        }
        [[nodiscard]] const storage& read(size_t at) const
        {
            if (buffer.has_source()) return buffer.read(at);
            return main.read(at);
        }
        [[nodiscard]] storage& read(size_t at)
        {
            if (buffer.has_source()) return buffer.read(at);
            return main.read(at);
        }

        [[nodiscard]] const storage& retrieve_page(size_t at) const
        {
            if (buffer.has_source()) return buffer.read(at);
            return main.read(at);
        }

        void iterate_arena(const std::function<bool(size_t, storage&)>& iter)
        {
            if (buffer.has_source()) return buffer.iterate_arena(iter);
            main.iterate_arena(iter);
        }

        void iterate_arena(const std::function<void(size_t, storage&)>& iter)
        {
            if (buffer.has_source()) return buffer.iterate_arena(iter);
            main.iterate_arena(iter);
        }

        void iterate_arena(const std::function<void(size_t, const storage&) >& iter) const
        {
            if (buffer.has_source()) return buffer.iterate_arena(iter);
            main.iterate_arena(iter);
        }

        [[nodiscard]] size_t get_max_address_accessed() const
        {
            if (buffer.has_source()) return buffer.get_max_address_accessed();
            return main.get_max_address_accessed();
        }
        void begin() {
            buffer.set_source(&main);
        }
        void commit() {
            buffer.move_to_source();
        }
        void rollback() {
            buffer.clear();
        }
        bool save(const std::string& filename, const std::function<void(std::ofstream&)>& extra) const {
            return main.save(filename, extra);
        };
        bool load(const std::string& filename, const std::function<void(std::ifstream&)>& extra) {
            return main.load(filename, extra);
        };

    };
}

#endif //HASH_ARENA_H
