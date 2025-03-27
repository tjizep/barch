//
// Created by linuxlite on 3/27/25.
//

#ifndef HASH_ARENA_H
#define HASH_ARENA_H
#include "storage.h"
#include "compressed_address.h"
#include <fstream>
#include <ankerl/unordered_dense.h>
struct hash_arena
{
    enum
    {
        cache_size = 1024
    };

private:
    // seems to make a small difference
    typedef ankerl::unordered_dense::map<size_t, storage> hash_type ;
    //typedef std::unordered_map<size_t, storage> hash_type ;
    hash_type hidden_arena{};
    heap::vector<size_t> free_address_list{};
    size_t top = 10000000;
    size_t free_pages = top;
    size_t last_allocated = 0;
    size_t max_address_accessed = 0;

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
    hash_arena() = default;
    // arena virtualization functions
    [[nodiscard]] size_t page_count() const
    {
        return hidden_arena.size();
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

    [[nodiscard]] bool is_free(size_t at) const
    {
        return hidden_arena.count(at) == 0;
    }

    [[nodiscard]] bool has_free() const
    {
        return free_pages > 1;
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

    [[nodiscard]] bool has_page(size_t at) const
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
        if (pi == hidden_arena.end())
        {
            throw std::runtime_error("missing page");
        }
        return pi->second;
    }

    [[nodiscard]] const storage& retrieve_page(size_t at) const
    {
        if (at > top)
        {
            throw std::runtime_error("missing page");
        }
        auto pi = hidden_arena.find(at);
        if (pi == hidden_arena.end())
        {
            throw std::runtime_error("invalid page");
        }
        return pi->second;
    }

    void iterate_arena(const std::function<bool(size_t, storage&)>& iter)
    {
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
        for (auto& [at,str] : hidden_arena)
        {
            iter(at, str);
        }
    }
    void iterate_arena(const std::function<void(size_t, const storage&) >& iter) const
    {
        for (auto& [at,str] : hidden_arena)
        {
            iter(at, str);
        }
    }

    [[nodiscard]] size_t get_max_address_accessed() const
    {
        return max_address_accessed;
    }

    bool save(const std::string& filename, const std::function<void(std::ofstream&)>& extra) const;
    bool load(const std::string& filename, const std::function<void(std::ifstream&)>& extra);
    static bool arena_read(hash_arena& arena, const std::function<void(std::ifstream&)>& extra, const std::string& filename) ;
};

#endif //HASH_ARENA_H
