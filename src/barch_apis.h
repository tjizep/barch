//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include <atomic>
#include "caller.h"
typedef std::function<int (caller& call, const arg_t& argv)> barch_function;
typedef heap::string_map<size_t> catmap;
heap::vector<std::string> categories();
catmap& get_category_map();
heap::vector<bool> cats2vec(const catmap& icats);

struct barch_info {
    barch_info() = default;
    void set_cats(const std::initializer_list<const char *>& icats) {
        catmap mycats;
        for (auto c : icats) {
            mycats[c] = true;
        }
        this->cats = cats2vec(mycats);
        this->dp = get_category_map().at("data");
        this->wr = get_category_map().at("write");
    }
    barch_info(const barch_function& call, const std::initializer_list<const char *>& cats, bool asynch = false) : call(call), is_asynch(asynch) {
        set_cats(cats);
    }
    barch_info(const barch_info& binfo) = default;
    barch_info& operator=(const barch_info& binfo) = default;
    barch_info& operator=(barch_info&& binfo) = default;
    barch_info(barch_info&& binfo) = delete;
    bool is_data() const {
        return cats[dp];
    }
    bool is_write() const {
        return cats[wr];
    }
    barch_function call{};
    heap::vector<bool> cats{};
    uint64_t calls {0};
    bool is_asynch{false};
    int dp = 0;
    int wr = 0;
    uint64_t total_nanos{};
};
typedef heap::string_map<barch_info> function_map;

/*
 * calls and total_nanos are written by every session thread that runs a command
 * and read by INFO commandstats, with nothing between them - TSan reports the
 * increment at asio_resp_session.h:355 on every chaos run. See TODO 205.
 *
 * atomic_ref rather than making the fields std::atomic: barch_info has a
 * defaulted move assignment that the table relies on, and an atomic member would
 * delete it. The accesses become atomic without the type changing.
 */
/*
 * fetch_add, and the fields stay plain uint64_t behind atomic_ref.
 *
 * These run once per command on the session path and every session thread hits
 * the same barch_info for a hot command, so the entry is one contended cache
 * line - the obvious worry is that this costs throughput.
 *
 * It does not, or not enough to see. A relaxed load-and-store was tried as the
 * cheaper alternative, since an approximate count would have been fine here,
 * and measured no better. Then the plain `++` and this were built and measured
 * back to back, and the atomic version came out *ahead* on that pair, which is
 * not possible - the memtier numbers on this box drift by more between adjacent
 * runs than the difference being looked for. So: no measurable cost, and the
 * exact version is the one to keep. See TODO 205 for the numbers.
 *
 * atomic_ref rather than std::atomic members: barch_info has a defaulted move
 * assignment the table relies on, and an atomic member would delete it.
 */
inline void note_command_call(barch_info& i, uint64_t nanos = 0) {
    std::atomic_ref<uint64_t>(i.calls).fetch_add(1, std::memory_order_relaxed);
    if (nanos)
        std::atomic_ref<uint64_t>(i.total_nanos).fetch_add(nanos, std::memory_order_relaxed);
}
inline void note_command_nanos(barch_info& i, uint64_t nanos) {
    std::atomic_ref<uint64_t>(i.total_nanos).fetch_add(nanos, std::memory_order_relaxed);
}
inline uint64_t command_calls(const barch_info& i) {
    return std::atomic_ref<uint64_t>(const_cast<uint64_t&>(i.calls)).load(std::memory_order_relaxed);
}
inline uint64_t command_nanos(const barch_info& i) {
    return std::atomic_ref<uint64_t>(const_cast<uint64_t&>(i.total_nanos)).load(std::memory_order_relaxed);
}
inline void reset_command_stats(barch_info& i) {
    std::atomic_ref<uint64_t>(i.calls).store(0, std::memory_order_relaxed);
    std::atomic_ref<uint64_t>(i.total_nanos).store(0, std::memory_order_relaxed);
}

/*
 * Every command now declares itself in its own {category}_api.h and registers itself
 * from the matching .cpp, through register_*_api(). What is left here is the vocabulary
 * they are all built from: the function signature, the category map that drives ACLs,
 * and the table itself.
 */

extern std::shared_ptr<function_map> functions_by_name();
#endif //BARCH_APIS_H
