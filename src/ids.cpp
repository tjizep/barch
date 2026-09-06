#include "ids.h"

#include "function_api.h"
#include "lzr_log.h"

#include <mutex>

namespace {

/** what one sequence has left in hand */
struct block {
    uint64_t next{0};
    uint64_t end{0};
    /** how much to take next time; doubles so an import pays a handful of writes */
    uint64_t step{64};
};

constexpr uint64_t max_step = 4096;

std::mutex mu;
heap::string_map<block> blocks;

std::string cache_key(const std::string& space, const std::string& name) {
    // a real NUL, not "\x00" in a literal, which appends nothing at all
    std::string tag = space;
    tag.push_back('\0');
    tag += name;
    return tag;
}

/** the key the counter itself lives under */
std::string counter_key(const std::string& name) {
    return "ids:" + name;
}

}

namespace barch {

bool reserve_ids(const key_space_ptr& space, const std::string& name,
                 uint64_t count, uint64_t& first, std::string& err) {
    first = 0;
    if (count == 0) {
        err = "a reservation of no ids";
        return false;
    }
    if (!space) {
        err = "no key space";
        return false;
    }
    std::lock_guard<std::mutex> g(mu);
    auto tag = cache_key(space->get_canonical_name(), name);
    auto& have = blocks[tag];
    if (have.end - have.next >= count) {
        first = have.next;
        have.next += count;
        return true;
    }

    /*
     * Whatever is left in the old block is abandoned rather than kept beside the new
     * one. Keeping both would make an id block a list, for the sake of at most
     * step-1 numbers out of 2^64.
     */
    uint64_t take = count > have.step ? count : have.step;
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.get || !acc.set) {
        err = "this key space cannot be written";
        return false;
    }
    auto key = counter_key(name);
    std::string raw;
    uint64_t at = 1;
    if (acc.get(key, raw) == foreign::store_access::read_state::present) {
        at = strtoull(raw.c_str(), nullptr, 10);
        if (at == 0)
            at = 1;                       // an unreadable counter starts again rather
    }                                     // than handing out 0, which means "none"

    // the counter moves past the whole block before a single id leaves this
    // function: that is what makes an abandoned block a gap rather than a repeat
    std::string e;
    if (!acc.set(key, std::to_string(at + take), e)) {
        err = e.empty() ? "could not write " + key : e;
        return false;
    }

    have.next = at + count;
    have.end = at + take;
    have.step = have.step < max_step ? have.step * 2 : max_step;
    first = at;
    return true;
}

void forget_sequences(const std::string& space_name) {
    std::lock_guard<std::mutex> g(mu);
    std::string prefix = space_name;
    prefix.push_back('\0');
    for (auto it = blocks.begin(); it != blocks.end();) {
        if (it->first.compare(0, prefix.size(), prefix) == 0)
            it = blocks.erase(it);
        else
            ++it;
    }
}

void forget_all_sequences() {
    std::lock_guard<std::mutex> g(mu);
    blocks.clear();
}

}
