#include "ids.h"

#include <algorithm>
#include "meta_keys.h"

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

std::mutex ids_mu;   // named for the unity build: function_sync.cpp has a `mu` - TODO 610
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
                 uint64_t count, uint64_t& first, std::string& err, const char* floor) {
    first = 0;
    if (count == 0) {
        err = "a reservation of no ids";
        return false;
    }
    if (!space) {
        err = "no key space";
        return false;
    }
    std::lock_guard<std::mutex> g(ids_mu);
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
    /*
     * The counter is a meta key - TODO 527 - so no client can DEL or SET it and
     * eviction leaves it alone (TODO 528). A plain `ids:<name>` can still be there:
     * from a store written before, or brought in by IMPORT, which is how EXPORT
     * carries the counter. The larger of the two is where the sequence is.
     */
    auto key = counter_key(name);
    const auto counter_of = [&](const std::string& k, bool& plain_there) {
        uint64_t past = 0;
        std::string raw;
        if (barch::meta::get(space, k, raw))
            past = strtoull(raw.c_str(), nullptr, 10);
        raw.clear();
        plain_there = acc.get(k, raw) == foreign::store_access::read_state::present;
        if (plain_there)
            past = std::max<uint64_t>(past, strtoull(raw.c_str(), nullptr, 10));
        return past;
    };
    bool plain = false, floor_plain = false;
    uint64_t at = counter_of(key, plain);
    if (at == 0)
        at = 1;                           // none, or unreadable: starts at 1 rather than
                                          // handing out 0, which means "none"
    if (floor) {
        // the other sequence's counter is past everything it ever handed out, so
        // starting here can't land on one of its ids
        const uint64_t past = counter_of(counter_key(floor), floor_plain);
        if (past > at)
            at = past;
    }

    // the counter moves past the whole block before a single id leaves this
    // function: that is what makes an abandoned block a gap rather than a repeat
    std::string e;
    if (!barch::meta::set(space, key, std::to_string(at + take), e)) {
        err = e.empty() ? "could not write " + key : e;
        return false;
    }
    // then the plain one goes. A crash in between leaves both, and the larger wins
    if (plain && acc.remove)
        acc.remove(key);

    have.next = at + count;
    have.end = at + take;
    have.step = have.step < max_step ? have.step * 2 : max_step;
    first = at;
    return true;
}

void forget_sequences(const std::string& space_name) {
    std::lock_guard<std::mutex> g(ids_mu);
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
    std::lock_guard<std::mutex> g(ids_mu);
    blocks.clear();
}

}
