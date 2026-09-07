#include "key_range.h"

#include "composite.h"
#include "keys.h"
#include "sharded_store.h"

#include <algorithm>

namespace {

/**
 * The same text, addressed in the composite region.
 *
 * A bound like `a` has no separator in it, so `encode_key` makes it a plain string
 * and it can only ever bound plain strings. A one part composite is the same text
 * placed where the composite keys are: `a` sorts below `a b`, which is what a text
 * bound has to mean there.
 */
/**
 * The same text, addressed in one region or the other.
 *
 * A bound has to be encoded the way the keys it is bounding were, and which of the
 * two that is depends on the text - so a range needs both, and needs them built
 * regardless of what the caller's own bound looks like. `plain_bound("a b")` is the
 * string, not the pair; `comp_bound("a")` is a one part composite, which sorts
 * below `a b` and is what a text bound means over there.
 */
conversion::comparable_key plain_bound(art::value_type v) {
    return conversion::convert(v.chars(), v.size, false);
}

conversion::comparable_key comp_bound(art::value_type v, char sep) {
    if (memchr(v.begin(), sep, v.size) != nullptr)
        return conversion::as_composite(v, false, sep);
    composite one;
    one.begin_plain();
    one.push(conversion::convert(v.chars(), v.size, false));
    return one.create();
}

struct found {
    std::string encoded;
    std::string text;
};

void gather(barch::sharded_store& store, art::value_type lo, art::value_type hi,
            int64_t limit, char sep, const barch::key_filter& keep,
            std::vector<found>& into) {
    store.range(lo, hi, limit, [&](art::value_type key) {
        if (keep && !keep(key))
            return;
        found f;
        f.encoded.assign((const char*) key.bytes, key.size);
        f.text = encoded_key_as_string(key, sep);
        into.push_back(std::move(f));
    });
}

char split_of(const barch::key_space_ptr& space) {
    const auto& pat = space->key_split;
    return pat.size() == 1 ? pat[0] : ' ';
}

}

namespace barch {

void text_range(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                int64_t limit, const std::function<void(art::value_type)>& cb,
                const key_filter& keep) {
    if (!space)
        return;
    sharded_store store(space);
    const char sep = split_of(space);

    /*
     * The composite region first, because it is usually empty and then none of the
     * rest is needed: the plain scan streams straight to the callback exactly as it
     * did before this existed, with no copy and no merge. Only a space that
     * actually holds split keys pays for the second half.
     */
    std::vector<found> comp;
    {
        auto l = comp_bound(lo, sep);
        std::string keep_lo((const char*) l.get_value().bytes, l.get_value().size);
        auto h = comp_bound(hi, sep);
        gather(store, art::value_type{keep_lo.data(), keep_lo.size()},
               h.get_value(), limit, sep, keep, comp);
    }
    auto plain_l = plain_bound(lo);
    auto plain_h = plain_bound(hi);
    if (comp.empty()) {
        int64_t sent = 0;
        store.range(plain_l.get_value(), plain_h.get_value(), limit,
                    [&](art::value_type key) {
            if (keep && !keep(key))
                return;
            if (limit > 0 && sent >= limit)
                return;
            cb(key);
            ++sent;
        });
        return;
    }

    std::vector<found> mixed;
    gather(store, plain_l.get_value(), plain_h.get_value(), limit, sep, keep, mixed);
    const size_t plain_end = mixed.size();
    mixed.insert(mixed.end(), std::make_move_iterator(comp.begin()),
                 std::make_move_iterator(comp.end()));
    std::inplace_merge(mixed.begin(), mixed.begin() + (long) plain_end, mixed.end(),
                       [](const found& a, const found& b) { return a.text < b.text; });

    int64_t sent = 0;
    for (const auto& f : mixed) {
        if (limit > 0 && sent >= limit)
            break;
        cb(art::value_type{f.encoded.data(), f.encoded.size()});
        ++sent;
    }
}

int64_t text_count(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                   const key_filter& keep) {
    if (!space)
        return 0;
    if (keep) {
        int64_t n = 0;
        text_range(space, lo, hi, 0, [&](art::value_type) { ++n; }, keep);
        return n;
    }
    sharded_store store(space);
    const char sep = split_of(space);
    auto pl = plain_bound(lo);
    auto ph = plain_bound(hi);
    int64_t n = store.count(pl.get_value(), ph.get_value());
    auto cl = comp_bound(lo, sep);
    std::string keep_lo((const char*) cl.get_value().bytes, cl.get_value().size);
    auto ch = comp_bound(hi, sep);
    n += store.count(art::value_type{keep_lo.data(), keep_lo.size()}, ch.get_value());
    return n;
}

}
