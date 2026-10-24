#include "key_range.h"

#include "composite.h"
#include "keys.h"
#include "sharded_store.h"

#include <algorithm>
#include <string_view>

namespace {

/**
 * A bound, encoded where a composite key with the same text would be stored.
 *
 * Each region is walked in stored order, so that is the order a range has to be
 * in - TODO 712. Merging them by text (TODO 260) handed out keys in an order the
 * bounds didn't agree with: `g2_90_xx:yy` is the composite (`g2_90_xx`, `yy`),
 * stored below the plain `g2_90_xx0y` because a part's end sorts below every byte,
 * while as text `:` sorts above `0`. A full listing put it after `g2_90_xx0y`, and
 * a range ending at `g2_90_xx0y` still let it in.
 *
 * So one order for both regions: a plain key sorts where the one part composite of
 * its text would. Those are the same bytes behind the composite's lead, which is
 * all plain_of below is, and why the plain region's bounds come from these.
 *
 * Text with no separator becomes a one part composite, which sorts below every
 * longer key sharing that first part. Text ending in a separator (`a:`) means past
 * `a` itself but below every `a:x`, so it ends the way a part does when another
 * follows - in a key_terminator rather than the final 0.
 */
std::string comp_bound(art::value_type v, char sep) {
    std::string out;
    if (memchr(v.begin(), sep, v.size) != nullptr) {
        auto c = conversion::as_composite(v, false, sep);
        out.assign((const char*) c.get_value().bytes, c.get_value().size);
    } else {
        composite one;
        one.begin_plain();
        one.push(conversion::convert(v.chars(), v.size, false));
        auto enc = one.create();
        out.assign((const char*) enc.bytes, enc.size);
    }
    if (v.size && v.bytes[v.size - 1] == (uint8_t) sep && out.size() > composite_key_size) {
        if (out.back() == 0)
            out.back() = (char) key_terminator;
        else
            out.push_back('\0'); // a part with no terminator, eg an integer
    }
    return out;
}

/**
 * the same position in the plain region: a plain key is a composite without its
 * lead, which is composite_key_size bytes (the tag and the end of that part)
 */
art::value_type plain_of(const std::string& comp) {
    return {comp.data() + composite_key_size, (unsigned) (comp.size() - composite_key_size)};
}

art::value_type as_value(const std::string& s) {
    return {s.data(), (unsigned) s.size()};
}

struct found {
    std::string encoded;
    bool plain;
    /** where it sorts against the other region: a plain key as if it had the lead */
    [[nodiscard]] std::string_view order() const {
        std::string_view all(encoded);
        return plain ? all : all.substr(composite_key_size);
    }
};

void gather(barch::sharded_store& store, art::value_type lo, art::value_type hi,
            int64_t limit, bool plain, const barch::key_filter& keep,
            std::vector<found>& into) {
    store.range(lo, hi, limit, [&](art::value_type key) {
        if (keep && !keep(key))
            return;
        found f;
        f.encoded.assign((const char*) key.bytes, key.size);
        f.plain = plain;
        into.push_back(std::move(f));
    });
}

char split_of(const barch::key_space_ptr& space) {
    const auto& pat = space->key_split;
    return pat.size() == 1 ? pat[0] : ' ';
}

/**
 * A range bound, held to the same rule as a key.
 *
 * `s_filter_key` refuses a key with a NUL anywhere but the end, and takes one at
 * the end as the terminator it is about to add anyway. Bounds went nowhere near
 * that check, so `RANGE "k\0" "l"` was accepted and answered **nothing at all** -
 * not the keys after `k`, not the keys from `k`, an empty range - because the
 * encoded bound descends past the terminator of a string key and the walk finds
 * no such path. `a\0` to `z` came back empty on a space full of keys.
 *
 * Same rule as a key, then: a trailing NUL is the terminator and comes off, and a
 * NUL anywhere else is refused out loud in the same words a key is refused in.
 * Whoever wanted "the key after k" wants `k\1`, which has always worked; what
 * they must not get is silence. See TODO 294.
 */
art::value_type checked_bound(art::value_type v) {
    if (v.size == 0)
        return v;
    size_t n = v.size;
    if (v.bytes[n - 1] == 0)
        --n;
    if (n && memchr(v.bytes, 0, n))
        throw_exception<std::runtime_error>("range bound contains a null interior byte");
    return {v.bytes, (unsigned) n};
}

}

namespace barch {

void text_range(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                int64_t limit, const std::function<void(art::value_type)>& cb,
                const key_filter& keep, int64_t offset) {
    if (!space)
        return;
    if (offset < 0)
        offset = 0;
    // a region is gathered into memory before the merge, so it has to hold the
    // skipped keys as well as the ones handed out
    const int64_t want = limit > 0 ? limit + offset : limit;
    lo = checked_bound(lo);
    hi = checked_bound(hi);
    sharded_store store(space);
    const char sep = split_of(space);

    /*
     * The composite region first, because it is usually empty and then none of the
     * rest is needed: the plain scan streams straight to the callback exactly as it
     * did before this existed, with no copy and no merge. Only a space that
     * actually holds split keys pays for the second half.
     */
    const std::string comp_l = comp_bound(lo, sep);
    const std::string comp_h = comp_bound(hi, sep);
    std::vector<found> comp;
    gather(store, as_value(comp_l), as_value(comp_h), want, false, keep, comp);
    const art::value_type plain_l = plain_of(comp_l);
    const art::value_type plain_h = plain_of(comp_h);
    if (comp.empty()) {
        int64_t sent = 0;
        // with a filter the offset counts what the filter lets through, which the
        // node counts can't know, so the skipped keys are walked and dropped here
        int64_t drop = keep ? offset : 0;
        store.range(plain_l, plain_h, keep ? want : limit,
                    [&](art::value_type key) {
            if (keep && !keep(key))
                return;
            if (drop > 0) {
                --drop;
                return;
            }
            if (limit > 0 && sent >= limit)
                return;
            cb(key);
            ++sent;
        }, keep ? 0 : offset);
        return;
    }

    std::vector<found> mixed;
    gather(store, plain_l, plain_h, want, true, keep, mixed);
    const size_t plain_end = mixed.size();
    mixed.insert(mixed.end(), std::make_move_iterator(comp.begin()),
                 std::make_move_iterator(comp.end()));
    std::inplace_merge(mixed.begin(), mixed.begin() + (long) plain_end, mixed.end(),
                       [](const found& a, const found& b) { return a.order() < b.order(); });

    int64_t sent = 0;
    int64_t drop = offset;
    for (const auto& f : mixed) {
        if (drop > 0) {
            --drop;
            continue;
        }
        if (limit > 0 && sent >= limit)
            break;
        cb(art::value_type{f.encoded.data(), f.encoded.size()});
        ++sent;
    }
}

std::vector<std::pair<std::string, std::string>> text_bounds(const key_space_ptr& space, art::value_type lo,
                                                             art::value_type hi) {
    std::vector<std::pair<std::string, std::string>> out;
    if (!space)
        return out;
    lo = checked_bound(lo);
    hi = checked_bound(hi);
    const char sep = split_of(space);
    const std::string cl = comp_bound(lo, sep);
    const std::string ch = comp_bound(hi, sep);
    const auto pl = plain_of(cl);
    const auto ph = plain_of(ch);
    out.emplace_back(std::string(pl.chars(), pl.size), std::string(ph.chars(), ph.size));
    out.emplace_back(cl, ch);
    return out;
}

int64_t text_count(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                   const key_filter& keep) {
    if (!space)
        return 0;
    // the counted path does not go through text_range, so it checks for itself:
    // a count answering zero is exactly as quiet as a range answering nothing
    lo = checked_bound(lo);
    hi = checked_bound(hi);
    if (keep) {
        int64_t n = 0;
        text_range(space, lo, hi, 0, [&](art::value_type) { ++n; }, keep);
        return n;
    }
    sharded_store store(space);
    const char sep = split_of(space);
    const std::string cl = comp_bound(lo, sep);
    const std::string ch = comp_bound(hi, sep);
    int64_t n = store.count(plain_of(cl), plain_of(ch));
    n += store.count(as_value(cl), as_value(ch));
    return n;
}

}
