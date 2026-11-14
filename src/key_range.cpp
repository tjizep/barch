#include "key_range.h"

#include "composite.h"
#include "dictionary_compressor.h"
#include "keys.h"
#include "sharded_store.h"

#include <algorithm>
#include <atomic>
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
    std::string value;      // text_entries only
    bool plain;
    /** where it sorts against the other region: a plain key as if it had the lead */
    [[nodiscard]] std::string_view order() const {
        std::string_view all(encoded);
        return plain ? all : all.substr(composite_key_size);
    }
};

/** the value a leaf holds, decompressed the way GET does it - TODO 764 */
art::value_type value_of(const art::node_ptr& n) {
    const auto* cl = n.const_leaf();
    auto v = cl->get_value();
    if (cl->is_compressed())
        return dictionary::decompress(art::space_of(n), v);
    return v;
}

/**
 * One region, walked a batch at a time - TODO 905.
 *
 * The merge used to gather each region whole and merge the two in memory. With no
 * limit that held every key in the range, and with values every value too: a range
 * over a few million split keys was a few million strings at once. A cursor holds
 * one batch, at most `batch` keys and about `cap` bytes, and walks the next from
 * the last key it walked.
 *
 * Each batch is a walk of its own, so a write between two of them can be seen by
 * the later one. Two regions were never one walk either.
 */
template<bool values>
struct cursor {
    static constexpr int64_t batch = 256;
    static constexpr size_t cap = 4u << 20;

    barch::sharded_store& store;
    art::value_type hi;
    bool plain;
    const barch::key_filter& keep;
    int64_t most;           // keys a batch need walk: the limit and offset, or a batch
    std::string from;       // the next batch starts here
    bool resumed{false};    // `from` was the last key walked, so it is left out
    bool done{false};
    std::vector<found> held;
    size_t at{0};

    cursor(barch::sharded_store& store, art::value_type lo, art::value_type hi, bool plain,
           const barch::key_filter& keep, int64_t want)
        : store(store), hi(hi), plain(plain), keep(keep),
          most(want > 0 ? std::min(want, batch) : batch), from(lo.chars(), lo.size) {}

    /** whether a key is held, walking batches until one is or the region ends */
    bool fill() {
        while (at >= held.size() && !done) {
            held.clear();
            at = 0;
            const int64_t asked = most + (resumed ? 1 : 0);
            int64_t walked = 0;
            size_t bytes = 0;
            bool full = false;
            std::string last;
            auto take = [&](art::value_type key, const art::node_ptr* n) {
                if (full)
                    return;     // past the cap: the next batch walks it again
                ++walked;
                const std::string_view k(key.chars(), key.size);
                if (walked == 1 && resumed && k == from)
                    return;
                last.assign(k);
                if (keep && !keep(key))
                    return;
                found& f = held.emplace_back();
                f.encoded.assign(k);
                f.plain = plain;
                bytes += k.size();
                if (n) {
                    const auto v = value_of(*n);
                    f.value.assign(v.chars(), v.size);
                    bytes += v.size;
                }
                full = bytes >= cap;
            };
            const art::value_type lo{from.data(), (unsigned) from.size()};
            if constexpr (values)
                store.range_entries(lo, hi, asked, [&](art::value_type key, const art::node_ptr& n) { take(key, &n); });
            else
                store.range(lo, hi, asked, [&](art::value_type key) { take(key, nullptr); });
            done = !full && walked < asked;
            if (!last.empty()) {
                from = std::move(last);
                resumed = true;
            }
        }
        return at < held.size();
    }
    [[nodiscard]] const found& top() const { return held[at]; }
};

std::atomic<size_t> held_peak{0};

void note_held(size_t n) {
    size_t was = held_peak.load(std::memory_order_relaxed);
    while (n > was && !held_peak.compare_exchange_weak(was, n, std::memory_order_relaxed)) {}
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

/*
 * text_range and text_entries, one walk: cb takes a key and, with `values`, its value -
 * TODO 764. Without, the value is empty and nothing reads one.
 */
template<bool values, typename F>
static void text_walk(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                      int64_t limit, const F& cb, const key_filter& keep, int64_t offset) {
    if (!space)
        return;
    if (offset < 0)
        offset = 0;
    // what a region need hand out at most: the skipped keys and the ones sent
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
    const art::value_type plain_l = plain_of(comp_l);
    const art::value_type plain_h = plain_of(comp_h);
    cursor<values> comp(store, as_value(comp_l), as_value(comp_h), false, keep, want);
    const bool has_comp = comp.fill();
    note_held(comp.held.size());
    if (!has_comp) {
        int64_t sent = 0;
        // with a filter the offset counts what the filter lets through, which the
        // node counts can't know, so the skipped keys are walked and dropped here
        int64_t drop = keep ? offset : 0;
        auto each = [&](art::value_type key, const art::node_ptr* n) {
            if (keep && !keep(key))
                return;
            if (drop > 0) {
                --drop;
                return;
            }
            if (limit > 0 && sent >= limit)
                return;
            cb(key, n ? value_of(*n) : art::value_type{});
            ++sent;
        };
        if constexpr (values)
            store.range_entries(plain_l, plain_h, keep ? want : limit,
                                [&](art::value_type key, const art::node_ptr& n) { each(key, &n); },
                                keep ? 0 : offset);
        else
            store.range(plain_l, plain_h, keep ? want : limit,
                        [&](art::value_type key) { each(key, nullptr); }, keep ? 0 : offset);
        return;
    }

    /*
     * Both regions, a batch from each, merged as they go. The offset is stepped over
     * here, since the node counts know each region and not where the two interleave.
     * That costs a walk of the offset, not a copy of it.
     */
    cursor<values> flat(store, plain_l, plain_h, true, keep, want);
    int64_t sent = 0;
    int64_t drop = offset;
    while (limit <= 0 || sent < limit) {
        const bool has_flat = flat.fill();
        const bool more = comp.fill();
        note_held(flat.held.size() + comp.held.size());
        if (!has_flat && !more)
            break;
        // a plain key first on a tie, as the merge of the gathered regions did
        auto& next = !more || (has_flat && flat.top().order() <= comp.top().order()) ? flat : comp;
        const found& f = next.top();
        if (drop > 0) {
            --drop;
        } else {
            cb(art::value_type{f.encoded.data(), f.encoded.size()},
               art::value_type{f.value.data(), f.value.size()});
            ++sent;
        }
        ++next.at;
    }
}

size_t text_held_peak() {
    return held_peak.exchange(0, std::memory_order_relaxed);
}

void text_range(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                int64_t limit, const std::function<void(art::value_type)>& cb,
                const key_filter& keep, int64_t offset) {
    text_walk<false>(space, lo, hi, limit, [&cb](art::value_type k, art::value_type) { cb(k); }, keep, offset);
}

void text_entries(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                  int64_t limit, const std::function<void(art::value_type, art::value_type)>& cb,
                  const key_filter& keep, int64_t offset) {
    text_walk<true>(space, lo, hi, limit, cb, keep, offset);
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
    sharded_store store(space);
    const char sep = split_of(space);
    const std::string cl = comp_bound(lo, sep);
    const std::string ch = comp_bound(hi, sep);
    if (keep) {
        // order doesn't matter to a count, so each region is walked on its own and
        // nothing is held - TODO 905. It went through text_range, which merged them
        int64_t n = 0;
        auto counted = [&](art::value_type k) { n += keep(k) ? 1 : 0; };
        store.range(plain_of(cl), plain_of(ch), 0, counted);
        store.range(as_value(cl), as_value(ch), 0, counted);
        return n;
    }
    int64_t n = store.count(plain_of(cl), plain_of(ch));
    n += store.count(as_value(cl), as_value(ch));
    return n;
}

}
