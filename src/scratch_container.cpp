#include "scratch_container.h"

#include "function_api.h"
#include "keys.h"
#include "sharded_store.h"
#include "statistics.h"

namespace barch::scratch {

namespace {

/*
 * The store's keys are text, and not every byte string comes back from it as it
 * went in - checked in test/scratchcontainertest.cpp with SHOW_KEYS=1:
 *   - a key that reads as a number ("123", "-5", "1.5") is kept as a number, and
 *     sorts with the numbers rather than with the text, outside a text range
 *   - a space splits a key into a composite, and a leading one is lost
 *   - control bytes don't come back, and NUL is refused outright in a range bound
 * So a key goes in as "k" and its bytes escaped: every byte up to and including
 * '!' (0x21) is '!' and then the byte plus 0x30, the rest are themselves. The "k"
 * means it never reads as a number; nothing escaped is a space or a control byte.
 * '!' is below every byte written as itself and the second bytes keep the order of
 * the bytes they stand for, so escaped keys sort the way the raw ones do, a prefix
 * before what it prefixes included.
 */
constexpr char lead = 'k';
constexpr unsigned char esc = 0x21;

std::string escaped(const std::string& raw) {
    std::string out;
    out.reserve(raw.size() + 4);
    out.push_back(lead);
    for (char c : raw) {
        auto b = (unsigned char) c;
        if (b <= esc) {
            out.push_back((char) esc);
            out.push_back((char) (b + 0x30));
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string unescaped(const std::string& key) {
    std::string out;
    out.reserve(key.size());
    for (size_t i = 1; i < key.size(); ++i) {
        if ((unsigned char) key[i] == esc && i + 1 < key.size())
            out.push_back((char) ((unsigned char) key[++i] - 0x30));
        else
            out.push_back(key[i]);
    }
    return out;
}

/** escaped(raw + "\0"): nothing escaped sorts between escaped(raw) and this */
std::string just_after_escaped(const std::string& escaped_key) {
    std::string out = escaped_key;
    out.push_back((char) esc);
    out.push_back((char) 0x30);
    return out;
}

} // namespace

/*
 * No space until the first write - TODO 777. A transaction makes three of these and a
 * one SET transaction writes at most one, and taking a space from the pool and giving
 * it back was about 15% of what such a transaction cost.
 */
store::store() = default;

store::~store() {
    acc = {};                   // it holds the space too
    key_space::give_back(std::move(space));
}

void store::reset() {
    // the old space goes back for reuse, or with its last pointer - TODO 711
    acc = {};
    key_space::give_back(std::move(space));
    count = 0;
}

void store::take() {
    if (space)
        return;
    space = key_space::make_scratch();
    ++statistics::scratch_spaces_taken;
    acc = functions::store_for_owner(space);
}

bool store::put(const std::string& key, const std::string& value) {
    return write(key, value, true);
}

bool store::add(const std::string& key, const std::string& value) {
    return write(key, value, false);
}

/*
 * One locked insert, straight into the space - TODO 754. It used to be a has() and
 * then the store's set, two trips through the whole store interface, and a map's
 * insert added a third. A transaction's read log pays this for every key it reads
 * first, so it was most of what a read in a transaction cost. The shard's insert
 * says whether the key count grew, which is whether the key is new, and with
 * `replace` off it leaves a key that's there alone.
 */
bool store::write(const std::string& key, const std::string& value, bool replace) {
    take();
    const std::string raw = escaped(key);
    auto converted = space->encode_key(art::value_type{raw.data(), raw.size()});
    const art::value_type k = converted.get_value();
    const art::value_type v{value.data(), value.size()};
    if (!fits_in_leaf(k.size, v.size))
        throw std::runtime_error(std::string("scratch container: ") + too_large_message());
    bool added = false;
    sharded_store(space).with_key_write(k, [&](const shard_ptr& t) {
        art::key_options opts;
        opts.set_hashed(!t->opt_ordered_keys);
        added = t->opt_insert(opts, k, v, replace, [](const art::node_ptr&) {});
    });
    if (added)
        ++count;
    return added;
}

bool store::get(const std::string& key, std::string& value) const {
    if (!space)
        return false;
    return acc.get(escaped(key), value) == foreign::store_access::read_state::present;
}

bool store::has(const std::string& key) const {
    std::string ignored;
    return get(key, ignored);
}

bool store::drop(const std::string& key) {
    if (!has(key))
        return false;
    acc.remove(escaped(key));
    --count;
    return true;
}

void store::page(const std::string& from, bool inclusive, size_t want,
                 heap::vector<std::string>& out) const {
    out.clear();
    if (count == 0)
        return;
    // range is [lo, hi). Nothing sorts between a key and the key with a NUL added,
    // so that is "after", and just after the largest key bounds it - any bytes at
    // all can start a string key, so no fixed upper bound would do
    std::string top;
    if (!acc.max(top))
        return;
    top = just_after_escaped(top);      // max is escaped already
    const std::string lo = inclusive ? escaped(from) : just_after_escaped(escaped(from));
    if (lo >= top)
        return;
    acc.range(lo, top, (int64_t) want, out);
    for (auto& k : out)
        k = unescaped(k);
}

}
