#include "scratch_container.h"

#include "function_api.h"

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

store::store() {
    reset();
}

void store::reset() {
    // the old space goes with its last pointer, and drop_on_release takes its files
    space = key_space::make_scratch();
    acc = functions::store_for_owner(space);
    count = 0;
}

bool store::put(const std::string& key, const std::string& value) {
    const bool added = !has(key);
    std::string err;
    if (!acc.set(escaped(key), value, err))
        throw std::runtime_error("scratch container: " + (err.empty() ? std::string("set failed") : err));
    if (added)
        ++count;
    return added;
}

bool store::get(const std::string& key, std::string& value) const {
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
