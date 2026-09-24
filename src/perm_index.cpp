#include "perm_index.h"

#include "composite.h"
#include "conversion.h"
#include "function_api.h"
#include "sharded_store.h"

#include <algorithm>
#include <mutex>
#include <simdjson.h>

namespace barch::pindex {

// ---- chains -------------------------------------------------------------------------

/*
 * Greene-Kleitman: read a subset's bits as brackets, a field in the subset as ")" and
 * one out of it as "(", and match them. What's left unmatched reads ")))(((". The
 * subsets with the same matched pairs form one chain, which starts where nothing is
 * left over as ")" and grows by turning the unmatched "(" into ")" from the left, one
 * at a time. Every subset is in exactly one chain, and there are C(n, n/2) of them.
 *
 * A chain becomes an ordering of the fields: its first subset, then the fields it
 * gains in the order it gains them, then the rest, which it never reaches. Each subset
 * in the chain is then the first |subset| fields of that ordering.
 */
static chain_set make_chains(size_t n) {
    chain_set cs;
    cs.n = n;
    const size_t full = (size_t) 1 << n;
    cs.chain_of.assign(full, 0xff);
    for (size_t m = 0; m < full; ++m) {
        std::vector<uint8_t> open;
        bool close_left = false;
        for (size_t i = 0; i < n; ++i) {
            if ((m >> i) & 1) {
                if (!open.empty())
                    open.pop_back();
                else
                    close_left = true;
            } else {
                open.push_back((uint8_t) i);
            }
        }
        if (close_left)
            continue;                   // not where a chain starts
        const auto id = (uint8_t) cs.chains.size();
        std::vector<uint8_t> order;
        for (size_t i = 0; i < n; ++i)
            if ((m >> i) & 1)
                order.push_back((uint8_t) i);
        size_t cur = m;
        cs.chain_of[cur] = id;
        for (uint8_t p : open) {
            order.push_back(p);
            cur |= (size_t) 1 << p;
            cs.chain_of[cur] = id;
        }
        for (size_t i = 0; i < n; ++i)
            if (!((cur >> i) & 1))
                order.push_back((uint8_t) i);
        cs.chains.push_back(std::move(order));
    }
    return cs;
}

const chain_set& chains_for(size_t n) {
    static std::once_flag once;
    static std::vector<chain_set> all;
    std::call_once(once, [] {
        for (size_t i = 0; i <= max_fields; ++i)
            all.push_back(make_chains(i));
    });
    return all[n > max_fields ? 0 : n];
}

// ---- components ---------------------------------------------------------------------

/*
 * A stored composite key is a lead (tplain) and then components, each a type byte, its
 * body and one terminator byte: 0x01 inside the key, 0x00 on the last. Strings run to
 * their terminator; numbers are a fixed size, taken from the encoder itself rather
 * than from a constant, so this can't disagree with what the store writes.
 */
static std::string encoded_alone(const conversion::comparable_key& k) {
    composite c;
    c.begin_plain();
    c.push(k);
    auto v = c.create(false);           // every terminator 0x01
    std::string out(v.chars() + 2, v.size - 2);
    // every component ends in its terminator; a key whose size left its trailing
    // zero out comes back without one, and join would overwrite its last byte
    if (out.empty() || (uint8_t) out.back() != key_terminator)
        out.push_back((char) key_terminator);
    return out;
}

static size_t fixed_size(uint8_t type) {
    static const size_t i64 = encoded_alone(conversion::comparable_key((int64_t) 0)).size();
    static const size_t f64 = encoded_alone(conversion::comparable_key(0.0)).size();
    static const size_t i32 = encoded_alone(conversion::comparable_key((int32_t) 0)).size();
    static const size_t f32 = encoded_alone(conversion::comparable_key(0.0f)).size();
    switch (type) {
        case art::tinteger: return i64;
        case art::tdouble: return f64;
        case art::tshort: return i32;
        case art::tfloat: return f32;
        default: return 0;
    }
}

bool split(art::value_type key, std::vector<std::string>& parts) {
    parts.clear();
    if (key.size < 3 || key.bytes[0] != art::tplain)
        return false;
    size_t i = 2;
    while (i < key.size) {
        uint8_t type = key.bytes[i];
        size_t len = 0;
        if (type == art::tstring) {
            size_t j = i + 1;
            while (j < key.size && key.bytes[j] != 0 && key.bytes[j] != key_terminator)
                ++j;
            if (j >= key.size)
                return false;
            len = j - i + 1;
        } else {
            len = fixed_size(type);
            if (!len || i + len > key.size)
                return false;
        }
        parts.emplace_back(key.chars() + i, len);
        i += len;
    }
    return !parts.empty();
}

/** the components run together with every terminator 0x01: a key's prefix */
static std::string join_open(const std::vector<std::string>& parts) {
    std::string out;
    out.push_back((char) art::tplain);
    out.push_back((char) key_terminator);
    for (const auto& p : parts) {
        out += p;
        out.back() = (char) key_terminator;
    }
    return out;
}

std::string join(const std::vector<std::string>& parts) {
    auto out = join_open(parts);
    if (!parts.empty())
        out.back() = 0;                 // the last component ends the key
    return out;
}

std::string component_of(const std::string& text) {
    return encoded_alone(conversion::convert(text.data(), text.size(), false));
}

/** always a string, even when it reads as a number - an index name */
static std::string string_component(const std::string& text) {
    return encoded_alone(conversion::convert(text.data(), text.size(), true));
}

static std::string chain_component(size_t chain) {
    return encoded_alone(conversion::comparable_key((int64_t) chain));
}

/*
 * Bounds for sharded_store::range end in a zero, the way a comparable_key's do: the
 * tree's lower bound reads a key as terminated, and a bare [0x07] found nothing. A
 * trailing zero moves neither bound past a real key, since every component starts
 * with a type byte of at least 1.
 */
static std::string bound(std::string b) {
    b.push_back('\0');
    return b;
}

static void range_keys(const key_space_ptr& space, const std::string& lo, const std::string& hi,
                       int64_t limit, std::vector<std::string>& out) {
    const auto l = bound(lo), h = bound(hi);
    barch::sharded_store store(space);
    store.range(art::value_type{l.data(), l.size()}, art::value_type{h.data(), h.size()}, limit,
                [&](art::value_type k) { out.emplace_back(k.chars(), k.size); });
}

// ---- registry -----------------------------------------------------------------------

/*
 * `<space>.index.<name>` in the configuration space, a JSON definition:
 *
 *     {"source": "composite", "fields": 3, "pinned": 1, "ready": [0, 2]}
 */
bool definition::is_ready(size_t chain) const {
    for (size_t c : ready)
        if (c == chain) return true;
    return false;
}

static key_space_ptr config_space() {
    return barch::get_keyspace("configuration");
}

static std::string def_key(const key_space_ptr& source, const std::string& name) {
    return source->get_canonical_name() + ".index." + name;
}

std::string index_space_name(const key_space_ptr& source) {
    return source->get_canonical_name() + "_ix";
}

static std::string to_json(const definition& d) {
    std::string out = "{\"source\":\"composite\",\"fields\":" + std::to_string(d.fields) +
                      ",\"pinned\":" + std::to_string(d.pinned) + ",\"ready\":[";
    for (size_t i = 0; i < d.ready.size(); ++i) {
        if (i) out += ",";
        out += std::to_string(d.ready[i]);
    }
    return out + "]}";
}

static bool from_json(const std::string& raw, definition& d) {
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(raw).get(doc) != simdjson::SUCCESS)
        return false;
    std::string_view source;
    uint64_t fields = 0, pinned = 0;
    if (doc["source"].get(source) != simdjson::SUCCESS || source != "composite")
        return false;
    if (doc["fields"].get(fields) != simdjson::SUCCESS || doc["pinned"].get(pinned) != simdjson::SUCCESS)
        return false;
    d.fields = (size_t) fields;
    d.pinned = (size_t) pinned;
    d.ready.clear();
    simdjson::dom::array ready;
    if (doc["ready"].get(ready) == simdjson::SUCCESS) {
        for (auto v : ready) {
            uint64_t c = 0;
            if (v.get(c) == simdjson::SUCCESS)
                d.ready.push_back((size_t) c);
        }
    }
    return d.fields >= 1 && d.fields <= max_fields;
}

static bool good_name(const std::string& name) {
    if (name.empty() || name.size() > 64)
        return false;
    for (char c : name)
        if (!isalnum((unsigned char) c) && c != '_' && c != '-')
            return false;
    return true;
}

static bool store_def(const key_space_ptr& source, const definition& d, std::string& err) {
    auto acc = barch::functions::store_for_owner(config_space());
    if (!acc.set) {
        err = "the configuration space can't be written";
        return false;
    }
    return acc.set(def_key(source, d.name), to_json(d), err);
}

bool load(const key_space_ptr& source, const std::string& name, definition& out, std::string& err) {
    auto acc = barch::functions::store_for_owner(config_space());
    std::string raw;
    if (!acc.get || acc.get(def_key(source, name), raw) != foreign::store_access::read_state::present) {
        err = "no index called " + name;
        return false;
    }
    out.name = name;
    if (!from_json(raw, out)) {
        err = "the definition of index " + name + " can't be read";
        return false;
    }
    return true;
}

std::vector<definition> list(const key_space_ptr& source) {
    // configuration keys are plain strings: a type byte, the text, a terminator
    std::string prefix = source->get_canonical_name() + ".index.";
    std::string lo = std::string(1, (char) art::tstring) + prefix;
    std::string hi = lo;
    hi.back() = (char) (hi.back() + 1);
    std::vector<std::string> names, found;
    range_keys(config_space(), lo, hi, -1, found);
    for (const auto& k : found)
        if (k.size() > lo.size() + 1)
            names.emplace_back(k.data() + lo.size(), k.size() - lo.size() - 1);
    std::vector<definition> out;
    for (const auto& n : names) {
        definition d;
        std::string err;
        if (load(source, n, d, err))
            out.push_back(std::move(d));
    }
    return out;
}

bool create(const key_space_ptr& source, const definition& def, std::string& err) {
    if (!good_name(def.name)) {
        err = "an index name is letters, digits, _ and -, up to 64 of them";
        return false;
    }
    if (def.fields < 1 || def.fields > max_fields) {
        err = "an index has 1 to 8 fields";
        return false;
    }
    if (def.pinned > 8) {
        err = "an index pins at most 8 components";
        return false;
    }
    definition existing;
    std::string ignored;
    if (load(source, def.name, existing, ignored)) {
        err = "there is already an index called " + def.name;
        return false;
    }
    // the index space splits its keys the way its source does, so KEYS there reads
    // the same. Only matters before the index space first opens
    auto conf = barch::functions::store_for_owner(config_space());
    if (source->key_split.size() == 1 && conf.get && conf.set) {
        std::string have, e;
        auto key = index_space_name(source) + ".key_split";
        if (conf.get(key, have) != foreign::store_access::read_state::present)
            conf.set(key, source->key_split, e);
    }
    definition d = def;
    d.ready.clear();
    return store_def(source, d, err);
}

/*
 * Every key in [lo, hi), a batch at a time, copied out so no lock is held while the
 * batch is used. Each page after the first starts at the last key of the one before,
 * inclusive, and drops it: starting just past it (the key with a zero appended) found
 * nothing, and every index lost all but its first 1,024 records.
 */
static void each_key(const key_space_ptr& space, const std::string& lo, const std::string& hi,
                     const std::function<void(const std::vector<std::string>&)>& batch_fn) {
    constexpr int64_t page = 1024;
    std::string from = lo, last;
    bool again = false;
    for (;;) {
        std::vector<std::string> batch;
        range_keys(space, from, hi, page + (again ? 1 : 0), batch);
        if (again && !batch.empty() && batch.front() == last)
            batch.erase(batch.begin());
        if (batch.empty())
            return;
        batch_fn(batch);
        if ((int64_t) batch.size() < page)
            return;
        last = batch.back();
        from = last;
        // range_keys terminates its bounds, and a stored key already ends in its
        // own terminator: take that off so the restart is the key itself
        if (!from.empty() && from.back() == 0)
            from.pop_back();
        again = true;
    }
}

bool drop(const key_space_ptr& source, const std::string& name, std::string& err) {
    definition d;
    if (!load(source, name, d, err))
        return false;
    auto ix = barch::get_keyspace(index_space_name(source));
    std::string lo = join_open({string_component(name)});
    std::string hi = lo;
    hi.back() = (char) (key_terminator + 1);
    barch::sharded_store store(ix);
    std::vector<std::string> gone;
    each_key(ix, lo, hi, [&](const std::vector<std::string>& batch) {
        gone.insert(gone.end(), batch.begin(), batch.end());
    });
    for (const auto& k : gone)
        store.remove(art::value_type{k.data(), k.size()}, [](const art::node_ptr&) {});
    auto acc = barch::functions::store_for_owner(config_space());
    if (acc.remove)
        acc.remove(def_key(source, name));
    return true;
}

// ---- build and find -----------------------------------------------------------------

bool build(const key_space_ptr& source, definition& def, size_t chain, build_result& out,
           std::string& err) {
    const auto& cs = chains_for(def.fields);
    if (chain >= cs.chains.size()) {
        err = "index " + def.name + " has chains 0 to " + std::to_string(cs.chains.size() - 1);
        return false;
    }
    const auto& order = cs.chains[chain];
    auto ix = barch::get_keyspace(index_space_name(source));
    barch::sharded_store dst(ix);
    const std::string name_part = string_component(def.name);
    const std::string chain_part = chain_component(chain);
    const std::string nothing;
    art::key_options opts;

    // the source's composite keys, in order, a page at a time, copied out before
    // anything is written so no source lock is held while the index is
    std::string lo(1, (char) art::tplain), hi(1, (char) (art::tplain + 1));
    std::vector<std::string> parts, path;
    each_key(source, lo, hi, [&](const std::vector<std::string>& batch) {
        for (const auto& k : batch) {
            if (!split(art::value_type{k.data(), k.size()}, parts) ||
                parts.size() != def.fields + def.pinned) {
                ++out.skipped;
                continue;
            }
            path.clear();
            path.push_back(name_part);
            path.push_back(chain_part);
            for (uint8_t f : order)
                path.push_back(parts[f]);
            for (size_t t = def.fields; t < parts.size(); ++t)
                path.push_back(parts[t]);
            auto key = join(path);
            dst.insert(opts, art::value_type{key.data(), key.size()},
                       art::value_type{nothing.data(), 0}, true, [](const art::node_ptr&) {});
            ++out.records;
        }
    });
    if (!def.is_ready(chain)) {
        def.ready.push_back(chain);
        std::sort(def.ready.begin(), def.ready.end());
    }
    return store_def(source, def, err);
}

bool find(const key_space_ptr& source, const definition& def,
          const std::vector<std::pair<size_t, std::string>>& equal, int64_t limit,
          std::vector<std::string>& keys, std::string& err) {
    keys.clear();
    size_t mask = 0;
    for (const auto& [field, value] : equal) {
        if (field >= def.fields) {
            err = "index " + def.name + " has fields 0 to " + std::to_string(def.fields - 1);
            return false;
        }
        if (mask & ((size_t) 1 << field)) {
            err = "field " + std::to_string(field) + " is given twice";
            return false;
        }
        mask |= (size_t) 1 << field;
    }
    const auto& cs = chains_for(def.fields);
    const size_t chain = cs.chain_of[mask];
    if (!def.is_ready(chain)) {
        err = "chain " + std::to_string(chain) + " of index " + def.name +
              " isn't built: INDEX BUILD " + def.name + " " + std::to_string(chain);
        return false;
    }
    const auto& order = cs.chains[chain];
    std::vector<std::string> head{string_component(def.name), chain_component(chain)};
    for (size_t j = 0; j < equal.size(); ++j) {
        size_t f = order[j];
        for (const auto& [field, value] : equal)
            if (field == f)
                head.push_back(component_of(value));
    }
    std::string lo, hi;
    if (equal.size() == def.fields && def.pinned == 0) {
        lo = join(head);                // the whole path: one key
        lo.pop_back();                  // bound() puts its final zero back
        hi = lo;
        hi.push_back((char) key_terminator);
    } else {
        lo = join_open(head);
        hi = lo;
        hi.back() = (char) (key_terminator + 1);
    }
    auto ix = barch::get_keyspace(index_space_name(source));
    std::vector<std::string> paths;
    range_keys(ix, lo, hi, limit, paths);
    std::vector<std::string> parts;
    for (const auto& p : paths) {
        if (!split(art::value_type{p.data(), p.size()}, parts) ||
            parts.size() != 2 + def.fields + def.pinned)
            continue;
        std::vector<std::string> rebuilt(def.fields);
        for (size_t j = 0; j < def.fields; ++j)
            rebuilt[order[j]] = parts[2 + j];
        for (size_t t = 2 + def.fields; t < parts.size(); ++t)
            rebuilt.push_back(parts[t]);
        keys.push_back(join(rebuilt));
    }
    return true;
}

}
