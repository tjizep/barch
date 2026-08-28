//
// Created by teejip on 8/23/26.
//

#include "function_api.h"

#include <algorithm>
#include <limits>

#include "composite.h"
#include "conversion.h"
#include "foreign/driver.h"
#include "glob.h"
#include "keys.h"
#include "configuration.h"
#include "rpc_caller.h"
#include "dictionary_compressor.h"
#include "sharded_store.h"
#include "art/key_options.h"
#include "key_type.h"
#include "function_sync.h"

namespace {
    /*
     * The key a function is stored under: a one component composite led by
     * art::ts_function. SETF is SET with a different type byte - same name in, same
     * value - and the space is implied by wherever the command ran, so it is not part
     * of the key.
     *
     * The name is converted with noint set, so a function called "123" stays a string
     * rather than encoding as an integer and landing somewhere else in the order.
     * A function may share a name with a builtin; the dotted form is how you call it.
     */
    art::value_type function_key(composite& q, art::value_type name) {
        return q.create(art::ts_function, {conversion::convert(name, true)});
    }

    /*
     * A function name is a command name, so it is case insensitive, and the stored key
     * carries the folded form.
     *
     * The dispatcher upper-cases whatever arrived before it looks a command up, so a
     * name stored as it was typed could never be found by it - `SETF greet` then
     * `greet` looked for GREET and missed. Folding here makes the key canonical, which
     * also stops a space holding `greet` and `GREET` as two functions no client could
     * tell apart. The space half of a dotted name is not folded: space names are case
     * sensitive, as they are in the `space:CMD` prefix.
     */
    std::string upper_name(art::value_type name) {
        std::string r(name.chars(), name.size);
        for (auto& ch : r) {
            ch = (char) toupper((unsigned char) ch);
        }
        return r;
    }

    /** the whole tfunction span, for walking every function in a space */
    art::value_type function_lo(composite& q) {
        return q.create(art::ts_function, {}, false);
    }

    art::value_type function_hi(composite& q) {
        return q.create(art::ts_function, {art::ts_end});
    }

}

namespace barch {
namespace functions {

    /** the source stored for `name` in `space`, if there is one */
    static bool read_source(const key_space_ptr& space, art::value_type name,
                            std::string& source) {
        if (!space)
            return false;
        auto folded = upper_name(name);
        composite q;
        auto key = function_key(q, art::value_type{folded.data(), folded.size()});
        barch::sharded_store store(space);
        return store.search(key, [&](const art::node_ptr& n) {
            auto v = n.const_leaf()->get_value();
            source.assign(v.chars(), v.size);
        });
    }

    /**
     * where a global function lives: the default key space.
     *
     * It was `configuration` at first, which is the wrong home now that functions are
     * ordinary keys - configuration holds `<name>.foreign` settings and is a store
     * with a job of its own. The default space is where an unqualified client already
     * works, so a function written there being callable everywhere reads as defining
     * something at the top level rather than as a special case.
     *
     * The consequence, which is symmetrical rather than a gap: there is no way to
     * write a function that is *only* in the default space, because the default space
     * is the global namespace.
     */
    static key_space_ptr global_space() {
        return get_default_ks();
    }

    /** one value, for the page walk to fill in after its lock has gone */
    static bool s_get_value(const key_space_ptr& space, const std::string& key,
                            std::string& out) {
        auto converted = space->encode_key(art::value_type{key.data(), key.size()});
        barch::sharded_store store(space);
        return store.search(converted.get_value(), [&](const art::node_ptr& n) {
            auto cl = n.const_leaf();
            auto v = cl->get_value();
            if (cl->is_compressed()) {
                auto d = dictionary::decompress(v);
                out.assign(d.chars(), d.size);
            } else {
                out.assign(v.chars(), v.size);
            }
        });
    }

    /**
     * how the driver asks for a source: this space first, then the globals. The same
     * order a bare name resolves in, so a require reads the way a call does.
     */
    static barch::foreign::source_loader loader_for(const key_space_ptr& space) {
        return [space](const std::string& want, std::string& source) -> bool {
            art::value_type n{want.data(), want.size()};
            if (read_source(space, n, source))
                return true;
            auto global = global_space();
            if (global == space)
                return false;
            return read_source(global, n, source);
        };
    }

    bool source_of(const key_space_ptr& space, const std::string& name, std::string& out) {
        auto folded = upper_name(art::value_type{name.data(), name.size()});
        return loader_for(space)(folded, out);
    }

    /** does this user hold every category the command asks for? */
    static bool allowed(const heap::vector<bool>& needs, const heap::vector<bool>& has) {
        if (has.size() < needs.size())
            return false;
        for (size_t i = 0; i < needs.size(); ++i) {
            if (needs[i] && !has[i])
                return false;
        }
        return true;
    }

    /**
     * What `barch.call` does. Runs an ordinary command on a caller of its own and
     * hands the reply back, refusing the kinds a script has no way to survive.
     *
     * The sub-caller is the point: `rpc_caller::call` clears results, errors and args
     * on the way in, so handing a script the caller that is answering the client would
     * destroy the reply being built. `finish_call_buffer` solves the same problem the
     * same way for EXEC.
     */
    /**
     * What one connection reuses across every `barch.call` its scripts make.
     *
     * The sub caller used to be built per command, and an `rpc_caller` is not a cheap
     * thing to build: `update_routes()`, then a real AUTH through the auth shard, plus
     * a function state holder and a copy of the global ACL vector. Since `call()`
     * clears args, errors, results and temp at its head, one caller serves any number
     * of commands - so it is built once, on the first call that needs it, and lives as
     * long as the interface does. See TODO 140.
     *
     * `busy` is for re-entrancy. CALLF is an ordinary built-in, so a script can reach a
     * script, and the inner call would otherwise clear the results the outer is still
     * in the middle of producing. Nesting is rare, so it just builds a fresh caller for
     * the duration rather than keeping a stack of them.
     */
    struct sub_caller_state {
        rpc_caller sub{};
        bool busy = false;
        /** the command table, held rather than re-fetched: a shared_ptr copy per call */
        std::shared_ptr<function_map> table{functions_by_name()};
        /** names already resolved, keyed as the script wrote them so no case folding */
        heap::string_map<const barch_info*> known{};
    };

    static barch::foreign::command_runner runner_for(caller& outer) {
        auto held = std::make_shared<std::shared_ptr<sub_caller_state>>();
        return [&outer, held](const heap::vector<std::string>& argv, Variable& out,
                              std::string& err) -> bool {
            if (argv.empty()) {
                err = "barch.call needs a command name";
                return false;
            }
            if (!*held)
                *held = std::make_shared<sub_caller_state>();
            auto& st = **held;

            /*
             * The name as the script wrote it is the cache key, so a loop calling the
             * same command pays for the case folding and the table lookup once. What
             * is cached is only that the name resolves and to what - every check that
             * can change between calls stays below.
             */
            const barch_info* fn = nullptr;
            auto seen = st.known.find(argv[0]);
            if (seen != st.known.end()) {
                fn = seen->second;
            } else {
                std::string name = argv[0];
                for (auto& ch : name) {
                    ch = (char) toupper((unsigned char) ch);
                }
                // a transaction is the connection's, not the script's. MULTI on a caller
                // nobody is going to EXEC just swallows everything after it
                if (name == "MULTI" || name == "EXEC" || name == "DISCARD" || name == "WATCH"
                    || name == "UNWATCH") {
                    err = "FUNCTION cannot call " + name;
                    return false;
                }
                auto f = st.table->find(name);
                if (f == st.table->end()) {
                    err = "FUNCTION unknown command '" + name + "'";
                    return false;
                }
                // an asynchronous command expects to own a worker and answer later; there
                // is nowhere for that answer to go from inside a script
                if (f->second.is_asynch) {
                    err = "FUNCTION cannot call '" + name + "', it is asynchronous";
                    return false;
                }
                fn = &f->second;
                st.known.emplace(argv[0], fn);
            }
            // the script runs as whoever called it, so this is the check that stops a
            // function being a way round the one the connection would have failed. It
            // is asked every call, never cached: the rights can change under a session
            if (!allowed(fn->cats, outer.get_acl())) {
                err = "FUNCTION not authorized to call '" + argv[0] + "'";
                return false;
            }

            /*
             * How deep this chain has gone. CALLF is an ordinary built-in, so a script
             * reaches another script through here, and each level used to start with a
             * deadline of its own - which meant the deadline bounded one call and not
             * a tree of them. Depth is carried on the caller, so it survives a nested
             * call parking and coming back on another thread. See TODO 98 E.
             */
            int depth = outer.script_depth() + 1;
            if (depth > (int) barch::get_function_max_depth()) {
                err = std::string("FUNCTION ") + barch::foreign::too_deep_marker
                    + ", stopped at " + std::to_string(depth)
                    + " - raise function_max_depth if this is meant to recurse";
                return false;
            }

            // nested calls get their own, see sub_caller_state
            std::unique_ptr<rpc_caller> nested;
            if (st.busy)
                nested = std::make_unique<rpc_caller>();
            rpc_caller& sub = nested ? *nested : st.sub;
            bool mine = !nested;
            if (mine)
                st.busy = true;
            struct release {
                sub_caller_state& s; bool mine; rpc_caller& c;
                // the sub caller outlives the call, so the depth has to be put back or
                // a connection would climb towards the limit one command at a time
                ~release() { if (mine) s.busy = false; c.set_script_depth(0); }
            } rel{st, mine, sub};

            sub.set_kspace(outer.kspace());
            sub.set_script_depth(depth);
            // argv goes straight in - `call` only indexes and iterates it, and copying
            // every argument into a std::vector first was pure cost
            out = sub.callv(argv, fn->call, Variable(nullptr));
            if (sub.has_blocks()) {
                // it parked. Nothing is going to service those blocks, and a parked
                // command has not done anything yet, so refusing here is safe
                sub.clear_blocks();
                err = "FUNCTION cannot call '" + argv[0] + "', it blocks";
                return false;
            }
            if (out.index() == var_error) {
                err = std::get<error>(out).what();
                // the refusal from further down comes back whole rather than wrapped
                // again, so the reason survives the trip up
                if (err.find(barch::foreign::too_deep_marker) != std::string::npos) {
                    auto at = err.find("FUNCTION ");
                    if (at != std::string::npos)
                        err = err.substr(at);
                }
                return false;
            }
            return true;
        };
    }

    /**
     * What `barch.store` reaches. Bound to the space the function runs against.
     *
     * The one rule: each of these takes its lock, copies what it needs and lets the
     * lock go before anything is handed back. `sharded_store::range` calls its
     * callback under a shared lock, so the callback here fills a vector and the Luau
     * table is built afterwards - calling into the script from in there would be
     * script code running under a lock, which is what TODO 98 F forbids.
     */
    /**
     * the character a composite key's components are joined back with.
     *
     * A key holding the space's split is stored as several components, so rendering
     * it has to put the split back or the caller gets a key it never wrote. This is
     * the rule `push_encoded_key` uses, which is what KEYS and MIN answer with, and
     * the two have to agree or a script and a client disagree about the same key.
     *
     * A regex split has no single character to rejoin with and falls back to a space,
     * so a space configured that way cannot round trip its keys exactly. That is true
     * of KEYS today and is not made worse here.
     */
    static char split_char(const key_space_ptr& space) {
        if (space && space->key_split.size() == 1)
            return space->key_split[0];
        return ' ';
    }

    /** the categories an equivalent command would ask for */
    static const heap::vector<bool>& cats_of(const char* a, const char* b) {
        static heap::string_map<heap::vector<bool>> built;
        std::string k = std::string(a) + "|" + b;
        auto it = built.find(k);
        if (it != built.end())
            return it->second;
        catmap m;
        m[a] = true;
        m[b] = true;
        m["data"] = true;
        return built.emplace(k, cats2vec(m)).first->second;
    }

    barch::foreign::store_access store_for(const key_space_ptr& space,
                                           const heap::vector<bool>& acl,
                                           bool owner) {
        barch::foreign::store_access s;
        // what GET and SET ask for. Reading through barch.store is reading, whatever
        // route it took, so it answers to the same categories
        s.may_read = owner || allowed(cats_of("read", "keys"), acl);
        s.may_write = owner || allowed(cats_of("write", "keys"), acl);
        s.may_see_functions = owner || allowed(cats_of("read", "function"), acl);
        const char sep = split_char(space);
        // functions are the top of the key order - tfunction is 12 and sorts after
        // every other lead - so hiding them is a bound rather than a filter: stop the
        // walk where the range begins. Held by value because the composite that built
        // it does not outlive this function.
        //
        // As things stand this clamp cannot fire: a caller's bounds go through
        // encode_key, which produces tstring, tinteger and the rest but never
        // tfunction, so every bound a script can name already sorts below the range.
        // It is here for the day something hands these bounds that were not built
        // from a client's string - min and max are the two that reach the range
        // today, and they filter their single answer below
        composite fq;
        auto fn_start_v = fq.create(art::ts_function, {}, false);
        const std::string fn_start(fn_start_v.chars(), fn_start_v.size);
        const bool hide = !s.may_see_functions;
        /** the caller's upper bound, or where the functions begin, whichever is lower */
        auto clamp_hi = [hide, fn_start](art::value_type hi) -> art::value_type {
            if (!hide)
                return hi;
            art::value_type limit{fn_start.data(), fn_start.size()};
            return hi < limit ? hi : limit;
        };
        s.get = [space](const std::string& key,
                        std::string& value) -> barch::foreign::store_access::read_state {
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            barch::sharded_store store(space);
            // through search_state rather than search, so a cached source miss is not
            // reported as though nothing had ever been looked for - TODO 148
            auto st = store.search_state(converted.get_value(), [&](const art::node_ptr& n) {
                auto cl = n.const_leaf();
                auto v = cl->get_value();
                if (cl->is_compressed()) {
                    auto d = dictionary::decompress(v);
                    value.assign(d.chars(), d.size);
                } else {
                    value.assign(v.chars(), v.size);
                }
            });
            switch (st) {
                case barch::sharded_store::read_state::present:
                    return barch::foreign::store_access::read_state::present;
                case barch::sharded_store::read_state::tombed:
                    return barch::foreign::store_access::read_state::tombed;
                default:
                    return barch::foreign::store_access::read_state::absent;
            }
        };
        s.exists = [space](const std::string& key) -> bool {
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            barch::sharded_store store(space);
            return store.exists(converted.get_value());
        };
        s.count = [space, clamp_hi](const std::string& lo, const std::string& hi) -> int64_t {
            auto l = space->encode_key(art::value_type{lo.data(), lo.size()});
            auto h = space->encode_key(art::value_type{hi.data(), hi.size()});
            barch::sharded_store store(space);
            return store.count(l.get_value(), clamp_hi(h.get_value()));
        };
        s.range = [space, sep, clamp_hi](const std::string& lo, const std::string& hi, int64_t limit,
                          heap::vector<std::string>& out) {
            auto l = space->encode_key(art::value_type{lo.data(), lo.size()});
            auto h = space->encode_key(art::value_type{hi.data(), hi.size()});
            barch::sharded_store store(space);
            // the callback runs under the lock, so it only copies - no Luau here
            store.range(l.get_value(), clamp_hi(h.get_value()), limit, [&](art::value_type key) {
                out.push_back(encoded_key_as_string(key, sep));
            });
        };
        s.min = [space, sep, hide](std::string& key) -> bool {
            barch::sharded_store store(space);
            bool found = false;
            store.minimum([&](art::value_type k) {
                // functions sort last, so the smallest key is only ever one of them in
                // a space that holds nothing else
                if (hide && k.size && k.bytes[0] == art::tfunction)
                    return;
                key = encoded_key_as_string(k, sep);
                found = true;
            });
            return found;
        };
        s.max = [space, sep, hide](std::string& key) -> bool {
            barch::sharded_store store(space);
            bool found = false;
            store.maximum([&](art::value_type k) {
                if (hide && k.size && k.bytes[0] == art::tfunction)
                    return;
                key = encoded_key_as_string(k, sep);
                found = true;
            });
            return found;
        };
        s.set = [space, sep, may_write = s.may_write](const std::string& key,
                                                        const std::string& value,
                                                        std::string& err) -> bool {
            if (!may_write) {
                err = "FUNCTION not authorized to write there";
                return false;
            }
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            auto k = converted.get_value();
            art::value_type v{value.data(), value.size()};
            if (!fits_in_leaf(k.size, v.size)) {
                err = too_large_message();
                return false;
            }
            art::key_options opts;
            auto fc = [](const art::node_ptr&) -> void {};
            barch::sharded_store store(space);
            store.insert(opts, k, v, true, fc);
            return true;
        };
        s.remove = [space](const std::string& key) -> bool {
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            barch::sharded_store store(space);
            auto fc = [](art::node_ptr) -> void {};
            return store.remove(converted.get_value(), fc);
        };
        /*
         * One page of the walk. The lock is held for the copy and nothing else, so no
         * Luau runs while a shard is held and a key erased behind the walk cannot
         * matter - what is being read is a copy. See TODO 98 F2.
         */
        s.page = [space, sep, hide](const std::string& after, size_t want,
                                    heap::vector<barch::foreign::store_access::row>& out,
                                    std::string& next) {
            /*
             * The first page starts below every key there is.
             *
             * It has to be a *valid* encoded key, not just a low byte: the iterators
             * are positioned with a lower bound, and a one byte tinteger lead is not
             * a key any comparison can place - it finds nothing and the walk comes
             * back empty. The smallest int64 is the smallest key, since tinteger is
             * the lowest lead and everything else sorts above it.
             */
            conversion::comparable_key from{std::numeric_limits<int64_t>::min()};
            art::value_type lo = after.empty()
                ? from.get_value()
                : art::value_type{after.data(), after.size()};
            composite fq;
            auto fn_end = fq.create(art::ts_function, {art::ts_end});
            barch::sharded_store store(space);
            store.range(lo, fn_end, (int64_t) want + 1, [&](art::value_type k) {
                if (!k.size)
                    return;
                barch::foreign::store_access::row r;
                if (art::is_container_lead(*k.bytes)) {
                    switch (*k.bytes) {
                        case art::tcomposite_list: r.type = "list"; break;
                        case art::tcomposite_hash: r.type = "hash"; break;
                        case art::tcomposite_ordered_map: r.type = "orderedset"; break;
                        default: return;          // the bookkeeping keys name nothing
                    }
                    r.container = encoded_container_name(k);
                    if (r.container.empty())
                        return;                   // an ordered set's member index
                    unsigned nl = encoded_container_name_len(k);
                    if (nl && k.size > nl)
                        r.key = encoded_key_as_string(k.sub(nl), sep);
                } else if (*k.bytes == art::tfunction) {
                    if (hide)
                        return;                   // not this user's business - F4
                    r.type = "function";
                    r.key = encoded_key_as_string(k.sub(composite_key_size), sep);
                } else {
                    r.type = "key";
                    r.key = encoded_key_as_string(k, sep);
                }
                // the range starts *at* the key the last page ended on, so it comes
                // back first and has to go. Without this a final page holding only
                // that key would hand back the same row for ever
                std::string encoded(k.chars(), k.size);
                if (encoded == after)
                    return;
                next = encoded;
                out.push_back(std::move(r));
            });
            // deliberately no values here. Reading one per row would be a store
            // lookup for every key the walk passes, which is exactly what the lazy
            // row object exists to avoid: a filter that looks at keys and wants the
            // value of a few should pay for those few. The row reads it when asked
        };
        /*
         * Containers. One lead per kind, so the kind is looked up once and the same
         * key shape serves a list, a hash and an ordered set - see key_type.h.
         */
        auto lead_of = [](barch::container_kind k) -> art::composite_type {
            switch (k) {
                case barch::container_kind::list: return art::ts_list;
                case barch::container_kind::hash: return art::ts_hash;
                default: return art::ts_ordered_map;
            }
        };
        /*
         * An ordered set's member entry does not hold the score. It holds the key of the
         * score index - the container's prefix, then the encoded score, then the member -
         * because that is what ZADD writes so it can find and unlink the old index entry
         * when a score changes. So a reader has to cut the score back out; handing Luau
         * the raw bytes would give it something tonumber() cannot read. See ordered_api.
         */
        auto value_text = [](barch::container_kind kind, art::value_type n,
                             art::value_type raw, std::string& out) -> void {
            if (kind != barch::container_kind::ordered_map
                || raw.size < numeric_key_size) {
                out.assign(raw.chars(), raw.size);
                return;
            }
            composite pfxq;
            auto prefix = pfxq.create(art::ts_ordered_map, {conversion::convert(n)}, false);
            if (raw.size < prefix.size + numeric_key_size) {
                out.assign(raw.chars(), raw.size);
                return;
            }
            double d = conversion::enc_bytes_to_dbl(raw.sub(prefix.size, numeric_key_size));
            char buf[40];
            auto len = snprintf(buf, sizeof(buf), "%.17g", d);
            out.assign(buf, len > 0 ? (size_t) len : 0);
        };
        /*
         * An ordered set keeps two families of key under its name: the member index,
         * {IX_MEMBER, name, member}, and the score index, {name, score, member}. Only
         * the first is one entry per member, so that is the one to read and walk - a
         * walk of the plain {name, ...} prefix lands on the score index and hands back
         * the score where the member belongs. A list and a hash have the one shape.
         */
        auto member_key = [lead_of](composite& q, barch::container_kind kind,
                                    art::value_type n, art::value_type member) {
            if (kind == barch::container_kind::ordered_map)
                return q.create(art::ts_ordered_map, {conversion::empty_component(),
                                conversion::convert(n), conversion::convert(member)});
            return q.create(lead_of(kind), {conversion::convert(n),
                            conversion::convert(member)});
        };
        auto member_prefix = [lead_of](composite& q, barch::container_kind kind,
                                       art::value_type n, bool terminate) {
            if (kind == barch::container_kind::ordered_map)
                return terminate
                    ? q.create(art::ts_ordered_map, {conversion::empty_component(),
                               conversion::convert(n), art::ts_end})
                    : q.create(art::ts_ordered_map, {conversion::empty_component(),
                               conversion::convert(n)}, false);
            return terminate
                ? q.create(lead_of(kind), {conversion::convert(n), art::ts_end})
                : q.create(lead_of(kind), {conversion::convert(n)}, false);
        };
        s.container_kind = [space](const std::string& name) -> std::string {
            barch::sharded_store store(space);
            auto k = barch::kind_of_container(store, art::value_type{name.data(), name.size()});
            switch (k) {
                case barch::container_kind::list: return "list";
                case barch::container_kind::hash: return "hash";
                case barch::container_kind::ordered_map: return "orderedset";
                default: return "";
            }
        };
        s.container_get = [space, may_read = s.may_read, member_key, value_text](
                const std::string& name, const std::string& member,
                std::string& value) -> bool {
            if (!may_read)
                return false;
            barch::sharded_store store(space);
            art::value_type n{name.data(), name.size()};
            auto kind = barch::kind_of_container(store, n);
            if (kind == barch::container_kind::none)
                return false;
            composite q;
            auto key = member_key(q, kind, n,
                                  art::value_type{member.data(), member.size()});
            // a container's keys all live on the shard its *name* routes to, so the
            // lookup has to go there rather than wherever the whole key hashes -
            // which is why hash_api uses these scopes and not a plain search
            bool found = false;
            store.with_container_read(n, [&](const barch::shard_ptr& t) {
                auto node = t->search(key);
                if (node.null() || !node.is_leaf)
                    return;
                value_text(kind, n, node.const_leaf()->get_value(), value);
                found = true;
            });
            return found;
        };
        s.container_set = [space, may_write = s.may_write, member_key](
                const std::string& name, const std::string& member,
                const std::string& value, std::string& err) -> bool {
            if (!may_write) {
                err = "FUNCTION not authorized to write there";
                return false;
            }
            barch::sharded_store store(space);
            art::value_type n{name.data(), name.size()};
            auto kind = barch::kind_of_container(store, n);
            if (kind == barch::container_kind::none) {
                err = "no such container";
                return false;
            }
            if (kind == barch::container_kind::ordered_map) {
                // a member of an ordered set is two keys that have to agree, and the old
                // score index entry has to be unlinked as well. Writing just the member
                // index here would leave the set readable but wrong, so send the caller
                // to the command that does it properly
                err = "FUNCTION cannot write an ordered set member directly, use ZADD";
                return false;
            }
            composite q;
            auto key = member_key(q, kind, n,
                                  art::value_type{member.data(), member.size()});
            art::value_type v{value.data(), value.size()};
            if (!fits_in_leaf(key.size, v.size)) {
                err = too_large_message();
                return false;
            }
            art::key_options opts;
            auto fc = [](const art::node_ptr&) -> void {};
            store.with_container_write(n, [&](const barch::shard_ptr& t) {
                t->insert(opts, key, v, true, fc);
            });
            return true;
        };
        s.container_del = [space, may_write = s.may_write, member_key](
                const std::string& name, const std::string& member) -> bool {
            if (!may_write)
                return false;
            barch::sharded_store store(space);
            art::value_type n{name.data(), name.size()};
            auto kind = barch::kind_of_container(store, n);
            if (kind == barch::container_kind::none)
                return false;
            if (kind == barch::container_kind::ordered_map)
                return false;               // two keys to unlink - ZREM does it, see above
            composite q;
            auto key = member_key(q, kind, n,
                                  art::value_type{member.data(), member.size()});
            auto fc = [](art::node_ptr) -> void {};
            bool gone = false;
            store.with_container_write(n, [&](const barch::shard_ptr& t) {
                gone = t->remove(key, fc);
            });
            return gone;
        };
        s.container_page = [space, sep, may_read = s.may_read, member_prefix, value_text](
                const std::string& name, const std::string& after, size_t want,
                heap::vector<std::pair<std::string, std::string>>& out,
                std::string& next) {
            if (!may_read)
                return;
            barch::sharded_store store(space);
            art::value_type n{name.data(), name.size()};
            auto kind = barch::kind_of_container(store, n);
            if (kind == barch::container_kind::none)
                return;
            composite qpfx, qlo, qhi;
            auto pfx = member_prefix(qpfx, kind, n, false);
            auto lo = after.empty() ? pfx : art::value_type{after.data(), after.size()};
            auto hi = member_prefix(qhi, kind, n, true);
            (void) qlo;
            // every member is on the one shard the name routes to, so this walks
            // that shard rather than ranging over the space
            store.with_container_read(n, [&](const barch::shard_ptr& t) {
                size_t taken = 0;
                for (art::iterator i(t, lo); i.ok() && taken < want; i.next()) {
                    auto k = i.key();
                    if (!(k < hi))
                        break;
                    std::string encoded(k.chars(), k.size);
                    if (encoded == after)
                        continue;                 // the key the last page ended on
                    // the name slice is known from the prefix that was built, and an
                    // ordered set's differs from what encoded_container_name_len measures
                    unsigned plen = pfx.size;
                    if (k.size <= plen)
                        continue;                 // the container's own bookkeeping
                    std::string value;
                    value_text(kind, n, i.value(), value);
                    out.emplace_back(encoded_key_as_string(k.sub(plen), sep),
                                     std::move(value));
                    next = encoded;
                    ++taken;
                }
            });
        };
        s.config = [space](heap::vector<std::pair<std::string, std::string>>& out) {
            out.emplace_back("name", space->get_canonical_name());
            out.emplace_back("shards", std::to_string(space->get_shard_count()));
            out.emplace_back("ordered", space->opt_ordered_keys ? "1" : "0");
            out.emplace_back("hybrid", space->opt_hybrid_keys ? "1" : "0");
            out.emplace_back("range_sharded", space->opt_range_sharded ? "1" : "0");
            out.emplace_back("foreign", space->foreign_kind_name());
            out.emplace_back("key_split", space->key_split);
        };
        /*
         * The locked region - TODO 98 F6.
         *
         * A write lock, not a read one, because the point is read-modify-write and
         * because a write lock also stops a rebalance moving the key out from under
         * the hold. One shard when a key is named, every shard in shard order when one
         * is not - never an arbitrary subset, which is what the ordering has to defend
         * against, since a fan out holding one at a time cannot form a cycle with it.
         *
         * The thread local hold is set only while the guards are alive, so every
         * acquire inside the body finds its shard already held and skips its own -
         * the shard mutex is not recursive, so that is correctness, not economy.
         */
        s.locked = [space, may_write = s.may_write](
                const std::string& key, const std::function<bool()>& body,
                std::string& err) -> bool {
            if (!may_write) {
                err = "FUNCTION not authorized to write there";
                return false;
            }
            auto& held = barch::shard_hold::current();
            if (held.space) {
                // a region inside a region: the inner one is already covered by the
                // outer hold, and re-entering would clear the hold when it left
                err = "FUNCTION already inside a locked region";
                return false;
            }
            barch::sharded_store store(space);
            /** puts the hold back however the body leaves */
            struct release {
                ~release() { barch::shard_hold::current() = {}; }
            } rel;

            bool ran = false;
            if (key.empty()) {
                // the whole space. Taken in shard order, which is the only ordering
                // that matters here - see F6
                heap::vector<storage_release> all;
                store.each_shard([&](const barch::shard_ptr& t) {
                    all.emplace_back(t);
                });
                held.space = space.get();
                held.all = true;
                ran = body();
            } else {
                // routed exactly the way a read of the same key is, or the region
                // would lock a shard the body then does not use
                auto converted = space->encode_key(art::value_type{key.data(), key.size()});
                auto t = store.shard_for(converted.get_value());
                if (!t) {
                    err = "FUNCTION no shard for that key";
                    return false;
                }
                storage_release one(t);
                held.space = space.get();
                held.shard = t.get();
                ran = body();
            }
            return ran;
        };
        s.shard_number = [space](const std::string& key) -> int64_t {
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            return (int64_t) space->get_shard_index(converted.get_value());
        };
        s.has_lock = [space](const std::string& key) -> bool {
            barch::sharded_store store(space);
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            auto t = store.shard_for(converted.get_value());
            if (!t)
                return false;
            return barch::shard_hold::current().covers(space.get(), t.get());
        };
        s.size = [space]() -> int64_t {
            int64_t n = 0;
            for (const auto& sh : space->get_shards())
                n += (int64_t) sh->get_size();
            return n;
        };
        return s;
    }

    barch::foreign::store_access store_for(const key_space_ptr& space,
                                           const heap::vector<bool>& acl) {
        return store_for(space, acl, false);
    }

    barch::foreign::store_access store_for_owner(const key_space_ptr& space) {
        heap::vector<bool> none;
        return store_for(space, none, true);
    }

    bool install(const key_space_ptr& space, const std::string& name,
                 const std::string& source, std::string& err) {
        if (!space) {
            err = "no key space";
            return false;
        }
        art::value_type raw{name.data(), name.size()};
        if (key_ok(raw) != 0 || name.empty()) {
            err = "a function needs a name";
            return false;
        }
        // a stored SET does not overload SET. The dotted form HNSW.SET is how you
        // call it; HNSW:SET and a bare SET stay the builtin. See TODO 160.
        auto folded = upper_name(raw);
        if (!barch::foreign::compile_function(space->get_canonical_name(), folded, source,
                                              loader_for(space), err))
            return false;
        composite q;
        auto key = function_key(q, art::value_type{folded.data(), folded.size()});
        if (!fits_in_leaf(key.size, source.size())) {
            err = too_large_message();
            return false;
        }
        art::key_options opts;
        auto fc = [](const art::node_ptr&) -> void {};
        barch::sharded_store store(space);
        store.insert(opts, key, art::value_type{source.data(), source.size()}, true, fc);
        return true;
    }

    bool remove(const key_space_ptr& space, const std::string& name) {
        if (!space) return false;
        auto folded = upper_name(art::value_type{name.data(), name.size()});
        composite q;
        auto key = function_key(q, art::value_type{folded.data(), folded.size()});
        barch::sharded_store store(space);
        auto fc = [](art::node_ptr) -> void {};
        return store.remove(key, fc);
    }

    heap::vector<std::string> names(const key_space_ptr& space) {
        heap::vector<std::string> out;
        if (!space) return out;
        composite qlo, qhi;
        auto lo = function_lo(qlo);
        auto hi = function_hi(qhi);
        barch::sharded_store store(space);
        store.range(lo, hi, -1, [&](art::value_type key) {
            if (key.size <= composite_key_size)
                return;
            out.push_back(encoded_key_as_string(key.sub(composite_key_size)));
        });
        std::sort(out.begin(), out.end());
        return out;
    }

    bool source_in(const key_space_ptr& space, const std::string& name, std::string& out) {
        return read_source(space, art::value_type{name.data(), name.size()}, out);
    }

    /**
     * Run the function `name` out of `from` (the caller's space when it is null), with
     * argv from `first` onwards as its arguments.
     */
    static int run(caller& call, const key_space_ptr& from, art::value_type name,
                   const arg_t& argv, size_t first) {
        auto space = from ? from : call.kspace();
        auto folded = upper_name(name);
        // only asked when the function is not already compiled on this connection, so
        // a warm call does not touch the store at all


        heap::vector<std::string> args;
        args.reserve(argv.size() > first ? argv.size() - first : 0);
        for (size_t i = first; i < argv.size(); ++i) {
            args.emplace_back(argv[i].chars(), argv[i].size);
        }

        // the function runs against the caller's space, which is not always the space
        // it was loaded from - see TODO 98 B
        // the state is cached per space the function was *loaded* from, since that is
        // where its requires will resolve. What it runs against is the caller's space
        const std::string& defined_in = space->canonical();
        // parked, not run here: the script goes on the foreign pool in slices and the
        // connection waits, so a long function owns no thread while it runs.
        //
        // The wake goes through a key nobody writes, on this space's shard 0, because
        // that is the machinery there is - `call_unblock` wakes whoever is registered
        // under a key. A stray write to that name would wake us early, which is what
        // the `finished` check in the callback is for: an unfinished job re-parks.
        // See TODO 98 H.
        struct parked {
            std::atomic<bool> finished{false};
            bool ok{false};
            Variable out{nullptr};
            std::string err{};
            /*
             * Filled only if the call actually parks. Most do not - a script that
             * finishes inside its first slice never registers a waiter - and naming
             * a key nobody will wait on cost a string and a shard lookup on every
             * call. Empty means there is nobody to wake. See TODO 98 H.
             */
            std::string wake_key{};
            key_space_ptr wake_space{};
            /*
             * Published after the key is written, and read before it. The job can
             * finish on the pool while this thread is still filling the key in, and
             * a reader that took a half written string would be a data race. A wake
             * that arrives too early does nothing; `after_blocks_registered` then
             * sees the job finished and wakes again, which is what it is for.
             */
            std::atomic<bool> can_wake{false};
        };
        auto slot = std::make_shared<parked>();
        auto wake = [slot]() {
            if (!slot->can_wake.load(std::memory_order_acquire))
                return;                       // not parked, or not published yet
            auto shard = slot->wake_space->get((size_t) 0);
            if (shard)
                shard->call_unblock(slot->wake_key);
        };

        /*
         * The interface a script reaches is built once and kept on the connection.
         * It depends on the key space and the user's rights there, not on the script
         * or its arguments, and rebuilding it per call was most of the 1.8us an empty
         * function cost. See TODO 98 F5.
         */
        const std::string& running_in = call.kspace()->canonical();
        auto& held = call.script_interface();
        if (!held || held->running_in != running_in || held->defined_in != defined_in) {
            auto built = std::make_shared<barch::foreign::call_interface>();
            built->running_in = running_in;
            built->defined_in = defined_in;
            built->load = loader_for(space);
            built->run_command = runner_for(call);
            // reads go against the space the call runs in, not the one it was defined
            // in - a function is written against an interface, not a space
            built->store = store_for(call.kspace(), call.get_space_acl());
            /*
             * `barch.space.other.k` reaches a space the call did not start in, so the
             * rights are asked for again *there* rather than inherited - which is what
             * per space ACLs are for, TODO 135. An unknown name is refused and must
             * never build a space as a side effect of being mentioned.
             */
            built->open_space =
                [&call](const std::string& name,
                        barch::foreign::store_access& out) -> bool {
                    if (!barch::is_keyspace(name))
                        return false;
                    auto other = barch::get_keyspace(name);
                    if (!other)
                        return false;
                    out = store_for(other, call.acl_for(other->get_canonical_name()));
                    return true;
                };
            held = built;
        }

        barch::foreign::start_function(
            defined_in, folded, held, args,
            // a slice and a deadline of the function's own, not foreign's - see 98 I.2
            call.kspace()->function_slice(),
            call.kspace()->function_deadline(), call.function_states(),
            [slot, wake](bool ok, Variable value, std::string failed) {
                slot->ok = ok;
                slot->out = std::move(value);
                slot->err = std::move(failed);
                slot->finished.store(true);
                wake();
            });

        // a script that finished on this thread answers here, with none of the parking
        // machinery touched. That is most of them: a one line function costs about
        // 20us to park and microseconds to run
        if (slot->finished.load()) {
            if (!slot->ok)
                return call.push_error(slot->err.empty() ? "FUNCTION failed"
                                                         : slot->err.c_str());
            return call.push_variable(slot->out);
        }

        // it yielded, so it is on the pool now and this connection waits for it.
        // Only here does a wake key exist to wait on
        static std::atomic<uint64_t> ticket{0};
        slot->wake_space = call.kspace();
        slot->wake_key = "\x01function.wake." + std::to_string(++ticket);
        slot->can_wake.store(true, std::memory_order_release);
        caller::keys_t blocks;
        blocks.emplace_back(slot->wake_key, 0);
        call.add_block(blocks, slot->wake_space->waiter_timeout_ms(),
                       [slot](caller& c, const caller::keys_t&) {
                           if (!slot->finished.load()) {
                               c.retry_block();   // woken by something else; stay parked
                               return;
                           }
                           if (!slot->ok) {
                               c.push_error(slot->err.empty() ? "FUNCTION failed"
                                                              : slot->err.c_str());
                               return;
                           }
                           c.push_variable(slot->out);
                       });
        // the session puts the waiter on its shard only after this command returns, so
        // a job that finishes in the meantime would wake nothing. This is that re-check
        call.set_blocks_registered([slot, wake]() {
            if (slot->finished.load())
                wake();
        });
        return call.ok();
    }





    /** is there a function of this name to run? */
    static bool exists_in(const key_space_ptr& space, art::value_type name) {
        if (!space)
            return false;
        auto folded = upper_name(name);
        composite q;
        auto key = function_key(q, art::value_type{folded.data(), folded.size()});
        barch::sharded_store store(space);
        return store.exists(key);
    }

    const barch_function* resolve(caller& call, const std::string& from_space,
                                  const std::string& name) {
        if (name.empty())
            return nullptr;
        // remembered per connection, and only when it resolved. A miss is looked up
        // again every time, which is what lets SETF then a call work on one connection
        auto* known = call.resolutions(call.kspace()->canonical());
        std::string key = from_space.empty() ? name : from_space + "." + name;
        if (known) {
            auto hit = known->find(key);
            if (hit != known->end())
                return &hit->second;
        }
        art::value_type n{name.data(), name.size()};
        key_space_ptr from;
        if (!from_space.empty()) {
            // a dotted name says which space the definition comes from. An unknown one
            // is not a function, and must not build the space as a side effect of a
            // client sending a name with a dot in it
            if (!barch::is_keyspace(from_space))
                return nullptr;
            from = barch::get_keyspace(from_space);
            if (!exists_in(from, n))
                return nullptr;
        } else if (exists_in(call.kspace(), n)) {
            from = call.kspace();
        } else {
            // globals live in the default space, callable from anywhere
            auto global = global_space();
            if (global == call.kspace() || !exists_in(global, n))
                return nullptr;
            from = global;
        }
        barch_function built = [from, name](caller& c, const arg_t& argv) -> int {
            return run(c, from, art::value_type{name.data(), name.size()}, argv, 1);
        };
        if (!known) {
            // nowhere to keep it - the swig and module paths - so it lives for this
            // call only
            static thread_local barch_function scratch;
            scratch = std::move(built);
            return &scratch;
        }
        return &known->emplace(std::move(key), std::move(built)).first->second;
    }

}
}

extern "C" {
/* SETF <name> <source>
 *
 * Store a Luau function under name. The source is compiled before anything is written,
 * so a script that will not compile is refused rather than saved as a command that
 * cannot run. A name that is already a builtin is allowed: SET stays SET, and the
 * stored one is reached as SPACE.SET. See TODO 160.
 */
int SETF(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto name = argv[1];
    auto source = argv[2];
    if (key_ok(name) != 0)
        return call.key_check_error(name);
    std::string err;
    if (!barch::functions::install(call.kspace(),
                                   {name.chars(), name.size},
                                   {source.chars(), source.size}, err))
        return call.push_error(err.c_str());
    return call.push_simple("OK");
}

/* GETF <name> - the source back, or null when there is no such function */
int GETF(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto name = argv[1];
    if (key_ok(name) != 0)
        return call.key_check_error(name);
    auto folded = upper_name(name);
    composite q;
    auto key = function_key(q, art::value_type{folded.data(), folded.size()});
    barch::sharded_store store(call.kspace());
    int r = call.ok();
    bool found = store.search(key, [&](const art::node_ptr& n) {
        r = call.push_bulk(n.const_leaf()->get_value());
    });
    return found ? r : call.push_null();
}

/* REMF <name> - 1 if a function went, 0 if there was nothing there */
int REMF(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto name = argv[1];
    if (key_ok(name) != 0)
        return call.key_check_error(name);
    auto folded = upper_name(name);
    composite q;
    auto key = function_key(q, art::value_type{folded.data(), folded.size()});
    barch::sharded_store store(call.kspace());
    auto fc = [](art::node_ptr) -> void {};
    return call.push_ll(store.remove(key, fc) ? 1 : 0);
}

/* CALLF <name> [arg...]
 *
 * Run a stored function and answer with what it returned. The arguments arrive as a
 * 1-based array of strings, the command name and the function name not among them:
 *
 *     function call(argv) return "hello " .. argv[1] end
 *
 * Marked asynchronous, so it runs on the worker pool rather than a service thread.
 * That still holds one worker for the whole call, bounded by the instruction budget
 * and the deadline - the same deal KEYS has. Parking is TODO 98 H.
 */
int CALLF(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    auto name = argv[1];
    if (key_ok(name) != 0)
        return call.key_check_error(name);
    // CALLF names the function in argv[1], so its arguments start one later than they
    // do when the function is called by its own name
    return barch::functions::run(call, call.kspace(), name, argv, 2);
}

/* KEYSF [pattern] - the names of the functions in this space
 *
 * Cheap because the tfunction keys are contiguous: this is a range walk over one span
 * rather than a scan of the whole space.
 */
int KEYSF(caller& call, const arg_t& argv) {
    if (argv.size() > 2)
        return call.wrong_arity();
    art::value_type pattern = argv.size() == 2 ? argv[1] : art::value_type{"*"};
    composite qlo, qhi;
    auto lo = function_lo(qlo);
    auto hi = function_hi(qhi);
    barch::sharded_store store(call.kspace());
    heap::vector<std::string> names;
    store.range(lo, hi, -1, [&](art::value_type key) {
        if (key.size <= composite_key_size)
            return;
        // past the {lead, 0x00} is the name, encoded as an ordinary key
        auto name = encoded_key_as_string(key.sub(composite_key_size));
        if (glob::stringmatchlen(pattern, art::value_type(name.data(), name.size()), 0))
            names.push_back(name);
    });
    // range walks each shard in turn, so the names arrive per shard rather than in
    // order. A function list nobody can predict the order of is not much of a list
    std::sort(names.begin(), names.end());
    call.start_array();
    for (const auto& n : names) {
        call.push_string(n);
    }
    return call.end_array();
}

/* FUNCTIONS SYNC | STATUS
 *
 * SYNC applies the checkout in functions_dir. STATUS is the last result.
 */
int FUNCTIONS(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    std::string sub(argv[1].chars(), argv[1].size);
    for (auto& ch : sub)
        ch = (char) toupper((unsigned char) ch);
    if (sub == "STATUS")
        return call.push_string(barch::functions_sync_status());
    if (sub == "SYNC") {
        auto err = barch::sync_functions();
        if (!err.empty())
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    return call.push_error("FUNCTIONS SYNC|STATUS");
}
}

void register_function_api(function_map& r) {
    r["SETF"] = {::SETF,{"write","data","function"}};
    r["GETF"] = {::GETF,{"read","data","function"}};
    r["REMF"] = {::REMF,{"write","data","function"}};
    r["KEYSF"] = {::KEYSF,{"read","data","function"}};
    // not asynchronous any more: a call parks and the script runs in slices on the
    // foreign pool, so it owns no thread at all while it waits. See TODO 98 H
    r["CALLF"] = {::CALLF,{"read","data","function"}};
    r["FUNCTIONS"] = {::FUNCTIONS,{"write","data","function","admin"}};
}
