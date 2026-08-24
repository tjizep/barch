//
// Created by teejip on 8/23/26.
//

#include "function_api.h"

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

namespace {
    /*
     * The key a function is stored under: a one component composite led by
     * art::ts_function. SETF is SET with a different type byte - same name in, same
     * value - and the space is implied by wherever the command ran, so it is not part
     * of the key.
     *
     * The name is converted with noint set, so a function called "123" stays a string
     * rather than encoding as an integer and landing somewhere else in the order.
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

    /** a name that is already a command cannot become a function - see TODO 98 */
    bool is_builtin_name(art::value_type name) {
        std::string upper(name.chars(), name.size);
        for (auto& ch : upper) {
            ch = (char) toupper((unsigned char) ch);
        }
        auto table = functions_by_name();
        return table->find(upper) != table->end();
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
     * how the driver asks for a source: this space first, then the globals in
     * `configuration`. The same order a bare name resolves in, so a require reads the
     * way a call does.
     */
    static barch::foreign::source_loader loader_for(const key_space_ptr& space) {
        return [space](const std::string& want, std::string& source) -> bool {
            art::value_type n{want.data(), want.size()};
            if (read_source(space, n, source))
                return true;
            auto global = barch::get_keyspace("configuration");
            if (global == space)
                return false;
            return read_source(global, n, source);
        };
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
    static barch::foreign::command_runner runner_for(caller& outer) {
        return [&outer](const heap::vector<std::string>& argv, Variable& out,
                        std::string& err) -> bool {
            if (argv.empty()) {
                err = "barch.call needs a command name";
                return false;
            }
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
            auto table = functions_by_name();
            auto f = table->find(name);
            if (f == table->end()) {
                err = "FUNCTION unknown command '" + name + "'";
                return false;
            }
            // an asynchronous command expects to own a worker and answer later; there
            // is nowhere for that answer to go from inside a script
            if (f->second.is_asynch) {
                err = "FUNCTION cannot call '" + name + "', it is asynchronous";
                return false;
            }
            // the script runs as whoever called it, so this is the check that stops a
            // function being a way round the one the connection would have failed
            if (!allowed(f->second.cats, outer.get_acl())) {
                err = "FUNCTION not authorized to call '" + name + "'";
                return false;
            }

            rpc_caller sub;
            sub.set_kspace(outer.kspace());
            std::vector<std::string> params(argv.begin(), argv.end());
            out = sub.callv(params, f->second.call, Variable(nullptr));
            if (sub.has_blocks()) {
                // it parked. Nothing is going to service those blocks, and a parked
                // command has not done anything yet, so refusing here is safe
                sub.clear_blocks();
                err = "FUNCTION cannot call '" + name + "', it blocks";
                return false;
            }
            if (out.index() == var_error) {
                err = std::get<error>(out).what();
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

    static barch::foreign::store_access store_for(const key_space_ptr& space,
                                                  const heap::vector<bool>& acl) {
        barch::foreign::store_access s;
        // what GET and SET ask for. Reading through barch.store is reading, whatever
        // route it took, so it answers to the same categories
        s.may_read = allowed(cats_of("read", "keys"), acl);
        s.may_write = allowed(cats_of("write", "keys"), acl);
        s.may_see_functions = allowed(cats_of("read", "function"), acl);
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
        s.get = [space](const std::string& key, std::string& value) -> bool {
            auto converted = space->encode_key(art::value_type{key.data(), key.size()});
            barch::sharded_store store(space);
            return store.search(converted.get_value(), [&](const art::node_ptr& n) {
                auto cl = n.const_leaf();
                auto v = cl->get_value();
                if (cl->is_compressed()) {
                    auto d = dictionary::decompress(v);
                    value.assign(d.chars(), d.size);
                } else {
                    value.assign(v.chars(), v.size);
                }
            });
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
        s.config = [space](heap::vector<std::pair<std::string, std::string>>& out) {
            out.emplace_back("name", space->get_canonical_name());
            out.emplace_back("shards", std::to_string(space->get_shard_count()));
            out.emplace_back("ordered", space->opt_ordered_keys ? "1" : "0");
            out.emplace_back("range_sharded", space->opt_range_sharded ? "1" : "0");
            out.emplace_back("foreign", space->foreign_kind_name());
            out.emplace_back("key_split", space->key_split);
        };
        return s;
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
        auto load = loader_for(space);

        heap::vector<std::string> args;
        args.reserve(argv.size() > first ? argv.size() - first : 0);
        for (size_t i = first; i < argv.size(); ++i) {
            args.emplace_back(argv[i].chars(), argv[i].size);
        }

        Variable out;
        std::string err;
        // the function runs against the caller's space, which is not always the space
        // it was loaded from - see TODO 98 B
        // the state is cached per space the function was *loaded* from, since that is
        // where its requires will resolve. What it runs against is the caller's space
        auto defined_in = space->get_canonical_name();
        auto run_command = runner_for(call);
        // reads go against the space the call is running in, not the one the function
        // was defined in - a function is written against an interface, not a space
        auto store_reads = store_for(call.kspace(), call.get_acl());
        if (!barch::foreign::call_function(defined_in, folded, load, run_command,
                                           store_reads, args,
                                           barch::get_foreign_script_insns(),
                                           call.kspace()->foreign_query_timeout_ms,
                                           call.function_states(), out, err))
            return call.push_error(err.empty() ? "FUNCTION failed" : err.c_str());
        return call.push_variable(out);
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

    bool resolve(caller& call, const std::string& from_space, const std::string& name,
                 barch_function& out) {
        if (name.empty())
            return false;
        art::value_type n{name.data(), name.size()};
        key_space_ptr from;
        if (!from_space.empty()) {
            // a dotted name says which space the definition comes from. An unknown one
            // is not a function, and must not build the space as a side effect of a
            // client sending a name with a dot in it
            if (!barch::is_keyspace(from_space))
                return false;
            from = barch::get_keyspace(from_space);
            if (!exists_in(from, n))
                return false;
        } else if (exists_in(call.kspace(), n)) {
            from = call.kspace();
        } else {
            // globals live in the configuration space, callable from anywhere
            auto global = barch::get_keyspace("configuration");
            if (!exists_in(global, n))
                return false;
            from = global;
        }
        out = [from, name](caller& c, const arg_t& argv) -> int {
            return run(c, from, art::value_type{name.data(), name.size()}, argv, 1);
        };
        return true;
    }

}
}

extern "C" {
/* SETF <name> <source>
 *
 * Store a Luau function under name. The source is compiled before anything is written,
 * so a script that will not compile is refused rather than saved as a command that
 * cannot run.
 */
int SETF(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto name = argv[1];
    auto source = argv[2];
    if (key_ok(name) != 0)
        return call.key_check_error(name);
    if (name.size == 0)
        return call.push_error("a function needs a name");
    if (is_builtin_name(name))
        return call.push_error("that name is already a command");
    auto folded = upper_name(name);
    art::value_type stored{folded.data(), folded.size()};

    std::string err;
    // compiled against this space, so a require in the script resolves now and a
    // cycle is refused here rather than at the first call
    if (!barch::foreign::compile_function(call.kspace()->get_canonical_name(), folded,
                                          {source.chars(), source.size},
                                          barch::functions::loader_for(call.kspace()), err))
        return call.push_error(err.c_str());

    composite q;
    auto key = function_key(q, stored);
    // the source and the key share a leaf and a leaf has to fit in a page. SET learned
    // the hard way that answering OK and storing nothing is the worst way to be wrong
    if (!fits_in_leaf(key.size, source.size))
        return call.push_error(too_large_message());

    art::key_options opts;
    auto fc = [](const art::node_ptr&) -> void {};
    barch::sharded_store store(call.kspace());
    store.insert(opts, key, source, true, fc);
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
}

void register_function_api(function_map& r) {
    r["SETF"] = {::SETF,{"write","data","function"}};
    r["GETF"] = {::GETF,{"read","data","function"}};
    r["REMF"] = {::REMF,{"write","data","function"}};
    r["KEYSF"] = {::KEYSF,{"read","data","function"}};
    // asynchronous: a script must not sit on a service thread. See CALLF
    r["CALLF"] = {::CALLF,{"read","data","function"}, true};
}
