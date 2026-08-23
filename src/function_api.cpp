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

    std::string err;
    if (!barch::foreign::compile_function({source.chars(), source.size}, err))
        return call.push_error(err.c_str());

    composite q;
    auto key = function_key(q, name);
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
    composite q;
    auto key = function_key(q, name);
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
    composite q;
    auto key = function_key(q, name);
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

    composite q;
    auto key = function_key(q, name);
    barch::sharded_store store(call.kspace());
    std::string source;
    bool found = store.search(key, [&](const art::node_ptr& n) {
        auto v = n.const_leaf()->get_value();
        source.assign(v.chars(), v.size);
    });
    if (!found)
        return call.push_error("no such function");

    heap::vector<std::string> args;
    args.reserve(argv.size() - 2);
    for (size_t i = 2; i < argv.size(); ++i) {
        args.emplace_back(argv[i].chars(), argv[i].size);
    }

    Variable out;
    std::string err;
    auto space = call.kspace()->get_canonical_name();
    if (!barch::foreign::call_function(space, source, args, barch::get_foreign_script_insns(),
                                       call.kspace()->foreign_query_timeout_ms, out, err))
        return call.push_error(err.empty() ? "FUNCTION failed" : err.c_str());
    return call.push_variable(out);
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
