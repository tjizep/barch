//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "config_api.h"
#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>

#include "barch_apis.h"
#include "caller.h"
#include "vk_caller.h"
#include "module.h"
#include "conversion.h"
#include "version.h"
#include "glob.h"
#include "keys.h"
#include "art/art.h"
#include "art/iterator.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "sharded_store.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"
#include "statistics.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "auth_api.h"
#include "rpc/server.h"
#include "rpc/restarter.h"
#include "rpc/redis_parser.h"

extern "C" {
#include "../external/include/valkeymodule.h"
}

extern "C" {

int CONFIG(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    auto s = argv[1];
    auto keyword_is = [&s](const char* lower, const char* upper) {
        return strncmp(lower, s.chars(), s.size) == 0 || strncmp(upper, s.chars(), s.size) == 0;
    };
    if (keyword_is("set", "SET")) {
        if (argv.size() != 4)
            return call.wrong_arity();
        std::string name = argv[2].chars(), why;
        // a setting barch reports but cannot change says so, rather than failing with
        // the same message as a value it could not parse
        if (barch::is_read_only_configuration(name, why)) {
            std::string msg = "cannot set '" + name + "': " + why;
            return call.push_error(msg.c_str());
        }
        int r = barch::set_configuration_value(name, argv[3].chars());
        if (r == 0) {
            return call.push_simple("OK");
        }
        return call.push_error("could not set configuration value");
    }
    if (keyword_is("get", "GET")) {
        // CONFIG GET <pattern> [<pattern> ...], as redis has it: each argument is a
        // glob, and the reply is a map of every variable that matches any of them. The
        // values are written in the form CONFIG SET takes back, so a client can read a
        // variable, change it and put it back without knowing its type.
        if (argv.size() < 3)
            return call.wrong_arity();
        std::string value;
        call.start_map();
        size_t matched = 0;
        // both barch's own names and the redis names it answers to, so a client that
        // asks for maxmemory and one that asks for max_memory_bytes are both served,
        // and CONFIG GET * shows the pair
        auto emit_matching = [&](const std::vector<std::string>& names) {
            for (const auto& name : names) {
                bool wanted = false;
                for (unsigned at = 2; at < argv.size() && !wanted; ++at) {
                    wanted = (1 == glob::stringmatchlen(argv[at], name, 1));
                }
                if (!wanted) continue;
                if (!barch::get_configuration_value(name, value)) continue;
                call.push_string(name);
                call.push_string(value);
                ++matched;
            }
        };
        emit_matching(barch::configuration_names());
        emit_matching(barch::redis_configuration_names());
        call.end_map();
        return call.ok();
    }
    if (keyword_is("resetstat", "RESETSTAT")) {
        if (argv.size() != 2)
            return call.wrong_arity();
        // the counters that count events, and the per command call counts INFO reports
        // as commandstats. Gauges are left alone - see statistics::reset_statistics
        statistics::reset_statistics();
        for (auto& f : *functions_by_name()) {
            reset_command_stats(f.second);
        }
        return call.push_simple("OK");
    }
    if (keyword_is("rewrite", "REWRITE")) {
        if (argv.size() != 2)
            return call.wrong_arity();
        // barch has no configuration file of its own - it is configured through its
        // host server's file and CONFIG SET - so there is nothing to write back. This
        // is the same answer redis gives when it was started without one, which is
        // what a client that handles the error already expects
        return call.push_error("The server is running without a config file");
    }
    if (keyword_is("help", "HELP")) {
        call.start_array();
        call.push_simple("CONFIG <subcommand>");
        call.push_simple("GET <pattern> [<pattern> ...]");
        call.push_simple("    Return settings matching any glob pattern. Both barch's own");
        call.push_simple("    names and the redis names they answer to are matched.");
        call.push_simple("SET <parameter> <value>");
        call.push_simple("    Set a parameter. Redis names are resolved to the barch setting");
        call.push_simple("    they mean; ones barch reports but cannot change are refused.");
        call.push_simple("RESETSTAT");
        call.push_simple("    Reset the counters INFO reports. Gauges are left alone.");
        call.push_simple("REWRITE");
        call.push_simple("    Not supported: barch has no configuration file of its own.");
        call.push_simple("HELP");
        call.push_simple("    This text.");
        call.end_array();
        return call.ok();
    }
    return call.push_error("only the GET, SET, RESETSTAT, REWRITE and HELP keywords are supported");
}
/* B.CONFIG [SET|GET] <key> [<value>]
 *
 * Set the specified key to the specified value. */
int cmd_CONFIG(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CONFIG);
}
int TRAIN(caller& call, const arg_t& argv) {
    std::string d;
    for (size_t i = 1; i < argv.size(); ++i) {
        d += argv[i].to_string();
        d += " ";
    }
    return call.push_ll(dictionary::train(d));
}
}

int add_config_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(CONFIG), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

void register_config_api(function_map& r) {
    r["CONFIG"] = {::CONFIG,{"write","read","config"}};
    r["TRAIN"] = {::TRAIN,{"write"}};
}
