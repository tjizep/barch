//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "connection_api.h"
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

/* HELLO [protover [SETNAME clientname]]
 *
 * the handshake a modern redis client sends when it opens a connection, and where the
 * RESP version is settled. 2 and 3 are both served; anything else is refused with
 * NOPROTO, the same as redis. The version sticks to the connection, so every reply
 * that follows is written in whichever the client asked for.
 *
 * the reply is a map, which RESP3 sends as '%' and RESP2 flattens into an array - the
 * writer picks, so this reads the same either way.
 *
 * AUTH runs the ordinary AUTH command with its own argument list and then takes its
 * OK back off the reply, so the handshake is all the client sees. A failure leaves an
 * error queued, which the caller turns into a failed call on its own.
 */
int HELLO(caller& call, const arg_t& argv) {
    unsigned at = 1;
    int64_t protover = call.get_protocol();
    if (argv.size() > at) {
        if (!conversion::to_i64(argv[at], protover)) {
            return call.push_error("Protocol version is not an integer or out of range");
        }
        if (protover < 2 || protover > 3) {
            return call.push_error("NOPROTO unsupported protocol version");
        }
        ++at;
    }
    art::value_type auth_user{}, auth_secret{};
    bool authenticate = false;
    while (argv.size() > at) {
        if (argv[at] == "SETNAME" && argv.size() > at + 1) {
            at += 2; // accepted and ignored, the same as CLIENT SETINFO
            continue;
        }
        if (argv[at] == "AUTH" && argv.size() > at + 2) {
            auth_user = argv[at + 1];
            auth_secret = argv[at + 2];
            authenticate = true;
            at += 3;
            continue;
        }
        return call.syntax_error();
    }
    if (authenticate) {
        // AUTH takes its own argument list, with the command name back in front
        arg_t auth_argv;
        auth_argv.push_back("AUTH");
        auth_argv.push_back(auth_user);
        auth_argv.push_back(auth_secret);
        ::AUTH(call, auth_argv);
        if (call.errors_count() > 0) {
            // AUTH already said why, and a queued error is enough to fail the call
            return call.ok();
        }
        // AUTH answers OK by pushing it, so take it back rather than let it travel in
        // front of the handshake
        Variable answer;
        if (call.pop_value(answer) && answer.to_string() != "OK") {
            return call.push_error("authentication failed");
        }
    }
    // the handshake itself goes out in the version just agreed, which is what a client
    // expects: it reads the reply with the parser it is about to switch to
    call.set_protocol((int) protover);

    call.start_map();
    call.push_string("server");
    call.push_string("redis"); // clients gate features on this, and INFO already says redis_version
    call.push_string("version");
    call.push_string(BARCH_PROJECT_VERSION);
    call.push_string("proto");
    call.push_int((int64_t) protover);
    call.push_string("id");
    call.push_int((int64_t) 0); // barch does not carry a per connection id
    call.push_string("mode");
    call.push_string("standalone");
    call.push_string("role");
    call.push_string("master");
    call.push_string("modules");
    call.start_array();
    call.end_array();
    call.end_map();
    return call.ok();
}
int cmd_HELLO(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HELLO);
}
int CLIENT(caller& call, const arg_t& arg_v) {
    if (arg_v.size()<=1) {
        return call.wrong_arity();
    }
    if (arg_v[1] == "INFO") {
        std::string r = call.get_info();
        return call.push_string(r);
    }
    if (arg_v[1] == "LIST") {
        // no filters yet: redis also takes TYPE and ID, which need the session to carry
        // more about itself than it does
        if (arg_v.size() != 2) {
            return call.wrong_arity();
        }
        barch::server::list_clients(call);
        return call.ok();
    }
    if (arg_v[1] == "SETINFO") {
        if (arg_v.size() == 4) {
            return call.ok();
        }
    }
    if (arg_v[1] == "CLEAR_ITERS") {
        if (arg_v.size() != 2) {
            return call.wrong_arity();
        }
        // a SCAN that runs to the end drops its own cursor, but one that is abandoned
        // part way through keeps it until the connection closes. this lets a client
        // that knows it has abandoned some let them go without reconnecting.
        return call.push_ll((int64_t) call.clear_iterations());
    }
    return call.syntax_error();
}
int MULTI(caller& call, const arg_t& arg_v) {
    if (arg_v.size()!=1) {
        return call.wrong_arity();
    }
    call.start_call_buffer();
    return call.ok();
}
int EXEC(caller& call, const arg_t& arg_v) {
    if (arg_v.size()!=1) {
        return call.wrong_arity();
    }
    call.finish_call_buffer();
    return 0;
}
/* PING [message]
 *
 * redis's health check. With no argument the reply is the simple string PONG; with one
 * it is that message echoed back as a bulk string. More than one is an arity error, as
 * in redis.
 */
int PING(caller& call, const arg_t& argv) {
    if (argv.size() == 1) {
        return call.push_simple("PONG");
    }
    if (argv.size() == 2) {
        return call.push_vt(argv[1]);
    }
    return call.wrong_arity();
}
int cmd_PING(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PING);
}
int COMMAND(caller& call, const arg_t& params) {
    if (params.size() < 2) {
        return call.wrong_arity();
    }
    if (params[1] == "DOCS") {
        std::vector<Variable> results;
        call.start_array();
        for (auto& p: *functions_by_name()) {
            call.push_simple(p.first.c_str());
            call.push_simple("function");
        }
        call.end_array();
        return call.push_simple("OK");
    }
    return call.push_error("unknown command");
}
}

int add_connection_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(PING), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

void register_connection_api(function_map& r) {
    r["PING"] = {::PING,{"read","connection"}};
    r["CLIENT"] = {::CLIENT,{"read","connection"}};
    r["HELLO"] = {::HELLO,{"connection"}};
    r["MULTI"] = {::MULTI,{"write"}};
    r["EXEC"] = {::EXEC,{"write"}};
    // COMMAND is deliberately not registered. It is implemented for the valkey module,
    // where the server asks the module to describe itself; over RESP a client that
    // sends COMMAND gets "unknown command" and falls back, which is what we want until
    // there is a command table worth publishing.
}
