#pragma once
//
// A pooled, asynchronous RESP client - what `resp.connect` in Luau talks through.
// TODO 379.
//
// Everything that touches a socket or the pool happens on one thread of its own, the
// client's reactor. A caller hands over owned data - the endpoint, the settings, the
// commands - and gets owned data back - one Variable per command - through `done`,
// which runs on that thread. Nothing crosses by reference, nothing here touches a Lua
// state or a shard, and no asio type is visible outside resp_client.cpp.
//
// The rules that keep a connection's byte stream in step, learnt the hard way in
// DONE 12, where one socket had two read chains and the parser lost its place:
//   - a connection belongs to at most one exchange at a time;
//   - an exchange is one write followed by one read chain into the connection's own
//     parser, until it has exactly the replies it asked for;
//   - anything left over afterwards, a timeout, a transport error, or a command that
//     changes the session means the connection is closed, never pooled.
//
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "resp_reply.h"
#include "variable.h"

namespace barch::resp_client {

struct endpoint {
    std::string host;
    uint16_t port{6379};
    std::string user;       // empty: AUTH with the password alone
    std::string password;   // empty: no AUTH
    int64_t db{-1};         // -1: leave the connection on the server's default
    int protocol{2};        // 2, or 3 for HELLO 3
};

/** from the configuration space (resp.*), read by the caller, which is allowed to */
struct settings {
    uint32_t connect_timeout_ms{5000};
    uint32_t timeout_ms{5000};
    size_t max_connections{256};
    size_t max_idle{8};
    uint64_t idle_ms{60000};
    resp::limits lim{};
};

/** a command as its words */
typedef std::vector<std::string> command;

/**
 * Called exactly once, on the client's thread. With an empty `err`, one reply per
 * command in the order sent, error replies included as var_error. A non-empty `err`
 * means the exchange as a whole failed - resolve, connect, AUTH, a timeout, the
 * connection closing, a stream that isn't RESP - and `replies` is empty.
 */
typedef std::function<void(std::vector<Variable> replies, std::string err)> done_fn;

/** send `commands` on a pooled connection to `ep` and hand back their replies */
void run(const endpoint& ep, const settings& s, std::vector<command> commands, done_fn done);

/**
 * Why a command can't go through a pooled connection at all - SUBSCRIBE and MONITOR
 * never finish replying, QUIT ends the connection - or empty when it can.
 */
std::string refused(const command& c);

/** one line per pool and a totals line, for RESP POOL. Never shows a password */
std::vector<std::string> pool_status();

}
