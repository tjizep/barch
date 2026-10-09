//
// Locks in a space replicated with Raft - TODO 614.
//
// A lock is the meta key `lock/<name>` in the space, holding `<owner> <expires_ms>`.
// A meta key goes wherever the data goes - logged, replicated, saved, copied - and no
// client command can name it, so only these three commands change a lock.
//
// LOCK and UNLOCK are writes, so they run on the space's leader, and a mutex here
// makes each one's look-then-write a single step: two clients asking at once can't
// both see the lock free. The grant is a write through the space's Raft group, and
// the index it commits at is the fencing token. Indexes only go up in a group, so a
// later grant always has a higher token than an earlier one, whoever made it.
//
// Expiry is decided by the leader's clock, when a LOCK asks for a lock whose time has
// passed. A follower never decides anything about a lock. After a leader change the
// new leader's clock decides, so clocks that disagree by more than a lock's slack can
// hand it out early.
//
#include "lock_api.h"

#include "cluster_hooks.h"
#include "meta_keys.h"

#include <chrono>
#include <mutex>
#include <sstream>

namespace {
    std::mutex& lock_grants() {
        static auto* m = new std::mutex;
        return *m;
    }

    int64_t lock_now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::string lock_key(const std::string& lock) {
        return "lock/" + lock;
    }

    bool lock_state(const barch::key_space_ptr& ks, const std::string& name, std::string& owner, int64_t& expires) {
        std::string v;
        if (!barch::meta::get(ks, lock_key(name), v))
            return false;
        std::istringstream in(v);
        return static_cast<bool>(in >> owner >> expires);
    }

    bool lock_space_replicated(caller& call) {
        auto& ks = call.kspace();
        return ks && ks->has_raft();
    }
}

extern "C" {
    /*
     * LOCK name owner ttl_ms
     *
     * Grants the lock to `owner` for `ttl_ms` when nobody holds it, its holder's
     * time is up, or `owner` already holds it (a renewal). Answers the fencing
     * token, above 0, or 0 when someone else holds it.
     */
    int LOCK(caller& call, const arg_t& argv) {
        if (argv.size() != 4)
            return call.wrong_arity();
        if (!lock_space_replicated(call))
            return call.push_error("ERR LOCK needs a space replicated with Raft (<space>.raft on)");
        const std::string name = argv[1].to_string();
        const std::string owner = argv[2].to_string();
        int64_t ttl = 0;
        try {
            ttl = std::stoll(argv[3].to_string());
        } catch (...) {
            return call.push_error("ERR the ttl is a number of milliseconds");
        }
        if (owner.empty() || owner.find(' ') != std::string::npos || ttl <= 0)
            return call.push_error("ERR the owner is one word and the ttl above 0");
        auto& ks = call.kspace();
        std::lock_guard l(lock_grants());
        std::string holder;
        int64_t expires = 0;
        const int64_t now = lock_now_ms();
        if (lock_state(ks, name, holder, expires) && holder != owner && expires > now)
            return call.push_ll(0);
        std::string err;
        if (!barch::meta::set(ks, lock_key(name), owner + " " + std::to_string(now + ttl), err))
            return call.push_error(err.c_str());
        // the grant is the one write this command made, in the group of the lock's
        // shard; a split space's groups are labelled `<space>/<k>` - TODO 615
        uint64_t token = 0;
        for (const auto& [group, at] : barch::cluster::committed())
            token = std::max(token, at);
        if (token == 0)
            return call.push_error("ERR the grant committed but its index wasn't reported");
        return call.push_ll((int64_t) token);
    }

    /** UNLOCK name owner - 1 when `owner` held it and now doesn't, else 0 */
    int UNLOCK(caller& call, const arg_t& argv) {
        if (argv.size() != 3)
            return call.wrong_arity();
        if (!lock_space_replicated(call))
            return call.push_error("ERR UNLOCK needs a space replicated with Raft (<space>.raft on)");
        const std::string name = argv[1].to_string();
        const std::string owner = argv[2].to_string();
        auto& ks = call.kspace();
        std::lock_guard l(lock_grants());
        std::string holder;
        int64_t expires = 0;
        if (!lock_state(ks, name, holder, expires) || holder != owner || expires <= lock_now_ms())
            return call.push_ll(0);
        return call.push_ll(barch::meta::remove(ks, lock_key(name)) ? 1 : 0);
    }

    /** LOCKINFO name - the holder and when its time is up (ms since the epoch), or nil */
    int LOCKINFO(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        if (!lock_space_replicated(call))
            return call.push_error("ERR LOCKINFO needs a space replicated with Raft (<space>.raft on)");
        std::string holder;
        int64_t expires = 0;
        if (!lock_state(call.kspace(), argv[1].to_string(), holder, expires) || expires <= lock_now_ms())
            return call.push_null();
        call.start_array();
        call.push_simple(holder);
        call.push_ll(expires);
        return call.end_array();
    }
}

void register_lock_api(function_map& r) {
    r["LOCK"] = {::LOCK, {"write", "data"}};
    r["UNLOCK"] = {::UNLOCK, {"write", "data"}};
    r["LOCKINFO"] = {::LOCKINFO, {"read", "data"}};
}
