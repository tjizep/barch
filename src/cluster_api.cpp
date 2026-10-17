//
// CLUSTER - TODO 610. The command is always there; what it does is the cluster's,
// when this build has one (see cluster_hooks.h).
//
#include "raft_writes.h"
#include "rpc/barch_functions.h"
#include <algorithm>
#include "cluster_api.h"

#include "cluster_hooks.h"
#include "rpc_caller.h"
#include "function_api.h"
#include "queue_service.h"
#include "auth_api.h"
#include "foreign/foreign.h"

#include <map>
#include <memory>

namespace {
    /*
     * How this node runs a command from a Raft log - TODO 669: in the space it was
     * logged for, as the server, so the leader's client was the one whose rights
     * were checked. Raft's own gate is off for it, as for the cluster's other local
     * calls: it's already in the log, and its writes are this member's to make.
     */
    bool run_logged(const std::string& space, const std::vector<std::string>& argv, uint64_t index,
                    barch::cluster::reply_ptr& reply, std::string& err) {
        auto ks = barch::get_keyspace(space);
        if (!ks) {
            err = "no space " + space;
            return false;
        }
        /*
         * A caller of its own for every entry - TODO 667. A kept one held compiled
         * functions from earlier entries: a member just started compiled the source
         * as it is now, and one that had been running kept the version it compiled
         * before a SETF, so the two ran different code. A new one compiles what the
         * store holds at this entry, on every member.
         */
        rpc_caller sc;
        sc.raft_local_reads = true;
        sc.set_kspace(ks);
        // the caller's user and rights, from the leader that logged it - TODO 667
        if (argv.size() < 3) {
            err = "a command entry with no user and rights";
            return false;
        }
        heap::vector<bool> rights;
        for (char c : argv[1])
            rights.push_back(c == '1');
        sc.set_acl(argv[0], rights);
        sc.space_acl = {};
        const std::vector<std::string> words(argv.begin() + 2, argv.end());
        // a stored function seeds math.random from the entry's index - TODO 673
        barch::functions::from_log logged(index);
        // a RELOAD's publish, for every member - TODO 667
        if (words.at(0) == barch::functions::publish_word) {
            for (size_t i = 1; i < words.size(); ++i)
                barch::functions::publish_compiled(words[i]);
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        // a replicated queue's push or settle, for every member - TODO 682
        if (words.at(0) == barch::mq::log_word)
            return barch::mq::run_from_log(ks, words, reply, err);
        // the cluster's first users - TODO 676
        if (words.at(0) == barch::auth_seed_word) {
            if (!barch::seed_users_from_log(words, err))
                return false;
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        // a miss a fetch found, kept by every member - TODO 685
        if (words.at(0) == barch::foreign::miss_word) {
            if (!barch::foreign::miss_from_log(ks, words, err))
                return false;
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        std::string name = words.at(0);
        if (const auto colon = name.find_last_of(':'); colon != std::string::npos && colon + 1 < name.size())
            name = name.substr(colon + 1);
        std::string upper = name;
        for (auto& c : upper) c = (char) std::toupper((unsigned char) c);
        auto r = std::make_shared<rpc_reply>();
        auto fi = barch::barch_functions->find(upper);
        if (fi != barch::barch_functions->end() && name.find('.') == std::string::npos) {
            r->code = sc.call(words, fi->second.call);
        } else {
            // a stored function, looked up the way a client's call finds it
            std::string from_space, fname = name;
            if (const auto dot = name.find('.'); dot != std::string::npos) {
                from_space = name.substr(0, dot);
                fname = name.substr(dot + 1);
            }
            const caller::resolved* fn = barch::functions::resolve(sc, from_space, fname);
            if (!fn) {
                // the same on every member: the function isn't there at this entry
                sc.errors.clear();
                sc.results.clear();
                sc.errors.emplace_back("unknown command " + name);
                r->code = -1;
            } else {
                r->code = sc.call(words, fn->call);
            }
        }
        r->results = sc.results;
        r->errors = sc.errors;
        reply = std::move(r);
        return true;
    }
    const bool runner_installed = [] {
        barch::cluster::set_command_runner(run_logged);
        return true;
    }();
}

extern "C" {
    int CLUSTER(caller& call, const arg_t& argv) {
        if (argv.size() < 2)
            return call.wrong_arity();
        std::vector<std::string> args;
        args.reserve(argv.size() - 1);
        for (size_t i = 1; i < argv.size(); ++i)
            args.push_back(argv[i].to_string());
        std::string sub = args[0];
        for (auto& c : sub) c = (char) std::toupper((unsigned char) c);
        /*
         * This connection's own settings - TODO 613. READS chooses whether a
         * follower may answer its reads. INDEX is the newest index it has seen in
         * a space, to hand to another connection, which AFTER makes read no older.
         */
        /*
         * Every write command and how a space replicated with Raft treats it - TODO
         * 663: replicated, server, or local (refused). One that's local only because
         * nobody has looked at it yet says `unlisted` too. See raft_writes.h.
         */
        if (sub == "WRITES" && args.size() == 1) {
            std::vector<std::string> names;
            for (const auto& [name, f] : *barch::barch_functions)
                if (f.is_write()) names.emplace_back(name.c_str());
            std::sort(names.begin(), names.end());
            call.start_array();
            for (const auto& n : names) {
                std::string line = n + " " + barch::raft_writes::name_of(barch::raft_writes::of(n, "", ""));
                if (!barch::raft_writes::listed(n)) line += " unlisted";
                call.push_simple(line);
                // the subcommands decided on their own - TODO 669
                for (const auto& [sub, k] : barch::raft_writes::subcommands(n))
                    call.push_simple(n + "/" + sub + " " + barch::raft_writes::name_of(k));
            }
            return call.end_array();
        }
        if (sub == "READS" || sub == "INDEX" || sub == "AFTER") {
            auto* session = call.cluster_session();
            if (!session)
                return call.push_error("ERR this connection has no cluster session");
            if (sub == "READS" && args.size() == 2) {
                std::string how = args[1];
                for (auto& c : how) c = (char) std::toupper((unsigned char) c);
                if (how != "FOLLOWER" && how != "LEADER")
                    return call.push_error("ERR CLUSTER READS FOLLOWER|LEADER");
                session->follower_reads = how == "FOLLOWER";
                return call.push_simple("OK");
            }
            if (sub == "INDEX" && args.size() == 2)
                return call.push_ll((int64_t) session->after[args[1]]);
            if (sub == "AFTER" && args.size() == 3) {
                uint64_t at = 0;
                try {
                    at = std::stoull(args[2]);
                } catch (...) {
                    return call.push_error("ERR the index is a number");
                }
                auto& seen = session->after[args[1]];
                if (at > seen) seen = at;
                return call.push_simple("OK");
            }
            return call.wrong_arity();
        }
        std::vector<std::string> out;
        std::string err;
        if (!barch::cluster::command(args, out, err))
            return call.push_error(err.c_str());
        if (out.size() == 1)
            return call.push_simple(out[0]);
        call.start_array();
        for (const auto& line : out)
            call.push_simple(line);
        return call.end_array();
    }
}

void register_cluster_api(function_map& r) {
    r["CLUSTER"] = {::CLUSTER, {"write", "dangerous"}};
}
