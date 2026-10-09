//
// CLUSTER - TODO 610. The command is always there; what it does is the cluster's,
// when this build has one (see cluster_hooks.h).
//
#include "cluster_api.h"

#include "cluster_hooks.h"

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
