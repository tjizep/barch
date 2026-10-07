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
