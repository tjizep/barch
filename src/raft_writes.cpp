//
// Which write commands a space replicated with Raft takes - TODO 663. See raft_writes.h.
//
#include "raft_writes.h"

#include <set>

namespace barch::raft_writes {
    namespace {
        // writes that change a shard only through insert, update, remove or FLUSHDB's
        // clear record, each of which commits to the shard's Raft group. Each one is
        // run on a leader by TestClusterWrites, which checks every member after it
        const std::set<std::string>& replicated() {
            static const std::set<std::string> s{
                // strings and keys
                "SET", "SETEX", "PSETEX", "SETNX", "SETRANGE", "MSET", "MSETNX", "GETSET", "GETDEL",
                "GETEX", "APPEND", "PREPEND", "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
                "UINCRBY", "UDECRBY", "DEL", "REM", "ADD", "RENAME", "RENAMENX", "COPY", "MOVE",
                "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "PERSIST", "FLUSHDB",
                // hashes
                "HSET", "HMSET", "HSETNX", "HDEL", "HGETDEL", "HINCRBY", "HINCRBYFLOAT", "HEXPIRE",
                "HEXPIREAT",
                // lists
                "LPUSH", "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "LINSERT", "LMOVE", "LMPOP",
                "RPOPLPUSH", "BLPOP", "BRPOP", "BLMOVE", "BLMPOP", "BRPOPLPUSH",
                // ordered sets
                "ZADD", "ZINCRBY", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
                "ZPOPMIN", "ZPOPMAX", "ZMPOP", "BZPOPMIN", "BZPOPMAX", "BZMPOP", "ZDIFFSTORE",
                "ZINTERSTORE", "ZUNIONSTORE", "ZRANGESTORE",
                // locks (TODO 614)
                "LOCK", "UNLOCK",
                // every space: a replicated one through its own group, the way FLUSHDB
                // clears it - TODO 680
                "FLUSHALL", "CLEARALL",
                // an export's commands, each through Raft's gate as a client's would
                // be - TODO 681
                "IMPORT",
                // files, the graph, key trees and imports from the leader's disk: each
                // is a staged batch, which a replicated space commits as one entry -
                // TODO 665
                "FS", "GRAPH", "DIR", "LOADFS", "LOADKEYS",
                // a push to a queue on a replicated space is an entry in that space's
                // log; one on any other space is that node's own - TODO 682
                "QUEUE",
                // a primary's records for a replicated space go in its log, through
                // its leader (REPLRAFT is that leader's end) - TODO 684
                "REPLAPPLY", "REPLRAFT",
                // a primary's cached miss, into its space's log - TODO 686
                "REPLMISS",
            };
            return s;
        }
        // writes to the server or the connection rather than to a space's data
        const std::set<std::string>& server() {
            static const std::set<std::string> s{
                "ACL", "CONFIG", "CLUSTER", "USE", "SELECT", "MULTI", "EXEC", "START", "STOP",
                // memory only: the space is saved and comes back from its files; a
                // replicated one refuses it in the handler - TODO 681
                "UNLOAD",
                // each shard saved and loaded straight back, the data as it was: this
                // node's storage only - TODO 681
                "RELOAD",
                "PUBLISH", "PULL", "ADDROUTE", "REMROUTE",
                // this node's web server for the space; a follower's sends what
                // writes to the leader's - TODO 687
                "HTTP",
            };
            return s;
        }
        // run by every member from the log - TODO 669. DICTIONARY, INDEX and KSPACE
        // are decided per subcommand in of()
        const std::set<std::string>& logged() {
            static const std::set<std::string> s{
                "KSOPTIONS", "TRAIN", "COMPRESS", "DECOMPRESS",
                // stored functions: every member installs them, with what an install
                // does besides the write (caches, AOT, cron, queues, RELOAD) - TODO 667
                "SETF", "REMF",
                // a transaction is a mark on the whole space, so every member marks it
                // and goes back to it at the same place in the log - TODO 677
                "BEGIN", "COMMIT", "ROLLBACK",
                // the space replaced from files or another server, by the member that
                // leads when the entry applies; the others copy it from there - TODO 681
                "LOAD", "RETRIEVE",
                // a cached miss is a tomb in the tree, so every member keeps it - TODO 685
                "FOREIGN_MISS",
            };
            return s;
        }
        // INDEX's subcommands that change something; the rest only read
        const std::set<std::string>& index_writes() {
            static const std::set<std::string> s{"CREATE", "BUILD", "DROP"};
            return s;
        }
        // FOREIGN FAKE's subcommands that change the fake source
        const std::set<std::string>& fake_writes() {
            static const std::set<std::string> s{"SET", "DEL", "FAIL", "DELAY", "RESET"};
            return s;
        }
        // KSPACE's subcommands that only read
        const std::set<std::string>& kspace_reads() {
            static const std::set<std::string> s{"EXIST", "DEPENDANTS", "SOURCE"};
            return s;
        }
    }

    kind of(const std::string& name, const std::string& first, const std::string& second) {
        if (replicated().count(name)) return kind::replicated;
        if (server().count(name)) return kind::server;
        if (logged().count(name)) return kind::logged;
        if (name == "DICTIONARY")
            return first == "SET" ? kind::logged : kind::server;
        if (name == "INDEX")
            return index_writes().count(first) ? kind::logged : kind::server;
        /*
         * TODO 683. SYNC writes the spaces its repositories fill, each where this node
         * leads it (function_sync.cpp); the rest report on this node.
         */
        if (name == "FUNCTIONS")
            return first == "SYNC" ? kind::replicated : kind::server;
        /*
         * FOREIGN FAKE - TODO 685: the fake source is each member's memory, so what
         * changes it is run by every member, and a new leader reads the same source.
         */
        if (name == "FOREIGN")
            return fake_writes().count(second) ? kind::logged : kind::server;
        if (name == "KSPACE") {
            if (kspace_reads().count(first) || (first == "OPTION" && second == "GET"))
                return kind::server;
            if (first == "OPTION" && second == "SET")
                return kind::logged;
            /*
             * TODO 670. These name the space they change, which rpc_caller finds and
             * routes by (call_kspace): MERGE writes its target through the shard
             * write paths, gathered into one batch; DEPENDS, RELEASE and DROP change
             * each node's own view of a space, so every member runs them. ACL writes
             * the auth store, each node's own, as ACL does.
             */
            if (first == "MERGE")
                return kind::replicated;
            if (first == "DEPENDS" || first == "RELEASE" || first == "DROP")
                return kind::logged;
            if (first == "ACL")
                return kind::server;
            /*
             * No subcommand, or one the parser refuses: a syntax error, which changes
             * nothing. The last local entry, until TODO 688 said so.
             */
            return kind::server;
        }
        return kind::local;
    }

    bool listed(const std::string& name) {
        return replicated().count(name) || server().count(name) || logged().count(name)
               || name == "DICTIONARY" || name == "INDEX" || name == "FUNCTIONS" || name == "FOREIGN"
               || name == "KSPACE";
    }

    bool needs_one_point(const std::string& name) {
        return name == "BEGIN" || name == "COMMIT" || name == "ROLLBACK" || name == "LOAD" || name == "RETRIEVE";
    }

    bool routes_itself(const std::string& name) {
        return name == "QUEUE" || name == "FUNCTIONS" || name == "REPLAPPLY" || name == "REPLRAFT"
               || name == "REPLMISS";
    }

    bool reaches_every_space(const std::string& name) {
        return name == "FLUSHALL" || name == "CLEARALL";
    }

    std::vector<std::pair<std::string, kind>> subcommands(const std::string& name) {
        std::vector<std::pair<std::string, kind>> out;
        if (name == "INDEX") {
            for (const auto& sub : index_writes())
                out.emplace_back(sub, kind::logged);
        } else if (name == "DICTIONARY") {
            out.emplace_back("SET", kind::logged);
        } else if (name == "KSPACE") {
            out.emplace_back("OPTION/SET", kind::logged);
            out.emplace_back("MERGE", kind::replicated);
            out.emplace_back("DEPENDS", kind::logged);
            out.emplace_back("RELEASE", kind::logged);
            out.emplace_back("DROP", kind::logged);
            out.emplace_back("ACL", kind::server);
        } else if (name == "FUNCTIONS") {
            out.emplace_back("SYNC", kind::replicated);
        } else if (name == "FOREIGN") {
            for (const auto& sub : fake_writes())
                out.emplace_back("FAKE/" + sub, kind::logged);
        }
        return out;
    }

    bool replaced_on_leader(const std::string& name) {
        return name == "LOAD" || name == "RETRIEVE";
    }

    const char* name_of(kind k) {
        switch (k) {
            case kind::replicated: return "replicated";
            case kind::server: return "server";
            case kind::logged: return "logged";
            default: return "local";
        }
    }
}
