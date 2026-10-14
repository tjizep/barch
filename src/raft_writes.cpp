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
                // locks (TODO 614) and stored functions
                "LOCK", "UNLOCK", "SETF", "REMF",
            };
            return s;
        }
        // writes to the server or the connection rather than to a space's data
        const std::set<std::string>& server() {
            static const std::set<std::string> s{
                "ACL", "CONFIG", "CLUSTER", "USE", "SELECT", "MULTI", "EXEC", "START", "STOP",
                "PUBLISH", "PULL", "ADDROUTE", "REMROUTE",
            };
            return s;
        }
        // KSPACE's subcommands that only read
        const std::set<std::string>& kspace_reads() {
            static const std::set<std::string> s{"EXIST", "DEPENDANTS", "SOURCE"};
            return s;
        }
        // listed as local on purpose: each changes this node's copy only
        const std::set<std::string>& local() {
            static const std::set<std::string> s{
                "BEGIN", "COMMIT", "ROLLBACK", "KSPACE", "KSOPTIONS", "LOAD", "RELOAD", "RETRIEVE",
                "REPLAPPLY", "UNLOAD", "IMPORT", "LOADFS", "LOADKEYS", "FLUSHALL", "CLEARALL",
                "COMPRESS", "DECOMPRESS", "TRAIN", "DICTIONARY", "INDEX", "GRAPH", "FS", "DIR",
                "QUEUE", "FUNCTIONS", "FOREIGN", "FOREIGN_MISS", "HTTP",
            };
            return s;
        }
    }

    kind of(const std::string& name, const std::string& first, const std::string& second) {
        if (replicated().count(name)) return kind::replicated;
        if (server().count(name)) return kind::server;
        if (name == "KSPACE") {
            if (kspace_reads().count(first) || (first == "OPTION" && second == "GET"))
                return kind::server;
            return kind::local;
        }
        return kind::local;
    }

    bool listed(const std::string& name) {
        return replicated().count(name) || server().count(name) || local().count(name);
    }

    bool reaches_every_space(const std::string& name) {
        return name == "FLUSHALL" || name == "CLEARALL";
    }

    const char* name_of(kind k) {
        switch (k) {
            case kind::replicated: return "replicated";
            case kind::server: return "server";
            default: return "local";
        }
    }
}
