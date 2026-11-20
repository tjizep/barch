#ifndef BARCH_RAFT_GATE_H
#define BARCH_RAFT_GATE_H
/*
 * What Raft's gate does with a command, with nothing else in it - TODO 937.
 *
 * rpc_caller::call sends a command in a space replicated with Raft one of a few
 * ways: refused, into the log, to the leader as a write, or read under the read
 * rules. That choice is made here, from plain values. The caller looks up the
 * command, its class and the space, and hands the answers in.
 *
 * This header includes no strings, no locks and no cluster, and TestCoreBoundary
 * fails if it starts to. test/raftgatetest.cpp runs every combination of inputs
 * through it.
 */
#include <cstdint>

namespace barch::raft_writes {
    /** a write command's class - see raft_writes.h */
    enum class kind {
        replicated,
        server,
        logged,
        local,
        /*
         * TODO 937. A subcommand of a write command that only reads the space: INDEX
         * FIND, LIST and CHAINS. As server they skipped the read rules too, and a
         * follower answered them from its own copy where its GET said NOTLEADER.
         */
        reads
    };

    /** how a stored function says it runs - functions::replication, as plain values */
    enum class stored { none, read_only, log, effects };

    struct gate_in {
        stored fn{stored::none};    // none: not a stored function
        bool builtin{false};        // a builtin command, not a stored function
        bool is_write{false};       // its categories say write
        bool is_data{false};        // its categories say data
        bool kspace{false};         // KSPACE, routed by the space it names
        kind k{kind::local};        // its class, asked only of a builtin write
        bool split{false};          // the space has more than one group
        bool one_point{false};      // raft_writes::needs_one_point
        bool install{false};        // SETF or REMF
        bool tx_layer{false};       // a transaction runs it on its overlay
        bool leader{false};         // this node leads the space's one group
        bool committx{false};       // COMMITTX, which finds each group's leader itself
        bool tx_function{false};    // a function joining a transaction
    };

    enum class step {
        run,            // run here, no check
        refuse_local,   // changes this node's copy only
        through_log,    // into the log, which every member runs
        every_group,    // into each group's log in turn
        not_leader,     // a write on a node that doesn't lead
        write,          // run here as a write: each shard checks its own group
        read            // run here under the read rules
    };

    struct gate_out {
        step what{step::run};
        bool gather{false};     // gather the writes into one batch over the groups
    };

    inline gate_out gate(const gate_in& in) {
        gate_out out;
        const bool is_stored = in.fn != stored::none;
        if (in.fn == stored::log) {
            if (!in.split)
                return {step::through_log, false};
            out.gather = true;
        }
        bool server_class = false;
        bool reads_only = false;
        if (!is_stored && !in.kspace && in.builtin && in.is_write) {
            if (in.k == kind::local)
                return {step::refuse_local, false};
            if (in.k == kind::logged) {
                if (!in.split)
                    return {step::through_log, false};
                if (in.install)
                    out.gather = true;
                else
                    return {in.one_point ? step::every_group : step::through_log, false};
            }
            server_class = in.k == kind::server;
            reads_only = in.k == kind::reads;
        }
        // INDEX says keys, not data: a subcommand that reads the space is a read of it
        if (server_class || in.tx_layer || !(is_stored || (in.builtin && (in.is_data || reads_only))))
            return out;
        const bool writes = is_stored ? in.fn != stored::read_only : in.is_write && !reads_only;
        if (!writes) {
            out.what = step::read;
            return out;
        }
        if (!in.split && !in.leader && !in.committx && !in.tx_function) {
            out.what = step::not_leader;
            return out;
        }
        out.what = step::write;
        return out;
    }
}
#endif //BARCH_RAFT_GATE_H
