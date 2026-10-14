#pragma once
//
// Which write commands a space replicated with Raft takes - TODO 663.
//
// A write is replicated only when it changes a shard through the shard's committing
// paths (insert, update, remove, and FLUSHDB's clear record). Several commands
// changed a space some other way - a ROLLBACK that undid writes on one node, a
// KSPACE MERGE, a LOAD from that node's files - and left the members holding
// different data. So every write command is listed here, and a space replicated
// with Raft refuses any write that isn't listed as replicated or server:
//
//   replicated  writes only through the committing paths; TestClusterWrites runs it
//               on a leader and checks every member ends up the same
//   server      changes the server or the connection, not a space's data
//   local       changes this node's copy only; refused in a Raft space
//
// A command that isn't in the table counts as local, so a new write command is
// refused in a Raft space until it's been looked at and listed. CLUSTER WRITES
// answers the table, and TestClusterWrites fails on a write command it can't place.
//
#include <string>
#include <vector>

namespace barch::raft_writes {
    enum class kind { replicated, server, local };

    /**
     * The class of a write command. `name` is upper case. `first` and `second` are
     * its first two arguments, upper cased, for the commands decided per subcommand
     * (KSPACE).
     */
    kind of(const std::string& name, const std::string& first, const std::string& second);

    /** whether `name` (upper case) is in the table at all, as opposed to local by default */
    bool listed(const std::string& name);

    /**
     * Commands that write other spaces than the connection's: refused while any
     * space on this node is replicated, whatever the connection's space is.
     */
    bool reaches_every_space(const std::string& name);

    const char* name_of(kind k);
}
