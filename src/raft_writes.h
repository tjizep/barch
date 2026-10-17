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
//   logged      changes a node's settings or what it derives from the data (an
//               index, the dictionary, how values are stored), so the command itself
//               goes in the space's log and every member runs it - TODO 669
//   local       changes this node's copy only; refused in a Raft space
//
// A command that isn't in the table counts as local, so a new write command is
// refused in a Raft space until it's been looked at and listed. CLUSTER WRITES
// answers the table, and TestClusterWrites fails on a write command it can't place.
//
#include <string>
#include <utility>
#include <vector>

namespace barch::raft_writes {
    enum class kind { replicated, server, logged, local };

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
    /**
     * Commands that name the spaces they change themselves, so Raft's gate doesn't
     * judge them by the connection's space - TODO 682, 683, 684: QUEUE (the queue's
     * space), FUNCTIONS (a sync's repositories), REPLAPPLY and REPLRAFT (each
     * record's space). Each one sees to its spaces' leaders itself.
     */
    bool routes_itself(const std::string& name);
    /**
     * Logged commands that act on the whole space at one point in its log, which a
     * space split over several groups doesn't have - TODO 689: BEGIN, COMMIT and
     * ROLLBACK (a mark on the whole space), LOAD and RETRIEVE (the whole space
     * replaced). There each goes in every group's log, and each group's run acts on
     * its own shards - TODO 690.
     */
    bool needs_one_point(const std::string& name);

    const char* name_of(kind k);

    /**
     * Logged commands that replace the space from this node's files or another
     * server (LOAD, RETRIEVE) - TODO 681. Run by the member that leads when
     * the entry applies; every other member is rebuilt from it.
     */
    bool replaced_on_leader(const std::string& name);

    /**
     * The subcommands of a command decided per subcommand, `/` between words, with
     * their class - for CLUSTER WRITES, which lists them as `INDEX/CREATE logged`.
     * Empty for every other command.
     */
    std::vector<std::pair<std::string, kind>> subcommands(const std::string& name);
}
