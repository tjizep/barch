//
// What the rest of barch sees of clustering - TODO 610. Always compiled; the
// cluster itself (src/cluster/) is only built with BARCH_CLUSTER, and without it
// nothing here is ever set, so every check is a null pointer.
//
#ifndef BARCH_CLUSTER_HOOKS_H
#define BARCH_CLUSTER_HOOKS_H

#include <memory>
#include <string>
#include <vector>

namespace barch::cluster {
    /*
     * A key space's Raft group, as its shards' write path sees it.
     *
     * A shard makes a write under its write latch, builds the same record the
     * change log and PUBLISH use, and hands it to commit() while it still holds
     * the latch. So nothing reads a write before a quorum has it, and nothing a
     * save writes was never committed. What commit() answers decides what the
     * shard does next:
     *
     *   - committed: the write stays and the client gets its answer.
     *   - refused:   the record never reached the log (this node isn't the
     *                leader, or the group is stopping). The shard puts the key
     *                back the way it was and the client gets the error.
     *   - unknown:   it reached the log and leadership changed before it was
     *                known to commit. It may commit yet, so it can't be undone.
     *                The binding rebuilds the space from the group, and the
     *                client gets the error.
     *
     * A write made while applying a committed entry (repl::applying) isn't
     * handed back here.
     */
    class binding {
    public:
        enum class result { committed, refused, unknown };

        virtual ~binding() = default;

        virtual result commit(const std::string& encoded_record, std::string& why) = 0;
        /** whether this node leads the space's group and can take its writes now */
        [[nodiscard]] virtual bool leader() const = 0;
        /**
         * The client error for a node that isn't the leader:
         * `NOTLEADER <host>:<port>`, with the leader's RESP address when it's known.
         */
        [[nodiscard]] virtual std::string not_leader() const = 0;
    };
    using binding_ptr = std::shared_ptr<binding>;

    /**
     * The binding for a space, by its canonical name. Set by the cluster before
     * the space opens; read once when it opens, like the space's change log.
     */
    binding_ptr binding_for(const std::string& canonical_space);
    void bind(const std::string& canonical_space, binding_ptr b);
    void unbind(const std::string& canonical_space);

    /*
     * The cluster's runtime, which barchd starts and stops. Installed by
     * src/cluster/ when it's built in.
     */
    class runtime {
    public:
        virtual ~runtime() = default;
        /** after the key spaces can open, before the listener. false and `err` stop barchd */
        virtual bool start(std::string& err) = 0;
        virtual void stop() = 0;
        /**
         * CLUSTER <subcommand> ... - the words after CLUSTER. `out` is the reply, one
         * line per element; false with `err` set is an error reply.
         */
        virtual bool command(const std::vector<std::string>& args, std::vector<std::string>& out,
                             std::string& err) = 0;
    };
    void install(runtime* r);
    /** nothing to do, and true, when the cluster isn't built in or raft_port is 0 */
    bool start(std::string& err);
    void stop();
    /** whether this build has the cluster in it */
    bool built();
    /** CLUSTER, for the command table; an error when the cluster isn't built in */
    bool command(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err);
}

#endif //BARCH_CLUSTER_HOOKS_H
