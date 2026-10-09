//
// What the rest of barch sees of clustering - TODO 610. Always compiled; the
// cluster itself (src/cluster/) is only built with BARCH_CLUSTER, and without it
// nothing here is ever set, so every check is a null pointer.
//
#ifndef BARCH_CLUSTER_HOOKS_H
#define BARCH_CLUSTER_HOOKS_H

#include <cstdint>
#include <map>
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
    class binding : public std::enable_shared_from_this<binding> {
    public:
        enum class result { committed, refused, unknown };

        virtual ~binding() = default;

        /** `shard` is the shard the write is in: one that's moving to another group is refused - TODO 616 */
        /**
         * `writer_applied` false: the writer took the record back out of its tree
         * and the commit applies it here, as on every member - TODO 632
         */
        virtual result commit(const std::string& encoded_record, size_t shard, std::string& why,
                              bool writer_applied) = 0;
        /**
         * The shard is being handed to another group by a split, so neither its
         * reads nor its writes go through this one any more - TODO 616. The reason
         * goes in `why`.
         */
        [[nodiscard]] virtual bool fenced(size_t shard, std::string& why) const = 0;
        /** whether this node leads the space's group and can take its writes now */
        [[nodiscard]] virtual bool leader() const = 0;
        /**
         * Whether this node can answer a read as the leader - TODO 613: it leads,
         * and a quorum of voters answered it within the lease, so no other node
         * can have been elected since.
         */
        [[nodiscard]] virtual bool leader_read() const = 0;
        /**
         * Whether a follower can answer a read for a session that has seen index
         * `after` - TODO 613. It has to hear from a leader, and have applied at
         * least that far; it waits up to `wait_ms` for that.
         */
        [[nodiscard]] virtual bool follower_read(uint64_t after, int wait_ms) const = 0;
        /** the last index applied here */
        [[nodiscard]] virtual uint64_t applied() const = 0;
        /**
         * What this group is called in sessions, heartbeats and routes: the space's
         * name, or `<space>/<k>` for the k-th group of a space split over several -
         * TODO 615. Each group has its own log, so its indexes mean nothing to another.
         */
        [[nodiscard]] virtual const std::string& label() const = 0;
        /**
         * The client error for a node that isn't the leader:
         * `NOTLEADER <host>:<port> <epoch>`, with the leader's RESP address when
         * it's known, and the group's term as the epoch: it goes up every time the
         * leader changes, so of two redirects the higher epoch is the newer.
         */
        [[nodiscard]] virtual std::string not_leader() const = 0;
    };
    using binding_ptr = std::shared_ptr<binding>;

    /*
     * A client connection's view of the replicated spaces - TODO 613.
     *
     * `after` is, per space, the newest log index this session has seen: a write
     * it made committed there, or a read it made came from a node that had applied
     * that far. A follower answers a read only once it has applied at least that,
     * so the session never reads anything older than what it already saw - its own
     * writes included. With `follower_reads` off, reads go to the leader.
     */
    struct session {
        bool follower_reads{false};
        std::map<std::string, uint64_t> after{};
    };

    /*
     * The index a write committed at, from the shard that made it to the session
     * whose command it was. A command runs on one thread, so it's handed over in
     * a thread_local: cleared before the command, read after it.
     */
    void note_committed(const std::string& space, uint64_t index);
    void clear_committed();
    /** the groups this thread's command committed in, by label, with the last index in each */
    const std::map<std::string, uint64_t>& committed();

    /*
     * The reads of the client command running on this thread - TODO 615. A space
     * split over several groups can't tell from a command which group a read
     * lands in, so it's checked where the read takes a shard's latch: begin_reads
     * before the command, and each shard's latch then asks check_read about the
     * shard's group, once per group per command. A group this node can't read
     * throws its NOTLEADER. end_reads records what each group read had applied in
     * the session. Nothing is checked on a thread that didn't begin.
     */
    void begin_reads(session* s);
    /** `shard`, when there is one, is checked against a split's fence every time */
    void check_read(const binding_ptr& b, size_t shard = SIZE_MAX);
    void end_reads();
    /** whether this node can read the group for `s` now, as the gate asks it */
    bool readable(binding& b, session& s);

    /**
     * The bindings for a space, by its canonical name: each with the run of shards
     * [from, to) it covers - all of them for a space in one group, a share each
     * for a space split over several (TODO 615). Set by the cluster before the
     * space opens; read once when it opens, like the space's change log.
     */
    struct shard_binding {
        size_t from{0};
        size_t to{SIZE_MAX};
        binding_ptr b{};
    };
    std::vector<shard_binding> bindings_for(const std::string& canonical_space);
    void bind(const std::string& canonical_space, binding_ptr b, size_t from = 0, size_t to = SIZE_MAX);
    /** that binding, or every binding of the space when `b` is null */
    void unbind(const std::string& canonical_space, const binding_ptr& b = nullptr);

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
    /**
     * The shard count the cluster decided for a space it replicates - TODO 631. A
     * space opened here takes it over `<space>.shards` and the default, since this
     * node's configuration can lag the cluster's record. 0 when it doesn't say.
     */
    void set_space_shards(const std::string& space, size_t shards);
    size_t space_shards(const std::string& space);

    /**
     * The shard a single-key write on this thread is under way in, or null - TODO
     * 632. Set by sharded_store around the write, read by the shard's raft_commit and
     * by storage_release's gate
     */
    void set_narrowing(const void* shard);
    const void* narrowing();

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
