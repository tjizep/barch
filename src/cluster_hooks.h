//
// What the rest of barch sees of clustering - TODO 610. Always compiled; the
// cluster itself (src/cluster/) is only built with BARCH_CLUSTER, and without it
// nothing here is ever set, so every check is a null pointer.
//
#ifndef BARCH_CLUSTER_HOOKS_H
#define BARCH_CLUSTER_HOOKS_H

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace barch { class key_space; }

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
    /*
     * A command run from the log - TODO 669. The rpc layer runs it and keeps its reply
     * in one of these; the cluster only carries it from the commit thread to the
     * leader's client thread, which answers the client with it.
     */
    struct command_reply {
        virtual ~command_reply() = default;
        /** what the command answered as an error, empty when it didn't - TODO 690 */
        [[nodiscard]] virtual std::string error() const { return {}; }
        /** its first result as text, for a reply that came from another node - TODO 690 */
        [[nodiscard]] virtual std::string result() const { return {}; }
    };
    using reply_ptr = std::shared_ptr<command_reply>;

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
         * Several records as one entry, applied whole by every member, the writer
         * included, so nobody ever holds part of it - TODO 665. `shards[i]` is the
         * shard of `records[i]`, checked against a split's fence like a single write.
         */
        virtual result commit_batch(const std::vector<std::string>& records,
                                    const std::vector<size_t>& shards, std::string& why) = 0;
        /**
         * Several records of one shard that the writer has already applied, as one
         * entry - TODO 738: what a command wrote under one lock. The writer skips it
         * as it commits, like a writer-applied `commit`; every other member applies
         * it under one hold of the shard.
         */
        virtual result commit_applied(const std::vector<std::string>& records, size_t shard,
                                      std::string& why) = 0;
        /**
         * A command for every member to run, `argv[0]` its name - TODO 669. Every
         * member runs it as it commits, in `space`, this one too; `reply` is what it
         * answered here, for the client. The caller holds no shard latch.
         */
        virtual result commit_command(const std::string& space, const std::vector<std::string>& argv,
                                      reply_ptr& reply, std::string& why) = 0;
        /** the group's number, the same on every member - TODO 666 */
        [[nodiscard]] virtual uint32_t group_number() const = 0;
        /** whether `shard` is one of the group's - TODO 666 */
        [[nodiscard]] virtual bool owns_shard(size_t shard) const = 0;
        /**
         * A batch across groups - TODO 666: `words` is its id, the groups it's in, and
         * every record of it; each member applies its group's. `shards` are this
         * group's, checked against a split's fence.
         */
        virtual result commit_xbatch(const std::vector<std::string>& words, const std::vector<size_t>& shards,
                                     std::string& why) = 0;
        /** every group has the batch `id`, so it can be forgotten - TODO 666 */
        virtual result commit_xclose(const std::string& id, std::string& why) = 0;
        /**
         * One step of a transaction in this group - TODO 693: tx_prepare puts its record
         * and intents in (`words` the id and its records), tx_commit applies it and
         * tx_abort drops it (`words` the id). `answer` is what the step said when it was
         * applied here: empty for done, or why not (a conflict, already aborted).
         */
        virtual result commit_tx(int op, const std::vector<std::string>& words, std::string& answer,
                                 std::string& why) = 0;
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
         * can have been elected since. Outside the lease, with `wait` (TODO 785),
         * it waits for a quorum to answer a round sent after the read came in, and
         * for what was committed by then to be applied here. Up to the group's
         * client timeout, on the calling thread, so not on an I/O thread.
         */
        [[nodiscard]] virtual bool leader_read(bool wait) const = 0;
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

    /** the steps of a transaction in a group - TODO 693 */
    constexpr int tx_prepare = 1;
    constexpr int tx_commit = 2;
    constexpr int tx_abort = 3;

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
     *
     * With `wait` (TODO 785), a leader the lease doesn't cover waits for a read
     * index, but only in a check without a shard: one under a shard's latch never
     * waits, as a write would be waiting on it.
     */
    void begin_reads(session* s, bool wait = false);
    /** `shard`, when there is one, is checked against a split's fence every time */
    void check_read(const binding_ptr& b, size_t shard = SIZE_MAX);
    /*
     * While one of these is alive on a thread, its reads at a shard are only checked
     * against a split's fence, not against the group's leader - TODO 796. It's for a
     * container command's type check: the plain key it looks up routes by its encoded
     * name and the container by the bare one, so in a split space the two are often in
     * different groups. Checked, a hash read needed a node that led both, and no node
     * could answer it. A write makes the same lookup unchecked.
     */
    struct unchecked_reads {
        unchecked_reads();
        ~unchecked_reads();
        unchecked_reads(const unchecked_reads&) = delete;
        unchecked_reads& operator=(const unchecked_reads&) = delete;
    };
    void end_reads();
    /** whether this node can read the group for `s` now, as the gate asks it */
    bool readable(binding& b, session& s, bool wait = false);
    /**
     * What check_read throws for a group this node leads but couldn't read without
     * waiting - TODO 785. Run again where it may wait, it could be answered.
     */
    struct read_needs_wait : std::runtime_error {
        using std::runtime_error::runtime_error;
    };
    /**
     * The group this thread's reads threw read_needs_wait for since begin_reads, or
     * null, cleared as it's asked. A read under a shard's latch throws it from inside
     * the command, which turns it into an error reply, so this is how the caller finds
     * out, and which group a copy that may wait has to check first.
     */
    binding_ptr take_read_wait();

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
         * Shutdown has begun - TODO 642. No more heartbeats, sent or taken, and new
         * Raft writes are refused at once, so the server's threads aren't left
         * waiting on commits a quorum may never answer while barchd stops them.
         */
        virtual void quiesce() {}
        /**
         * CLUSTER <subcommand> ... - the words after CLUSTER. `out` is the reply, one
         * line per element; false with `err` set is an error reply.
         */
        virtual bool command(const std::vector<std::string>& args, std::vector<std::string>& out,
                             std::string& err) = 0;
        /**
         * Push transaction `holder` aside for `mine` - TODO 696: abort it at its home,
         * group `home`, and then in group `at`, where it holds a key `mine` wants. Only
         * if `mine` is older, or `holder` has been pending a second (its coordinator is
         * quiet), and only once its home has it aborted. false, saying why, otherwise.
         */
        virtual bool push_tx(uint32_t /*home*/, uint32_t /*at*/, const std::string& /*holder*/,
                             const std::string& /*mine*/, std::string& why) {
            why = "this build has no cluster";
            return false;
        }
    };
    /** runtime::push_tx on the installed runtime - TODO 696 */
    bool push_tx(uint32_t home, uint32_t at, const std::string& holder, const std::string& mine, std::string& why);
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

    /*
     * Writes gathered into one batch entry - TODO 665. While a `batching` is alive on
     * a thread, a shard write in a replicated space builds its record, takes the
     * write back out of the tree and adds the record here, rather than committing it.
     * Whoever made the `batching` commits what it gathered with commit_batch, and the
     * writes appear, on every member, once that commits.
     */
    struct batched {
        std::string record;
        size_t shard{0};
        binding_ptr via{};
    };
    class batching {
    public:
        explicit batching(std::vector<batched>& into);
        ~batching();
        batching(const batching&) = delete;
        batching& operator=(const batching&) = delete;
    private:
        std::vector<batched>* outer;
    };
    /** where this thread's writes are being gathered, or null */
    std::vector<batched>* batch_now();
    /**
     * Commit what a `batching` gathered: one batch entry per group, in turn - TODO
     * 665, 670. True when every part committed. False with `why` at the first that
     * didn't; `committed` says how many parts did, so a caller can put those back.
     */
    bool commit_gathered(std::vector<batched>& got, std::string& why, size_t* committed = nullptr);

    /*
     * A client command running in a replicated space - TODO 738. While one is open on a
     * thread, the writes it makes under one shard's write lock stay in the tree and are
     * held back, and go in the log as one entry just before that lock is let go (see
     * shard::end_write_hold). So an HSET of ten fields is one entry, not ten, and a
     * follower applies it under one lock: it reads all ten or none.
     *
     * The commit happens in a destructor, which can't throw, so a refusal is kept here
     * for the caller to answer with, and every write the command tries after it is
     * refused straight away.
     */
    class command_writes {
    public:
        command_writes();
        ~command_writes();
        command_writes(const command_writes&) = delete;
        command_writes& operator=(const command_writes&) = delete;
        /** is a command open on this thread */
        static bool open();
        /** what a held commit said when it didn't commit, empty when they all did */
        static std::string& error();
    };

    /*
     * How a member runs a command from the log - TODO 669. Installed by the rpc layer.
     * False with `err` means it couldn't run it at all, and the member is rebuilt; the
     * command's own error is part of its reply, the same on every member.
     */
    /**
     * `index` is the entry's log index: a stored function's random seed (TODO 673).
     * `ms` is the time the node that logged it put in it, for barch.time, or -1 for an
     * entry from before they carried one (TODO 763).
     */
    using command_runner = std::function<bool(const std::string& space, const std::vector<std::string>& argv,
                                              uint64_t index, int64_t ms, reply_ptr& reply, std::string& err)>;
    void set_command_runner(command_runner r);
    bool run_command(const std::string& space, const std::vector<std::string>& argv, uint64_t index,
                     int64_t ms, reply_ptr& reply, std::string& err);

    /*
     * A command run from the log found this member doesn't hold what the others do -
     * TODO 677: a ROLLBACK on a member that lost the transaction's mark. The command
     * calls mark_diverged, and the commit thread rebuilds the member from the others
     * once the entry is applied, as for an entry it couldn't apply.
     */
    void mark_diverged(const std::string& why);

    /*
     * The shards a run from the log is for - TODO 690. In a space split over several
     * groups, a command that acts on the whole space (BEGIN, COMMIT, ROLLBACK, LOAD,
     * RETRIEVE) goes in every group's log, and each group's run acts on that group's
     * shards only, at that group's point in its log. Set by the cluster around such a
     * run; unset, a run is for every shard.
     */
    class run_shards {
    public:
        run_shards(size_t from, size_t to);
        ~run_shards();
        run_shards(const run_shards&) = delete;
        run_shards& operator=(const run_shards&) = delete;
    private:
        size_t was_from, was_to;
        bool was_set;
    };
    /** the range a run is for, false when it's for every shard */
    bool running_shards(size_t& from, size_t& to);
    /** whether this run acts on `shard` */
    bool run_takes_shard(size_t shard);
    /**
     * The range as a value, read on the run's thread, for a loop that hands shards to
     * other threads (each_shard_parallel): the range is this thread's own, and a
     * worker sees none, so it took every shard - TODO 690.
     */
    struct run_range {
        bool set{false};
        size_t from{0}, to{0};
        [[nodiscard]] bool takes(size_t shard) const { return !set || (shard >= from && shard < to); }
    };
    run_range current_run();

    /**
     * One of the server's own command entries into `space`'s log, and wait for every
     * member to run it - TODO 682, 685. `words` starts with a word the runner knows
     * (a queue's push, a cached miss); the entry carries no user or rights. The run
     * writes the space, so its shards count it as committing while it does, as
     * rpc_caller::call_through_log has them. Refused with NOTLEADER on a member that
     * doesn't lead the space's group; `reply` is the leader's own run's. `via` is the
     * group whose log it goes in, for an entry about one key of a space split over
     * several (TODO 689); the space's first group otherwise.
     */
    bool log_server_command(const std::shared_ptr<barch::key_space>& space, const std::vector<std::string>& words,
                            reply_ptr& reply, std::string& why, binding_ptr via = nullptr);
    /** what mark_diverged said on this thread since the last take, cleared */
    std::string take_diverged();

    /** whether any space on this node is bound to a Raft group - TODO 663 */
    bool any_bound();

    void install(runtime* r);
    /** nothing to do, and true, when the cluster isn't built in or raft_port is 0 */
    bool start(std::string& err);
    void stop();
    /** see runtime::quiesce; nothing to do when the cluster isn't built in */
    void quiesce();
    /** whether this build has the cluster in it */
    bool built();
    /** CLUSTER, for the command table; an error when the cluster isn't built in */
    bool command(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err);
}

#endif //BARCH_CLUSTER_HOOKS_H
