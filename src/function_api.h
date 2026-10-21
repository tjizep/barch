//
// Created by teejip on 8/23/26.
//

#ifndef FUNCTION_API_H
#define FUNCTION_API_H
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "caller.h"
#include "barch_apis.h"
#include "foreign/driver.h"
#include "key_space.h"
#include "sastam.h"

/*
 * Stored Luau functions. A function is an ordinary key under art::tfunction, so it
 * persists, replicates and exports like anything else, and the key space it lives in
 * is the one the command ran in - `space:SETF` or `USE space` then `SETF`.
 *
 * These are deliberately not a flag on SET and REM. The redis clones stay
 * bit-compatible that way, and more importantly defining a function is code, so it
 * carries its own ACL category rather than riding on "may write a string".
 */
extern "C" {
    int SETF(caller& call, const arg_t& argv);
    int GETF(caller& call, const arg_t& argv);
    int REMF(caller& call, const arg_t& argv);
    int KEYSF(caller& call, const arg_t& argv);
    int CALLF(caller& call, const arg_t& argv);
    int FUNCTIONS(caller& call, const arg_t& argv);
}

namespace barch {
namespace functions {
    /**
     * Which stored functions asked for native code (SETF ... AOT) - TODO 392.
     *
     * The source is the record: a name in this set compiles its call() through
     * CodeGen on the call path, in whatever session state runs it. A plain
     * set holds it - writes are rare next to calls, and the check is one
     * lookup per cold compile, not per call. Entries leave with REMF; there
     * is no per-space split, the key is already space-qualified.
     */
    bool wants_aot(const std::string& qualified_key);
    void set_aot(const std::string& qualified_key, bool on);
    void clear_aot(const std::string& qualified_key);
    /**
     * Find a stored function to answer for `name` and, if there is one, hand back
     * something the caller can run like any other command.
     *
     * `from_space` is the part before the dot in `KS1.PRINT_NAME`, empty when the name
     * carried none. An empty one resolves in the selected space and then in
     * the default space, where the globals live. A dotted name is always this
     * lookup, even when the name is a builtin - `HNSW.SET` is the stored function,
     * `HNSW:SET` is SET. See TODO 160.
     */
    /**
     * Returns what to run for `name`, or null if there is no such function.
     *
     * The answer is kept on the connection, so a name called twice is resolved once -
     * the lookup that decides a bare name is a function was the largest single thing
     * a call did. A pointer rather than a value, so the caller does not copy a
     * std::function per call either. See TODO 98 F5.
     */
    const caller::resolved* resolve(caller& call, const std::string& from_space,
                                    const std::string& name);

    /**
     * the source stored for `name`, looked up in `space` and then in the global one.
     *
     * Exposed for the foreign fill path, which names a function in its configuration
     * rather than carrying the script there - see TODO 139. False means no function
     * of that name, which is not the same as an error.
     */
    bool source_of(const barch::key_space_ptr& space, const std::string& name,
                   std::string& out);

    /**
     * the same store_access `barch.store` and `barch.space.NAME` use.
     * `store_for_owner` skips ACL - a private scratch space has no user.
     */
    barch::foreign::store_access store_for(const barch::key_space_ptr& space,
                                           const heap::vector<bool>& acl);
    barch::foreign::store_access store_for_owner(const barch::key_space_ptr& space);
    /**
     * `plain`, with each call asking `session` for the transaction open at that moment
     * and going through its overlay, or refused - TODO 707. `owner` for a store built
     * with store_for_owner - TODO 709.
     */
    barch::foreign::store_access tx_aware(barch::foreign::store_access plain, const barch::key_space_ptr& space,
                                          const heap::vector<bool>& acl,
                                          std::function<std::shared_ptr<barch::tx_session>()> session,
                                          bool owner = false);

    /**
     * HTTP request identity. Opaque sid in the cookie; user name in the store.
     * Set around a handler so barch.call and barch.store see this ACL, not owner.
     */
    struct http_ident {
        std::string user;
        heap::vector<bool> acl;
        std::string sid;
        bool sid_new{false};
        /** transport().user is set; barch.auth must not replace it */
        bool pinned{false};
        key_space_ptr space;
        /** the request's own session for a transaction, made at its first barch.call - TODO 709 */
        std::shared_ptr<barch::tx_session> tx;
    };
    http_ident*& http_ident_tls();
    /** barch.call for Crow: rights come from http_ident_tls(), not a RESP caller */
    barch::foreign::command_runner runner_for_http(const key_space_ptr& space);

    /**
     * drop what this space's functions were known to expose - TODO 188.
     *
     * Called whenever a function key is written or removed, which is the only
     * thing that can change the answer. Rebuilt on the next name that is not a
     * key, which is when it is next needed.
     */
    void forget_exposed(const std::string& space);

    /**
     * Bumped whenever something that can change compiled luau is written - a
     * function key through install/remove, a directory through LOADFS/LOADKEYS, a
     * file written from a script. A compiled copy remembers the epoch it was built
     * at, and a session that finds a newer one throws its copy away and compiles
     * again. See TODO 243.
     *
     * Deliberately one counter for the whole process rather than one per space or
     * per key: it costs a relaxed load per call, and over-invalidating on an
     * unrelated write is far easier to reason about than tracking which key a
     * compiled function might have read. Writes are rare next to calls.
     *
     * It does not see a plain `SET fs:d:...` over RESP. Files written that way are
     * not noticed by a session that has already compiled them.
     */
    uint64_t compile_epoch();
    void bump_compile_epoch();

    /**
     * Publishing is per name, because publishing one thing must not publish every
     * other change that happened to be staged - that was the first attempt and it
     * defeated the point of asking. `publish_compiled` records that this one entry
     * has moved, and `published_at` says when.
     *
     * The global epoch stays as the fast path: a compiled copy whose epoch matches
     * it is current and nothing is looked up. Only after somebody publishes does a
     * call consult the map, once, and then catch its own epoch up.
     *
     * The keys are the ones the luau side caches under - `compiled_key` for a
     * function, `compiled_path_key` for a module in the file store. Built here so
     * that the publisher and the cache cannot disagree about the shape.
     */
    uint64_t published_at(const std::string& key);
    void publish_compiled(const std::string& key);
    std::string compiled_key(const std::string& space, const std::string& folded_name);
    std::string compiled_path_key(const std::string& space, const std::string& path);

    /** one command a resp transport() exposes, as FUNCTIONS COMMANDS shows it */
    struct exposed_info {
        std::string name;
        std::string key;
        std::vector<std::string> categories;
    };
    heap::vector<exposed_info> exposed_commands(const barch::key_space_ptr& space);

    /**
     * Run a stored function without a client - the C++ side of `CALLF`.
     *
     * `resolve` needs a caller and every caller so far has been a connection or an
     * HTTP request, so nothing inside the server could call a stored function. The
     * file source needs to (TODO 263) and it is the sort of thing that will be
     * wanted again. Runs with owner rights, because the caller is the server.
     *
     * False fills `err`; a function that parks is refused rather than waited for,
     * the same as CALLF refuses one.
     */
    bool call_named(const barch::key_space_ptr& space, const std::string& name,
                    const std::vector<std::string>& args, Variable& out, std::string& err);

    /**
     * Function sources gathered into a Raft batch, not in the store yet - TODO 665.
     * While one is alive on a thread, a require() and source_in() see these first, so
     * a function in the batch can need a module installed by the same batch. An
     * empty optional is a function the batch removes.
     */
    class gathered_sources {
    public:
        gathered_sources();
        ~gathered_sources();
        gathered_sources(const gathered_sources&) = delete;
        gathered_sources& operator=(const gathered_sources&) = delete;
        void set(const barch::key_space_ptr& space, const std::string& name, const std::string& source);
        void remove(const barch::key_space_ptr& space, const std::string& name);
        /** what the batch did to `name` in `space`: nothing, a source, or removed */
        [[nodiscard]] const std::optional<std::string>* find(const std::string& canonical_space,
                                                             const std::string& folded) const;
    private:
        gathered_sources* outer;
        std::map<std::pair<std::string, std::string>, std::optional<std::string>> sources;
    };

    /**
     * Whether calling the stored function `name` may write - TODO 672: false only
     * when its `--@barch` header says `"write": false`, or it's a command a resp
     * transport() exposes without the write category. `name` may be dotted
     * (`SPACE.NAME`). Looked up the way a call resolves it: the current space, then
     * the globals. A name that isn't a function counts as one that may write.
     */
    bool may_write(const barch::key_space_ptr& current, const std::string& name);

    /** how a call to a stored function reaches every member - TODO 673 */
    enum class replication {
        read_only,  ///< "write": false; nothing to replicate
        log,        ///< the call goes in the log and every member runs it (the default)
        effects,    ///< "replicate": "effects"; runs on the leader, its writes replicated one by one
    };
    /** `name` looked up as may_write looks it up; a name that isn't a function is `log` */
    replication replication_of(const barch::key_space_ptr& current, const std::string& name);

    /**
     * A stored function running from a Raft log on this thread - TODO 673. Every
     * member runs it as the entry commits, so it has to do the same thing on each:
     * the run uses `seed` for math.random, has no wall-clock deadline, and is refused
     * whatever reaches outside the data. Set by the cluster's command runner.
     */
    class from_log {
    public:
        explicit from_log(uint64_t seed);
        ~from_log();
        from_log(const from_log&) = delete;
        from_log& operator=(const from_log&) = delete;
        const uint64_t seed;
    private:
        from_log* outer;
    };
    /** the run from the log this thread is in, or null */
    const from_log* running_from_log();
    /**
     * Why a function run from the log can't run this command through barch.call, or
     * empty when it can - TODO 673. Reads, replicated writes and logged commands are
     * the same on every member. A save would wait on the run itself (the space's
     * `log_runs`), other writes change the server or one node's copy, and a function
     * that says "replicate": "effects" reaches outside the data.
     */
    std::string refused_from_log(const barch::key_space_ptr& current, const std::vector<std::string>& argv);
    /**
     * Whether a command a function runs through barch.call writes a space - TODO 672:
     * a builtin write that isn't Raft's `server` class (which changes the server or
     * the connection, not data), or a stored function that may write.
     */
    bool command_writes(const barch::key_space_ptr& current, const std::vector<std::string>& argv);

    /** whether a stored key is a function's - TODO 667 */
    bool is_function_key(art::value_type key);
    /**
     * A function's key changed in `space` some way other than install or remove - TODO
     * 667: applied from a Raft log (a record, a batch), or brought by a copy. Does what
     * they do after the write: the exposed-command cache and the server's kept states
     * forget the space's functions, and cron and the queue service look again.
     */
    void function_keys_changed(const barch::key_space_ptr& space);
    /**
     * RELOAD's publish, for these compiled keys - TODO 667. In a space replicated with
     * Raft it goes in the space's log so every member publishes; anywhere else, and
     * inside a run from the log, it's this node's publish_compiled.
     */
    void publish_everywhere(const barch::key_space_ptr& space, const std::vector<std::string>& keys);
    /**
     * call_as, for the server's own calls into a space replicated with Raft - TODO 667:
     * a stored function that would run from the log goes through the log, as `user`,
     * so it runs once on every member. Anything else is call_as. The caller runs it
     * only where the space's group is led.
     */
    bool call_as_logged(const barch::key_space_ptr& space, const std::string& user, const std::string& call,
                        const heap::vector<std::string>& args, Variable& out, std::string& err);

    /** the word a logged publish starts with; no client command can be called it */
    constexpr const char* publish_word = "\x01PUBLISH";

    /** SETF/REMF/KEYSF without a client. false fills err and writes nothing. */
    bool install(const barch::key_space_ptr& space, const std::string& name,
                 const std::string& source, std::string& err, bool aot = false);
    bool remove(const barch::key_space_ptr& space, const std::string& name);
    heap::vector<std::string> names(const barch::key_space_ptr& space);
    bool source_in(const barch::key_space_ptr& space, const std::string& name,
                   std::string& out);

    /** one `kind = "cron"` transport(), found under configuration:cron/jobs/<name> */
    struct cron_entry {
        /** the last path segment - configuration:cron/jobs/compact is "compact" */
        std::string name;
        /** the full function key, as SETF/GETF/REMF see it */
        std::string key;
        barch::foreign::cron_spec spec;
        /** non-empty when the source would not compile, or the transport() is bad */
        std::string parse_err;
    };
    /**
     * Every cron transport() declared in the configuration space - TODO 249. A
     * key under cron/jobs/ with no transport(), or one of another kind, is not an
     * entry and is not among these; one that fails to compile is, with parse_err
     * set, so the scheduler can say which job is broken rather than only that one
     * is missing.
     */
    heap::vector<cron_entry> cron_jobs();

    /** one `kind = "queue"` transport(), found under configuration:queues/<name> */
    struct queue_entry {
        /** the last path segment - configuration:queues/mail is "mail" */
        std::string name;
        /** the full function key, as SETF/GETF/REMF see it */
        std::string key;
        barch::foreign::queue_spec spec;
        /** non-empty when the source would not compile, or the transport() is bad */
        std::string parse_err;
    };
    /**
     * Every queue transport() declared in the configuration space - TODO 366.
     *
     * Same rules as cron_jobs: a key under queues/ with no transport(), or one
     * of another kind, is not an entry; one that will not compile is, with
     * parse_err set, so the consumer can say which queue is broken rather than
     * only that one is missing.
     *
     * Note the declaration's key name and `spec.name` are two different things.
     * The key is where the declaration lives, the spec's name is what senders
     * publish to, and nothing makes them match - a queue declared at
     * `queues/mail` may take messages published to "outbound". Keeping them
     * separate is what lets a declaration be renamed without every sender
     * having to change.
     */
    heap::vector<queue_entry> queue_declarations();

    /**
     * Run `call` in `space` as `user`, refusing what that user's rights do not
     * cover - the same check CALLF makes, with no connection behind it. This is
     * what a cron tick calls through: the entry says who may *schedule* a job,
     * this is what decides what the job may *do*, from the user it names.
     */
    bool call_as(const barch::key_space_ptr& space, const std::string& user,
                const std::string& call, const heap::vector<std::string>& args,
                Variable& out, std::string& err);

    /** how an asynchronous call answers: once, on whichever thread it ends on */
    typedef std::function<void(bool ok, Variable out, std::string err)> call_done;

    /**
     * call_as without the wait - TODO 436. `done` is called exactly once: inline
     * when the call is refused or is a builtin, otherwise from the function pool
     * when the stored function ends, so a handler parked on I/O holds no thread
     * of the caller's.
     */
    void call_as_async(const barch::key_space_ptr& space, const std::string& user,
                       const std::string& call, const heap::vector<std::string>& args,
                       call_done done);
}
}

/** register the function commands for RESP, into the table functions_by_name() builds */
void register_function_api(function_map& r);

#endif //FUNCTION_API_H
