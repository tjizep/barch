//
// Created by teejip on 8/23/26.
//

#ifndef FUNCTION_API_H
#define FUNCTION_API_H
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

    /** SETF/REMF/KEYSF without a client. false fills err and writes nothing. */
    bool install(const barch::key_space_ptr& space, const std::string& name,
                 const std::string& source, std::string& err);
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

    /**
     * Run `call` in `space` as `user`, refusing what that user's rights do not
     * cover - the same check CALLF makes, with no connection behind it. This is
     * what a cron tick calls through: the entry says who may *schedule* a job,
     * this is what decides what the job may *do*, from the user it names.
     */
    bool call_as(const barch::key_space_ptr& space, const std::string& user,
                const std::string& call, const heap::vector<std::string>& args,
                Variable& out, std::string& err);
}
}

/** register the function commands for RESP, into the table functions_by_name() builds */
void register_function_api(function_map& r);

#endif //FUNCTION_API_H
