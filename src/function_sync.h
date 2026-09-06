#pragma once
#include <string>

#include <vector>

namespace barch {
    /**
     * One file a directory import turns into - see scan_directory and TODO 238.
     */
    struct import_file {
        /** the folded function name for a .luau file, or the key with its extension */
        std::string name;
        std::string source;
        /** where it came from, so an error can say which file */
        std::string path;
        bool luau{false};
    };
    /**
     * Walk `dir` the way the function sync walks a checkout: dot files skipped,
     * directories recursed with their name added to the prefix, `.luau` files
     * becoming stored functions and everything else keys named `prefix:sub:file`.
     *
     * Shared so LOADKEYS and the `functions_dir` sync cannot disagree about what a
     * directory means. It only reads - what to do with the result is the caller's.
     */
    bool scan_directory(const std::string& dir, const std::string& prefix,
                        std::vector<import_file>& out, std::string& err);

    /**
     * Apply every enabled repository. Empty is success; anything else is the first
     * reason, and the rest are attempted anyway - one repository that cannot fetch
     * is no reason to leave the others stale. `pin` is a git rev for this call.
     */
    std::string sync_functions(const std::string& pin = {});
    /** apply one repository by name - see TODO 252 */
    std::string sync_repo(const std::string& name, const std::string& pin = {});
    bool have_repo(const std::string& name);
    bool any_repo_configured();
    /**
     * Apply the repositories that said `asynch off`, which is a promise that they
     * are in place before anything is served. Everything else is left to the sync
     * thread so start-up never waits on somebody else's network. Non-empty is a
     * reason not to start.
     */
    std::string sync_startup_repos();
    /** one line per repository, as FUNCTIONS STATUS reports it */
    std::string functions_sync_status();
    void start_function_sync();
    void stop_function_sync();
    /** wake a waiting poller, used by FUNCTIONS SYNC */
    void request_function_sync();
    void request_repo_sync(const std::string& name);
}
