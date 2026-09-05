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

    /** apply the checkout. empty string is success; anything else is the reason.
     *  `pin` is a git rev for this call; empty uses functions_git_commit. */
    std::string sync_functions(const std::string& pin = {});
    std::string functions_sync_status();
    void start_function_sync();
    void stop_function_sync();
    /** wake a waiting poller, used by FUNCTIONS SYNC */
    void request_function_sync();
}
