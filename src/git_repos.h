#pragma once
//
// Git repositories, configured as a directory of little files in the configuration
// space - see TODO 252.
//
// Before this there were six flat globals (`functions_dir`, `functions_git_*`,
// `functions_sync_ms`), so there was one checkout, one remote, one branch and one
// key. One repository could already feed several key spaces, because a top level
// folder maps onto the space of that name, but two repositories could not exist at
// once. The settings live under
//
//     configuration:git/repositories/<name>/<setting>
//
// one file per setting, because the shape is open ended - N repositories times M
// settings - and CONFIG is a flat namespace that cannot hold that without inventing
// a separator and pretending it is not a tree.
//
#include <string>

#include "sastam.h"

namespace barch {

    /** one repository, as the configuration space describes it */
    struct repo_conf {
        std::string name;
        /** where to fetch from. Empty means the checkout is somebody else's business */
        std::string url;
        /** the checkout. Empty defaults to <functions_dir>/<name> */
        std::string dir;
        std::string branch{"main"};
        /** pin to this rev; empty follows the branch */
        std::string commit;
        /** fetch before scanning. Without a url this needs an existing checkout */
        bool pull{false};
        /** poll interval in ms; 0 never polls and waits to be asked */
        uint64_t ms{0};
        /**
         * a *reference* to a deploy key - a path, or file:/path - never the key
         * itself. A key stored here would be in the saved shards and replicated.
         */
        std::string ssh_key;
        /**
         * where the checkout lands. Empty is the historical behaviour: each top
         * level folder becomes the key space of that name. Otherwise everything
         * goes into this one space.
         */
        std::string space;
        /**
         * what an import means: "keys" is the historical mapping - `.luau` becomes
         * a stored function and everything else a key - and "fs" puts the checkout
         * in the chunked file store instead, which is what a repository of images,
         * fonts or a built web application wants. See TODO 253.
         */
        std::string as{"keys"};
        /** where an `as = fs` checkout hangs, the way LOADFS takes a root */
        std::string fs_root{"/"};
        bool enabled{true};
        /**
         * true keeps start-up off the network: the first fetch happens on the sync
         * thread and the listener opens on time. False is for the repository that
         * must be present before anything serves, and blocks the boot.
         */
        bool asynch{true};
        /**
         * why this repository will not run, when a setting is not usable. Checked
         * where the settings are read rather than where they are written: they are
         * ordinary keys and anyone can SET one, so the read is the only place that
         * sees all of them. A repository with this set is disabled and says so in
         * FUNCTIONS STATUS, which beats syncing something half configured.
         */
        std::string invalid;
    };

    /** where a repository with no `dir` of its own is checked out */
    std::string repo_default_dir(const std::string& name);

    /**
     * every repository configured under `git/repositories/`, in name order.
     *
     * When there are none and the old `functions_dir` is set, the six globals are
     * returned as one repository called `default`, so an existing deployment keeps
     * working and a new one never meets them.
     */
    heap::vector<repo_conf> read_repos();

    /**
     * refuse what cannot be resolved: two repositories declaring the same `space`,
     * or sharing a checkout `dir`. Both of a conflicting pair are disabled - picking
     * a winner would make the survivor depend on iteration order - and every other
     * repository is left alone. Returns one line per conflict.
     */
    heap::vector<std::string> refuse_overlap(heap::vector<repo_conf>& repos);

    /** validate one setting name and value, for the write path. empty is fine */
    std::string check_repo_setting(const std::string& setting, const std::string& value);
}
