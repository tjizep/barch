#pragma once
//
// package.luau - TODO 582.
//
// A git repository can carry a `package.luau` at its root. Its `setup()` returns a
// table that says how the repository wants to be installed: settings for the key
// spaces it uses, which folders go into which space and how, an HTTP server, and
// two hooks. The function sync reads it on every sync and applies it itself, in an
// order it controls, so the package only describes and never acts.
//
//     function setup()
//         return {
//             spaces = { shop = { shards = 7 }, geo = { shards = 7, ordered = true } },
//             load   = { { path = "luau", space = "shop" },
//                        { path = "app", space = "shop", as = "fs", fs_root = "/app" } },
//             http   = { space = "shop", key = "CONF", port = 18090, bind = "127.0.0.1" },
//             hooks  = { after = { space = "shop", call = "SEED" } },
//             depends = { { name = "ui", url = "https://example.com/ui.git", tag = "v1.2" } },
//         }
//     end
//
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace barch::package {

    /** where the convention puts it: the root of the checkout */
    extern const char* file_name;

    /** one `<space>.<setting>` for the configuration space */
    struct setting {
        std::string space;
        std::string name;
        std::string value;
    };

    struct load_entry {
        /** relative to the checkout, "." for all of it */
        std::string path;
        /** empty is the default space */
        std::string space;
        /** "keys": .luau becomes a stored function, the rest keys. "fs": the file store */
        std::string as{"keys"};
        std::string fs_root{"/"};
    };

    struct http_entry {
        std::string space;
        /** the config function, as HTTP START takes it. Empty asks every function */
        std::string key;
        uint16_t port{0};
        std::string bind;
    };

    struct hook {
        std::string space;
        /** a command function, run through call_as */
        std::string call;
        std::vector<std::string> args;
    };

    /**
     * another git repository the package needs - TODO 585. Installed as an ordinary
     * repository and synced before the package's own folders. Its own package.luau
     * can name more.
     */
    struct dependency {
        std::string name;
        /** as git/repositories/<name>/<setting> takes them, url always among them */
        std::vector<std::pair<std::string, std::string>> settings;
        /**
         * the version coordinate - TODO 593. Copies of what settings holds, plus the
         * tag, which is no repository setting: an application pinned by tag gets the
         * commit the tag resolved to. Empty is not given; no ref at all means main.
         */
        std::string url;
        std::string branch;
        std::string tag;
        std::string commit;
        bool pull{false};
    };

    struct spec {
        /**
         * "application", the default, installs the way every repository always has.
         * "dependency" is a library - TODO 593: stored by commit in the repository
         * graph, several versions at once, and code only, so no spaces, load, http
         * or hooks.
         */
        std::string kind{"application"};
        std::vector<dependency> depends;
        /** every space `spaces` names, settings or not: each exists after a sync */
        std::vector<std::string> spaces;
        std::vector<setting> settings;
        std::vector<load_entry> loads;
        std::optional<http_entry> http;
        std::optional<hook> before;
        std::optional<hook> after;
    };

    /**
     * Read `<dir>/package.luau`. No file is not an error: `present` comes back false
     * and `out` empty. Everything is checked here, before anything is applied, so a
     * typo cannot leave half a package behind.
     */
    bool read(const std::string& dir, bool& present, spec& out, std::string& err);

    /** the checking half of read, on what setup() returned as JSON */
    bool parse(const std::string& json, spec& out, std::string& err);

    /** whether `name` is a `<space>.<name>` setting a package may write */
    bool known_setting(const std::string& name);
}
