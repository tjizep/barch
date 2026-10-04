#pragma once
//
// Library packages in the repository graph - TODO 593.
//
// A package whose package.luau says `kind = "dependency"` is a library. It never
// becomes a repository. Each commit of it that something pins is stored once, in
// the `repository` key space's graph, and whatever pins it gets a link:
//
//     /packages/<name>/source                       leaf: the url it came from
//     /packages/<name>/versions/<sha>/content/...   the tree at that commit, write-once
//     /packages/<name>/versions/<sha>/deps/<dep>    link: the version of <dep> it pinned
//     /packages/<name>/refs/{branch,tag}/<ref>      link: what <ref> last resolved to
//     /apps/<app>/pinsets/<n>/deps/<name>           link: one whole set of pins
//     /apps/<app>/current                           link: the pin set the app runs on
//     /spaces/<space>                               link: /apps/<app> for the app whose
//                                                   functions are in <space>
//
// require("@name/path") resolves through those links - phase 2. From a library
// it starts at the library's own version node, from anything else at the space
// the call runs in, so the version is always the one the code's own package
// pinned and never one the caller names.
//
// The commit is the version. A branch or a tag is only how one was found, which
// is why they live under refs and point into versions rather than being part of
// a version's name. Two apps pinning the same commit share one node.
//
// A version is staged under /packages/<name>/staging/<sha> and moved into
// versions/ only once its content and its links are all there, so anything under
// versions/ is complete.
//
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace barch::library {

    /** the key space the graph lives in */
    extern const char* space_name;

    struct file {
        /** relative to the package's root */
        std::string path;
        std::string body;
    };

    /**
     * what a url is compared as: without a trailing `/` or `.git`, so the two
     * spellings of one repository are not taken for two
     */
    std::string normal_url(const std::string& url);

    /**
     * Take `name` for `url`. The first url to use a name keeps it; another url is
     * refused, since without a registry the name is the only thing a require has
     * to go on. Empty is success.
     */
    std::string claim(const std::string& name, const std::string& url);

    /** whether a complete version is there */
    bool has_version(const std::string& name, const std::string& sha);

    /**
     * Store one version. Nothing happens when it is already there: a commit's tree
     * doesn't change, so neither does a version. `next` hands over the files one at
     * a time and answers false at the end, or false with `err` set when one can't be
     * read; they are written in bounded batches, so a package is never in memory
     * whole - TODO 597. `deps` are (name, sha) of versions that have to be there
     * already. Empty is success.
     */
    std::string put_version(const std::string& name, const std::string& sha,
                            const std::function<bool(file& out, std::string& err)>& next,
                            const std::vector<std::pair<std::string, std::string>>& deps);

    /** point refs/<kind>/<ref> at versions/<sha>; kind is "branch" or "tag" */
    std::string set_ref(const std::string& name, const std::string& kind,
                        const std::string& ref, const std::string& sha);

    /** (name, sha) of the libraries an app pins */
    using pins = std::vector<std::pair<std::string, std::string>>;

    /**
     * Make `want` the app's pins - TODO 595. A new pin set is built whole under
     * /apps/<app>/pinsets/<n> and then `current` is moved onto it, one link, so every
     * library switches at once. Nothing happens when `want` is what current already
     * holds. `previous` gets the pin set current named before, empty for none, and is
     * kept along with the new one; older ones go.
     */
    std::string switch_pins(const std::string& app, const pins& want, std::string& previous,
                            bool& switched);

    /**
     * A call's claim on the pin set it resolved - TODO 596. Held for as long as the
     * call lives, parked or not, and a replaced pin set isn't released while any
     * call holds it. Counted in this process: a call doesn't outlive the process.
     */
    class pinset_hold {
    public:
        pinset_hold() = default;
        ~pinset_hold() { release(); }
        pinset_hold(const pinset_hold&) = delete;
        pinset_hold& operator=(const pinset_hold&) = delete;
        /** hold `id` instead of whatever was held */
        void take(uint64_t id);
        void release();
        uint64_t held() const { return id; }
    private:
        uint64_t id{0};
    };

    /** whether any call in this process holds the pin set `id` */
    bool pinset_in_use(uint64_t id);

    /** point current back at `previous`, or at nothing when it's empty */
    std::string restore_pins(const std::string& app, const std::string& previous);

    /**
     * Point /spaces/<space> at /apps/<app> for each of `spaces`, canonical names,
     * and drop the ones that point at it and aren't listed. An app that has never
     * pinned a library has no /apps node and gets no links.
     */
    std::string bind_spaces(const std::string& app, const std::set<std::string>& spaces);

    /**
     * While one of these lives, a require from outside a library on this thread
     * resolves through /apps/<app> rather than the space it runs in. The function
     * sync checks what it imports in a scratch space of its own, whose name means
     * nothing, so this is how a module's top level `require("@lib")` finds the
     * libraries of the package being installed.
     */
    class installing {
    public:
        explicit installing(const std::string& app);
        ~installing();
        installing(const installing&) = delete;
        installing& operator=(const installing&) = delete;
    private:
        std::string prev;
    };

    struct module {
        /** the library version it is in */
        uint64_t version{0};
        /** the file's own node: a version never changes, so this is a cache key */
        uint64_t leaf{0};
        std::string source;
    };

    /**
     * Find a module for require. `from` is the version node of the library asking,
     * or 0 for code that is not in one, which resolves from `space`. `name` empty
     * is the library's own package, which only a library has. `path` is relative
     * to the package root; empty is init.luau and `.luau` is added when it is
     * missing. Read with the server's rights: what a package may reach is what
     * it declared, and an operator vouched for that when it was installed.
     *
     * `pinset` is the call's: 0 the first time, when the app's `current` is read
     * and kept there, so the rest of the call's requires use that same pin set
     * however often `current` moves meanwhile - TODO 595.
     */
    bool find_module(const std::string& space, uint64_t from, const std::string& name,
                     const std::string& path, uint64_t& pinset, module& out,
                     std::string& err);

    /**
     * one line per stored version: name, version, the apps that pin it, the
     * versions that link to it and the refs pointing at it - FUNCTIONS LIBRARIES
     */
    std::vector<std::string> list_versions();

    /**
     * per app: what it pins, as name@sha12 joined by commas, and the names it
     * reaches at more than one version through its libraries' own pins (empty
     * when there are none). Two versions of one name is allowed - each library
     * gets the one it asked for - but a table from one handed to the other may
     * not mean the same thing, so FUNCTIONS STATUS says so.
     */
    std::map<std::string, std::pair<std::string, std::string>> app_summaries();

    /**
     * Drop every version no app reaches - through its own pins or its libraries'
     * pins - with the refs pointing at it, and every package left with no
     * versions, which frees its name. Returns name@sha12 of what went;
     * `names_left` gets the packages still stored.
     */
    std::vector<std::string> collect(std::set<std::string>& names_left);
}
