#include "repo_library.h"
#include "scratch_container.h"

#include "fs.h"
#include "function_api.h"
#include "graph.h"
#include "key_space.h"
#include "lzr_log.h"

#include <algorithm>
#include <functional>
#include <chrono>
#include <cstdlib>
#include <map>
#include <mutex>
#include <unordered_map>

namespace barch::library {

const char* space_name = "repository";

namespace {

graph::view view_of(const key_space_ptr& space) {
    graph::view acc;
    acc.space = space;
    acc.may_read = acc.may_write = true;
    return acc;
}

key_space_ptr space_or_null() {
    return keyspace_exists(space_name) ? get_keyspace(space_name) : nullptr;
}

std::string package_path(const std::string& name) {
    return "/packages/" + name;
}

thread_local std::string installing_app;

/** a space's segment under /spaces: the default space's name is empty */
std::string space_segment(const std::string& space) {
    return space.empty() ? std::string(":default") : space;
}

/** the first-created edge `name` under `parent`, the way a path step resolves */
bool child(const graph::view& acc, graph::node_id parent, const std::string& name,
           graph::node& out) {
    std::vector<graph::edge> edges;
    if (!graph::edges_for(acc, parent, name, edges) || edges.empty())
        return false;
    return graph::stat_node(acc, edges.front().child, out);
}

std::string version_path(const std::string& name, const std::string& sha) {
    return package_path(name) + "/versions/" + sha;
}

std::string parent_path(const std::string& path) {
    auto at = path.rfind('/');
    return at == 0 || at == std::string::npos ? std::string("/") : path.substr(0, at);
}

/**
 * add a mkdir to `b` for every directory on the way to `path` (itself included)
 * that isn't there yet. `planned` holds the ones this batch already makes, since
 * a batch may build under a directory an earlier item of it creates.
 */
void dirs(const graph::view& acc, graph::batch& b, const std::string& path,
          std::set<std::string>& planned) {
    if (path == "/" || planned.count(path))
        return;
    graph::node n;
    if (graph::resolve(acc, path, n))
        return;
    dirs(acc, b, parent_path(path), planned);
    b.mkdir(path);
    planned.insert(path);
}

std::string make_dirs(const key_space_ptr& space, const std::string& path) {
    auto acc = view_of(space);
    graph::batch b(space);
    std::set<std::string> planned;
    dirs(acc, b, path, planned);
    std::string err;
    if (!planned.empty() && !b.commit(err))
        return path + ": " + err;
    return {};
}

bool read_leaf(const key_space_ptr& space, const graph::node& n, std::string& body) {
    body.clear();
    if (n.dir)
        return false;
    if (!n.inode)
        return true;
    auto owner = functions::store_for_owner(space);
    fs::file f;
    std::string err;
    return fs::file::open_id(owner, n.inode, f, err) && f.read_at(0, f.meta().size, body, err);
}

/**
 * Point the link at `path` to `target`. A new edge is made before the old one
 * goes - the older edge wins a lookup until it's unlinked - so whatever resolves
 * the path in between sees one version or the other and never nothing.
 */
std::string relink(const key_space_ptr& space, const std::string& path, graph::node_id target) {
    auto acc = view_of(space);
    graph::node have;
    graph::edge old{};
    const bool had = graph::resolve(acc, path, have, &old);
    if (had && have.id == target)
        return {};
    auto err = make_dirs(space, parent_path(path));
    if (!err.empty())
        return err;
    {
        graph::batch b(space);
        if (!b.link(target, path, err) || !b.commit(err))
            return path + ": " + err;
    }
    if (had) {
        graph::batch b(space);
        if (!b.unlink(path, err, old.id) || !b.commit(err))
            return path + ": " + err;
    }
    return {};
}

} // namespace

std::string normal_url(const std::string& url) {
    std::string u = url;
    while (u.size() > 1 && u.back() == '/')
        u.pop_back();
    if (u.size() > 4 && u.compare(u.size() - 4, 4, ".git") == 0)
        u.resize(u.size() - 4);
    while (u.size() > 1 && u.back() == '/')
        u.pop_back();
    return u;
}

std::string claim(const std::string& name, const std::string& url) {
    auto space = get_keyspace(space_name);
    if (!space)
        return "no repository space";
    auto acc = view_of(space);
    const auto at = package_path(name) + "/source";
    graph::node n;
    if (graph::resolve(acc, at, n)) {
        std::string have;
        if (!read_leaf(space, n, have))
            return at + " does not read";
        if (normal_url(have) != normal_url(url))
            return name + " is already a library, with another url (" + have + ")";
        return {};
    }
    auto err = make_dirs(space, package_path(name));
    if (!err.empty())
        return err;
    graph::batch b(space);
    b.write(at, url, "text/plain");
    if (!b.commit(err))
        return at + ": " + err;
    return {};
}

bool has_version(const std::string& name, const std::string& sha) {
    auto space = space_or_null();
    if (!space)
        return false;
    graph::node n;
    return graph::resolve(view_of(space), version_path(name, sha), n) && n.dir;
}

std::string put_version(const std::string& name, const std::string& sha,
                        const std::function<bool(file& out, std::string& err)>& next,
                        const std::vector<std::pair<std::string, std::string>>& deps) {
    if (has_version(name, sha))
        return {};
    auto space = get_keyspace(space_name);
    if (!space)
        return "no repository space";
    auto acc = view_of(space);
    const auto staged = package_path(name) + "/staging/" + sha;
    std::string err;
    // what an earlier attempt left half made
    graph::node n;
    if (graph::resolve(acc, staged, n)) {
        graph::batch b(space);
        if (!b.remove(staged, true, err) || !b.commit(err))
            return staged + ": " + err;
    }
    {
        graph::batch b(space);
        std::set<std::string> planned;
        dirs(acc, b, package_path(name) + "/versions", planned);
        dirs(acc, b, staged + "/content", planned);
        dirs(acc, b, staged + "/deps", planned);
        if (!b.commit(err))
            return staged + ": " + err;
    }
    /*
     * The content goes in a batch at a time, never all of it at once - TODO 597. A
     * batch holds what it stages in memory until it commits, and a large package
     * held whole, read and then staged, was the package twice over in RAM. Staging
     * doesn't need one batch: nothing can reach it, and the rename below is what
     * publishes the version.
     */
    constexpr size_t batch_bytes = 8u << 20;
    constexpr size_t batch_files = 1000;
    for (bool more = true; more;) {
        graph::batch b(space);
        std::set<std::string> planned;
        size_t bytes = 0, files = 0;
        while (bytes < batch_bytes && files < batch_files) {
            file f;
            if (!next(f, err)) {
                if (!err.empty())
                    return err;
                more = false;
                break;
            }
            const auto at = staged + "/content/" + f.path;
            dirs(acc, b, parent_path(at), planned);
            bytes += f.body.size();
            ++files;
            b.write(at, std::move(f.body), "");
        }
        if (files && !b.commit(err))
            return staged + ": " + err;
    }
    for (const auto& [dep, dep_sha] : deps) {
        graph::node target;
        if (!graph::resolve(acc, version_path(dep, dep_sha), target))
            return dep + " " + dep_sha + " is not stored";
        graph::batch b(space);
        if (!b.link(target.id, staged + "/deps/" + dep, err) || !b.commit(err))
            return staged + "/deps/" + dep + ": " + err;
    }
    graph::batch b(space);
    if (!b.rename(staged, version_path(name, sha), err) || !b.commit(err))
        return version_path(name, sha) + ": " + err;
    return {};
}

std::string set_ref(const std::string& name, const std::string& kind,
                    const std::string& ref, const std::string& sha) {
    auto space = get_keyspace(space_name);
    if (!space)
        return "no repository space";
    graph::node target;
    if (!graph::resolve(view_of(space), version_path(name, sha), target))
        return name + " " + sha + " is not stored";
    return relink(space, package_path(name) + "/refs/" + kind + "/" + ref, target.id);
}

namespace {

std::string app_path(const std::string& app) {
    return "/apps/" + app;
}

/** name -> version node of what a pin set links to */
std::map<std::string, graph::node_id> links_of(const graph::view& acc, graph::node_id set) {
    std::map<std::string, graph::node_id> out;
    graph::node deps;
    if (!child(acc, set, "deps", deps))
        return out;
    std::vector<graph::edge> edges;
    graph::children_of(acc, deps.id, edges);
    for (const auto& e : edges)
        out.emplace(e.name, e.child);       // first-created wins, as a lookup does
    return out;
}

/** the pin set `current` names, by its name under pinsets/; empty when there is none */
std::string current_name(const graph::view& acc, const std::string& app, graph::node& set) {
    graph::node sets;
    if (!graph::resolve(acc, app_path(app) + "/current", set) ||
        !graph::resolve(acc, app_path(app) + "/pinsets", sets))
        return {};
    std::vector<graph::edge> edges;
    graph::children_of(acc, sets.id, edges);
    for (const auto& e : edges)
        if (e.child == set.id)
            return e.name;
    return {};
}

std::string unlink_all(const key_space_ptr& space, const std::string& path) {
    auto acc = view_of(space);
    for (;;) {
        graph::node n;
        graph::edge e{};
        if (!graph::resolve(acc, path, n, &e))
            return {};
        graph::batch b(space);
        std::string err;
        if (!b.unlink(path, err, e.id) || !b.commit(err))
            return path + ": " + err;
    }
}

uint64_t now_ms() {
    return (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/**
 * when a pin set stopped being current. A call that resolved it just before can
 * still be using it, so it is kept until that call has to have ended - see collect.
 */
std::string stamp_superseded(const key_space_ptr& space, const std::string& set) {
    graph::node have;
    if (graph::resolve(view_of(space), set + "/superseded", have))
        return {};
    graph::batch b(space);
    b.write(set + "/superseded", std::to_string(now_ms()), "text/plain");
    std::string err;
    if (!b.commit(err))
        return set + "/superseded: " + err;
    return {};
}

} // namespace

std::string switch_pins(const std::string& app, const pins& want, std::string& previous,
                        bool& switched) {
    switched = false;
    previous.clear();
    if (want.empty() && !space_or_null())
        return {};
    auto space = get_keyspace(space_name);
    if (!space)
        return "no repository space";
    auto acc = view_of(space);
    std::map<std::string, graph::node_id> wanted;
    for (const auto& [name, sha] : want) {
        graph::node v;
        if (!graph::resolve(acc, version_path(name, sha), v))
            return name + " " + sha + " is not stored";
        wanted[name] = v.id;
    }
    graph::node now;
    previous = current_name(acc, app, now);
    const bool same = !previous.empty() ? links_of(acc, now.id) == wanted : wanted.empty();
    // what phases 1 and 2 left: one deps directory under the app, no pin sets
    auto failed = unlink_all(space, app_path(app) + "/deps");
    if (!failed.empty())
        return failed;
    if (same)
        return {};

    // a new pin set, complete before anything can reach it
    uint64_t next = 1;
    graph::node sets;
    if (graph::resolve(acc, app_path(app) + "/pinsets", sets)) {
        std::vector<graph::edge> edges;
        graph::children_of(acc, sets.id, edges);
        for (const auto& e : edges) {
            char* end = nullptr;
            auto n = std::strtoull(e.name.c_str(), &end, 10);
            if (end && *end == 0 && n >= next)
                next = n + 1;
        }
    }
    const auto name = std::to_string(next);
    const auto at = app_path(app) + "/pinsets/" + name;
    failed = make_dirs(space, at + "/deps");
    if (!failed.empty())
        return failed;
    for (const auto& [lib, id] : wanted) {
        graph::batch b(space);
        std::string err;
        if (!b.link(id, at + "/deps/" + lib, err) || !b.commit(err))
            return at + "/deps/" + lib + ": " + err;
    }
    graph::node set;
    if (!graph::resolve(acc, at, set))
        return at + " did not read back";
    // the switch: one link, and every library moves with it
    failed = relink(space, app_path(app) + "/current", set.id);
    if (!failed.empty())
        return failed;
    switched = true;
    // the previous one stays, for a call that resolved it just before the switch and
    // for switching back, until collect finds it has been replaced long enough
    if (!previous.empty())
        return stamp_superseded(space, app_path(app) + "/pinsets/" + previous);
    return {};
}

namespace {
std::mutex holds_mu;
std::unordered_map<uint64_t, uint64_t> holds;
}

void pinset_hold::take(uint64_t want) {
    if (want == id)
        return;
    release();
    if (!want)
        return;
    std::lock_guard<std::mutex> g(holds_mu);
    ++holds[want];
    id = want;
}

void pinset_hold::release() {
    if (!id)
        return;
    std::lock_guard<std::mutex> g(holds_mu);
    auto at = holds.find(id);
    if (at != holds.end() && --at->second == 0)
        holds.erase(at);
    id = 0;
}

bool pinset_in_use(uint64_t id) {
    std::lock_guard<std::mutex> g(holds_mu);
    return holds.count(id) != 0;
}

std::string restore_pins(const std::string& app, const std::string& previous) {
    auto space = space_or_null();
    if (!space)
        return {};
    auto acc = view_of(space);
    // the one being given up was current for a moment, so a call may hold it
    graph::node failed_set;
    const auto failed_name = current_name(acc, app, failed_set);
    std::string err;
    if (previous.empty()) {
        err = unlink_all(space, app_path(app) + "/current");
    } else {
        const auto at = app_path(app) + "/pinsets/" + previous;
        graph::node set;
        if (!graph::resolve(acc, at, set))
            return at + " is gone";
        err = relink(space, app_path(app) + "/current", set.id);
        // current again, so not on its way out
        if (err.empty())
            err = unlink_all(space, at + "/superseded");
    }
    if (err.empty() && !failed_name.empty() && failed_name != previous)
        err = stamp_superseded(space, app_path(app) + "/pinsets/" + failed_name);
    return err;
}

std::string bind_spaces(const std::string& app, const std::set<std::string>& spaces) {
    auto space = space_or_null();
    if (!space)
        return {};
    auto acc = view_of(space);
    graph::node at;
    if (!graph::resolve(acc, "/apps/" + app, at))
        return {};
    std::set<std::string> wanted;
    for (const auto& s : spaces) {
        wanted.insert(space_segment(s));
        auto failed = relink(space, "/spaces/" + space_segment(s), at.id);
        if (!failed.empty())
            return failed;
    }
    graph::node all;
    if (!graph::resolve(acc, "/spaces", all))
        return {};
    std::vector<graph::edge> edges;
    if (!graph::children_of(acc, all.id, edges))
        return "/spaces does not read";
    for (const auto& e : edges) {
        if (e.child != at.id || wanted.count(e.name))
            continue;
        graph::batch b(space);
        std::string err;
        if (!b.unlink("/spaces/" + e.name, err, e.id) || !b.commit(err))
            return "/spaces/" + e.name + ": " + err;
    }
    return {};
}

installing::installing(const std::string& app) : prev(installing_app) {
    installing_app = app;
}

installing::~installing() {
    installing_app = prev;
}

bool find_module(const std::string& space, uint64_t from, const std::string& name,
                 const std::string& path, uint64_t& pinset, module& out, std::string& err) {
    auto repo = space_or_null();
    auto acc = view_of(repo);
    const std::string where = from ? std::string("this library")
                            : !installing_app.empty() ? "package " + installing_app
                            : "key space " + (space.empty() ? std::string("(default)") : space);
    graph::node version;
    if (name.empty()) {
        if (!from) {
            err = "require wants @name: only a library has modules of its own";
            return false;
        }
        if (!repo || !graph::stat_node(acc, from, version)) {
            err = "the library asking is not stored any more";
            return false;
        }
    } else {
        graph::node base, deps;
        bool found = false;
        if (repo && from) {
            found = graph::stat_node(acc, from, base);
        } else if (repo && pinset) {
            // the pin set this call already resolved: every require in one call gets
            // the same one, however many times `current` switches meanwhile
            found = graph::stat_node(acc, pinset, base);
        } else if (repo) {
            graph::node app;
            found = (!installing_app.empty()
                     ? graph::resolve(acc, "/apps/" + installing_app, app)
                     : graph::resolve(acc, "/spaces/" + space_segment(space), app))
                    && child(acc, app.id, "current", base);
            if (found)
                pinset = base.id;
        }
        found = found && child(acc, base.id, "deps", deps) && child(acc, deps.id, name, version);
        if (!found) {
            err = name + " is not a dependency of " + where;
            return false;
        }
    }
    std::string rel = path.empty() ? std::string("init.luau") : path;
    if (rel.size() < 5 || rel.compare(rel.size() - 5, 5, ".luau") != 0)
        rel += ".luau";
    std::string clean;
    if (!graph::normalise("/" + rel, clean, err)) {
        err = "require path " + path + ": " + err;
        return false;
    }
    graph::node at;
    bool found = child(acc, version.id, "content", at);
    for (const auto& step : graph::split(clean)) {
        if (!found)
            break;
        found = child(acc, at.id, step, at);
    }
    const std::string label = (name.empty() ? std::string("its own package") : "@" + name);
    if (!found || at.dir) {
        err = label + " has no module " + rel;
        return false;
    }
    if (!read_leaf(repo, at, out.source)) {
        err = label + ": " + rel + " does not read";
        return false;
    }
    out.version = version.id;
    out.leaf = at.id;
    return true;
}

namespace {

/** everything stored, walked once: who pins what, and what each version pins */
struct inventory {
    struct one {
        std::string name;
        std::string sha;
        std::vector<std::string> pinned_by;
        /** name@sha12 of the versions that link to this one */
        std::vector<std::string> used_by;
        std::vector<std::string> refs;
        /** the version nodes this one links to */
        std::vector<graph::node_id> deps;
    };
    std::map<graph::node_id, one> versions;
    /** app -> (name, version node) of its current pin set */
    std::map<std::string, std::vector<std::pair<std::string, graph::node_id>>> apps;
    /** the versions every kept pin set links to, the previous ones included */
    std::vector<graph::node_id> pinned_anywhere;
};

std::string short_of(const inventory::one& v) {
    return v.name + "@" + v.sha.substr(0, 12);
}

/** the refs under `at`, which nest the way a branch name with a / does */
void walk_refs(const graph::view& acc, graph::node_id at, const std::string& prefix,
               inventory& inv, size_t depth) {
    if (depth > 16)
        return;
    std::vector<graph::edge> edges;
    if (!graph::children_of(acc, at, edges))
        return;
    for (const auto& e : edges) {
        auto v = inv.versions.find(e.child);
        if (v != inv.versions.end())
            v->second.refs.push_back(prefix + e.name);
        else
            walk_refs(acc, e.child, prefix + e.name + "/", inv, depth + 1);
    }
}

bool take_inventory(const key_space_ptr& space, inventory& inv) {
    auto acc = view_of(space);
    graph::node packages;
    if (graph::resolve(acc, "/packages", packages)) {
        std::vector<graph::edge> names;
        graph::children_of(acc, packages.id, names);
        for (const auto& p : names) {
            graph::node versions;
            if (!child(acc, p.child, "versions", versions))
                continue;
            std::vector<graph::edge> shas;
            graph::children_of(acc, versions.id, shas);
            for (const auto& v : shas) {
                auto& one = inv.versions[v.child];
                one.name = p.name;
                one.sha = v.name;
            }
        }
        for (auto& [id, v] : inv.versions) {
            graph::node deps;
            if (!child(acc, id, "deps", deps))
                continue;
            std::vector<graph::edge> edges;
            graph::children_of(acc, deps.id, edges);
            for (const auto& e : edges)
                v.deps.push_back(e.child);
        }
        for (auto& [id, v] : inv.versions) {
            for (auto dep : v.deps) {
                auto d = inv.versions.find(dep);
                if (d != inv.versions.end())
                    d->second.used_by.push_back(short_of(v));
            }
        }
        for (const auto& p : names) {
            graph::node refs;
            if (child(acc, p.child, "refs", refs))
                walk_refs(acc, refs.id, "", inv, 0);
        }
    }
    graph::node apps;
    if (graph::resolve(acc, "/apps", apps)) {
        std::vector<graph::edge> edges;
        graph::children_of(acc, apps.id, edges);
        for (const auto& a : edges) {
            graph::node current, sets;
            if (child(acc, a.child, "current", current)) {
                auto& pinned = inv.apps[a.name];
                for (const auto& [name, id] : links_of(acc, current.id)) {
                    pinned.emplace_back(name, id);
                    auto v = inv.versions.find(id);
                    if (v != inv.versions.end())
                        v->second.pinned_by.push_back(a.name);
                }
            }
            if (!child(acc, a.child, "pinsets", sets))
                continue;
            std::vector<graph::edge> kept;
            graph::children_of(acc, sets.id, kept);
            for (const auto& k : kept)
                for (const auto& [name, id] : links_of(acc, k.child))
                    inv.pinned_anywhere.push_back(id);
        }
    }
    return true;
}

/*
 * Remove the pin sets an app no longer runs on, once no call is using one - TODO 595,
 * 596. A call resolves its pin set once and holds it until it ends, however long that
 * is: parked calls run for longer in wall time than any deadline, which counts running
 * time, and a space can have a cap above the server's. So the hold count decides.
 *
 * The grace only covers the moment between a call reading `current` and taking its
 * hold: a set replaced in that moment shows no hold yet. Two seconds is a great deal
 * longer than that moment. One with no stamp, which nothing should leave, is stamped
 * now rather than trusted.
 */
void release_pinsets(const key_space_ptr& space) {
    auto acc = view_of(space);
    const uint64_t grace = 2000;
    const uint64_t now = now_ms();
    graph::node apps;
    if (!graph::resolve(acc, "/apps", apps))
        return;
    std::vector<graph::edge> each;
    graph::children_of(acc, apps.id, each);
    for (const auto& a : each) {
        graph::node current;
        const bool has_current = child(acc, a.child, "current", current);
        graph::node sets;
        if (!child(acc, a.child, "pinsets", sets))
            continue;
        std::vector<graph::edge> kept;
        graph::children_of(acc, sets.id, kept);
        for (const auto& k : kept) {
            if (has_current && k.child == current.id)
                continue;
            const auto at = app_path(a.name) + "/pinsets/" + k.name;
            graph::node stamp;
            std::string when;
            if (!child(acc, k.child, "superseded", stamp) || !read_leaf(space, stamp, when)) {
                (void) stamp_superseded(space, at);
                continue;
            }
            if (now < std::strtoull(when.c_str(), nullptr, 10) + grace ||
                pinset_in_use(k.child))
                continue;
            graph::batch b(space);
            std::string err;
            if (!b.remove(at, true, err) || !b.commit(err))
                barch::err({"library", "could not remove", at, err});
        }
    }
}

std::string joined(const std::vector<std::string>& parts) {
    std::string out;
    for (const auto& p : parts)
        out += (out.empty() ? "" : ",") + p;
    return out.empty() ? std::string("-") : out;
}

} // namespace

std::vector<std::string> list_versions() {
    std::vector<std::string> out;
    auto space = space_or_null();
    if (!space)
        return out;
    inventory inv;
    take_inventory(space, inv);
    std::vector<const inventory::one*> sorted;
    for (const auto& [id, v] : inv.versions)
        sorted.push_back(&v);
    std::sort(sorted.begin(), sorted.end(), [](auto* a, auto* b) {
        return a->name != b->name ? a->name < b->name : a->sha < b->sha;
    });
    for (const auto* v : sorted)
        out.push_back("name=" + v->name + " version=" + v->sha +
                      " pinned_by=" + joined(v->pinned_by) +
                      " used_by=" + joined(v->used_by) +
                      " refs=" + joined(v->refs));
    return out;
}

std::map<std::string, std::pair<std::string, std::string>> app_summaries() {
    std::map<std::string, std::pair<std::string, std::string>> out;
    auto space = space_or_null();
    if (!space)
        return out;
    inventory inv;
    take_inventory(space, inv);
    for (const auto& [app, pinned] : inv.apps) {
        std::vector<std::string> direct;
        // everything it reaches, to find a name it gets at two versions: name, NUL,
        // sha, so a name's versions come out of the set together. In scratch sets,
        // since this grows with the libraries - TODO 597
        scratch::set<std::string> reached;
        scratch::set<uint64_t> seen;
        std::vector<graph::node_id> todo;
        for (const auto& [name, id] : pinned) {
            auto v = inv.versions.find(id);
            if (v == inv.versions.end())
                continue;
            direct.push_back(short_of(v->second));
            todo.push_back(id);
        }
        while (!todo.empty()) {
            auto id = todo.back();
            todo.pop_back();
            if (!seen.insert(id))
                continue;
            auto v = inv.versions.find(id);
            if (v == inv.versions.end())
                continue;
            reached.insert(v->second.name + '\0' + v->second.sha.substr(0, 12));
            for (auto dep : v->second.deps)
                todo.push_back(dep);
        }
        std::string diamond, name, line;
        size_t versions = 0;
        auto end_name = [&]() {
            if (versions > 1)
                diamond += (diamond.empty() ? "" : ",") + line;
        };
        for (const auto& entry : reached) {
            auto nul = entry.find('\0');
            auto this_name = entry.substr(0, nul);
            if (this_name != name || versions == 0) {
                end_name();
                name = this_name;
                line = name;
                versions = 0;
            }
            line += "@" + entry.substr(nul + 1);
            ++versions;
        }
        end_name();
        if (!direct.empty() || !diamond.empty())
            out[app] = {direct.empty() ? std::string("-") : joined(direct), diamond};
    }
    return out;
}

std::vector<std::string> collect(std::set<std::string>& names_left) {
    std::vector<std::string> removed;
    names_left.clear();
    auto space = space_or_null();
    if (!space)
        return removed;
    release_pinsets(space);
    inventory inv;
    take_inventory(space, inv);
    // held: what any kept pin set links to - an app's current one and the one before,
    // which a call may still be resolving through - and everything those reach
    scratch::set<uint64_t> held;            // TODO 597
    std::vector<graph::node_id> todo = inv.pinned_anywhere;
    while (!todo.empty()) {
        auto id = todo.back();
        todo.pop_back();
        if (!held.insert(id))
            continue;
        auto v = inv.versions.find(id);
        if (v != inv.versions.end())
            for (auto dep : v->second.deps)
                todo.push_back(dep);
    }
    std::map<std::string, size_t> kept;
    for (const auto& [id, v] : inv.versions) {
        if (held.count(id)) {
            ++kept[v.name];
            continue;
        }
        // RM takes every edge naming it, the refs included, and the sweep takes
        // the content nothing else holds
        graph::batch b(space);
        std::string err;
        if (!b.remove(version_path(v.name, v.sha), true, err) || !b.commit(err)) {
            ++kept[v.name];
            continue;
        }
        removed.push_back(short_of(v));
    }
    // a package with no versions left goes altogether, and its name is free again
    auto acc = view_of(space);
    graph::node packages;
    if (graph::resolve(acc, "/packages", packages)) {
        std::vector<graph::edge> names;
        graph::children_of(acc, packages.id, names);
        for (const auto& p : names) {
            if (kept[p.name] > 0) {
                names_left.insert(p.name);
                continue;
            }
            graph::batch b(space);
            std::string err;
            if (!b.remove(package_path(p.name), true, err) || !b.commit(err))
                names_left.insert(p.name);
        }
    }
    return removed;
}

}
