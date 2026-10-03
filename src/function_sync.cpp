#include "function_sync.h"

#include "configuration.h"
#include "git_repos.h"
#include "fs_api.h"
#include "local_fs.h"
#include "staged.h"
#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"
#include "http_api.h"
#include "repo_package.h"
#include "index_sink.h"
#include "keys.h"
#include "abstract_shard.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstring>
#include <functional>
#include <set>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <mutex>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
extern char** environ;

#include <poll.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace lfs = barch::localfs;

namespace {

struct checkout_file {
    std::string space; // empty is the default space
    std::string name;  // folded function stem, or the key including its extension
    std::string source;
    std::string path;
    bool luau{false};
};

std::mutex mu;
std::condition_variable cv;
std::atomic<bool> running{false};
std::thread worker;

/*
 * What is known about one repository between syncs. `state` is what FUNCTIONS
 * STATUS reports and is the thing that makes an asynchronous first fetch visible
 * rather than mysterious: a client that gets "no such command" for a function the
 * checkout provides can be told the repository is still pending.
 */
struct repo_state {
    std::string last_ok;
    std::string last_err;
    std::string last_stamp;
    std::string state{"pending"};
    std::chrono::steady_clock::time_point due{};
    bool due_set{false};
    /** package.luau: "applied", "failed", or empty when there is none - TODO 582 */
    std::string package;
    /** what the hooks last did, as FUNCTIONS STATUS shows it */
    std::string hooks;
    /** the after hook has run in this process, which it does once even with no new commit */
    bool after_ran{false};
    /** the interval `due` was worked out with, so a changed one starts over - TODO 584 */
    uint64_t ms{0};
    /** what package.luau's depends last did - TODO 585 */
    std::string depends;
};

std::mutex state_mu;
heap::string_map<repo_state> states;
/** which repository owns a key space, so one cannot swap away another's work */
heap::string_map<std::string> managed_by;
heap::string_map<heap::string_set> managed_data;
/** repositories asked for by name, and the "everything" flag */
heap::string_set kick_names;
std::atomic<bool> kick_all{false};
/**
 * read the repository list again, without asking any of them to run: a
 * repository was set up, changed or removed - TODO 584
 */
std::atomic<bool> kick_rescan{false};

repo_state& state_of(const std::string& name) {
    return states[name];
}

std::string fold_name(std::string s) {
    for (auto& ch : s)
        ch = (char) toupper((unsigned char) ch);
    return s;
}

bool hidden_name(const std::string& name) {
    return !name.empty() && name[0] == '.';
}

bool is_luau_name(const std::string& name) {
    return name.size() >= 6 && name.compare(name.size() - 5, 5, ".luau") == 0
        && !hidden_name(name);
}

std::string stem_of(const std::string& name) {
    if (!is_luau_name(name))
        return {};
    return fold_name(name.substr(0, name.size() - 5));
}

/*
 * A deploy key setting is a *reference*: a path, `file:/path`, or `env:VAR` naming
 * one. It resolves to a path here because that is what `ssh -i` wants; the key
 * material is never read into barch and never stored in a key.
 *
 * `env:` used to be refused outright with "must be a path to a key file", which was
 * the right requirement attached to the wrong answer - the variable holds the path.
 */
std::string key_file_of(const std::string& raw, std::string& err) {
    if (raw.compare(0, 5, "file:") == 0)
        return raw.substr(5);
    if (raw.compare(0, 4, "env:") == 0) {
        const char* v = std::getenv(raw.substr(4).c_str());
        if (!v || !*v) {
            err = "git ssh key env " + raw.substr(4) + " is not set";
            return {};
        }
        return v;
    }
    return raw;
}

/**
 * Flatten a message onto one line.
 *
 * A RESP error is terminated by CRLF, so an error carrying one of its own ends the
 * reply early and everything after it is read as the next reply - the client then
 * waits for an answer that already went past it, gives up, reconnects and sends the
 * command again. git's stderr is multi line and keeps its trailing newline, which is
 * exactly how a failing FUNCTIONS SYNC turned into the same sync running over and
 * over on different threads. Anything that can reach push_error goes through here.
 */
std::string one_line(std::string text) {
    for (auto& c : text) {
        if (c == '\n' || c == '\r' || c == '\t')
            c = ' ';
    }
    std::string out;
    out.reserve(text.size());
    bool space = false;
    for (char c : text) {
        if (c == ' ') {
            space = !out.empty();
            continue;
        }
        if (space)
            out.push_back(' ');
        space = false;
        out.push_back(c);
    }
    return out;
}

/** the absolute path of a program, resolved here so the child never searches PATH */
std::string program_path(const std::string& name) {
    if (name.find('/') != std::string::npos)
        return name;
    const char* path = std::getenv("PATH");
    if (!path)
        path = "/usr/local/bin:/usr/bin:/bin";
    std::string all = path;
    size_t at = 0;
    while (at <= all.size()) {
        auto end = all.find(':', at);
        if (end == std::string::npos)
            end = all.size();
        std::string dir = all.substr(at, end - at);
        if (!dir.empty()) {
            std::string full = dir + "/" + name;
            if (::access(full.c_str(), X_OK) == 0)
                return full;
        }
        at = end + 1;
    }
    return {};
}

/**
 * Run a program and collect what it said.
 *
 * Everything the child needs - the resolved program path, the argument vector and
 * the whole environment - is built before the fork, and the child then does nothing
 * but dup2, close and execve. That is not tidiness. Between fork and exec the child
 * may only call async-signal-safe functions, because it holds copies of every lock
 * the other threads happened to be holding: the old shape called setenv and execvp
 * in the child, both of which allocate, and a git command run while another thread
 * was inside malloc would hang forever in the child with the parent blocked in
 * waitpid. It showed up as a FUNCTIONS SYNC that never answered and no git process
 * anywhere, since the child died before it ever became git.
 */
int run_cmd(const std::vector<std::string>& args,
            const std::vector<std::pair<std::string, std::string>>& extra_env,
            std::string& out, std::string& err) {
    if (args.empty())
        return -1;
    std::string exe = program_path(args[0]);
    if (exe.empty()) {
        err = args[0] + " is not on the path";
        return -1;
    }

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& a : args)
        argv.push_back(const_cast<char*>(a.c_str()));
    argv.push_back(nullptr);

    std::vector<std::string> env_strings;
    for (char** e = environ; e && *e; ++e) {
        std::string entry = *e;
        bool replaced = false;
        for (const auto& [k, v] : extra_env)
            replaced = replaced || (entry.compare(0, k.size(), k) == 0
                                    && entry.size() > k.size() && entry[k.size()] == '=');
        if (!replaced)
            env_strings.push_back(std::move(entry));
    }
    for (const auto& [k, v] : extra_env)
        env_strings.push_back(k + "=" + v);
    std::vector<char*> envp;
    envp.reserve(env_strings.size() + 1);
    for (auto& e : env_strings)
        envp.push_back(const_cast<char*>(e.c_str()));
    envp.push_back(nullptr);

    int outp[2], errp[2];
    if (pipe(outp) != 0)
        return -1;
    if (pipe(errp) != 0) {
        close(outp[0]); close(outp[1]);
        return -1;
    }
    pid_t pid = fork();
    if (pid < 0) {
        close(outp[0]); close(outp[1]);
        close(errp[0]); close(errp[1]);
        return -1;
    }
    if (pid == 0) {
        dup2(outp[1], STDOUT_FILENO);
        dup2(errp[1], STDERR_FILENO);
        close(outp[0]); close(outp[1]);
        close(errp[0]); close(errp[1]);
        execve(exe.c_str(), argv.data(), envp.data());
        _exit(127);
    }
    close(outp[1]);
    close(errp[1]);
    /*
     * Both pipes are drained together. Reading one to the end first deadlocks as
     * soon as the other fills its buffer, which git does happily on a big fetch.
     */
    char buf[4096];
    int fds[2] = {outp[0], errp[0]};
    std::string* into[2] = {&out, &err};
    bool open_fd[2] = {true, true};
    while (open_fd[0] || open_fd[1]) {
        struct pollfd p[2];
        int n = 0;
        int which[2] = {-1, -1};
        for (int i = 0; i < 2; ++i) {
            if (!open_fd[i])
                continue;
            p[n].fd = fds[i];
            p[n].events = POLLIN;
            p[n].revents = 0;
            which[n] = i;
            ++n;
        }
        if (::poll(p, (nfds_t) n, -1) < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        for (int j = 0; j < n; ++j) {
            if (!(p[j].revents & (POLLIN | POLLHUP | POLLERR)))
                continue;
            ssize_t got = read(p[j].fd, buf, sizeof buf);
            if (got > 0)
                into[which[j]]->append(buf, (size_t) got);
            else
                open_fd[which[j]] = false;
        }
    }
    close(outp[0]);
    close(errp[0]);
    int st = 0;
    waitpid(pid, &st, 0);
    if (WIFEXITED(st))
        return WEXITSTATUS(st);
    return -1;
}

bool git_head(const std::string& dir, std::string& sha) {
    std::string out, err;
    int rc = run_cmd({"git", "-C", dir, "rev-parse", "HEAD"}, {}, out, err);
    if (rc != 0)
        return false;
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r'))
        out.pop_back();
    sha = out;
    return !sha.empty();
}

/** mkdir -p, so a clone into <base>/<name> does not need the base to exist */
bool make_dirs(const std::string& path) {
    std::string at;
    for (size_t i = 0; i <= path.size(); ++i) {
        if (i == path.size() || path[i] == '/') {
            if (!at.empty() && !lfs::is_dir(at) && ::mkdir(at.c_str(), 0755) != 0 && errno != EEXIST)
                return false;
        }
        if (i < path.size())
            at.push_back(path[i]);
    }
    return true;
}

std::string parent_of(const std::string& path) {
    auto at = path.rfind('/');
    if (at == std::string::npos || at == 0)
        return {};
    return path.substr(0, at);
}

/**
 * Bring the checkout to where the repository says it should be.
 *
 * A missing checkout is cloned when there is a url, which is new - before this
 * there was no url anywhere and a missing `.git` simply meant "leave it alone",
 * so somebody set the checkout up by hand. Without a url that is still what
 * happens, which is what keeps the old `functions_dir` behaviour intact.
 */
/*
 * `before_move` is package.luau's before hook - TODO 582. It is called with the
 * commit the checkout is about to be reset to, after the fetch and before the
 * reset, and only for a checkout that was already there; anything it returns stops
 * the sync with the checkout where it was.
 */
std::string git_checkout(const barch::repo_conf& r, const std::string& pin, std::string& err,
                         const std::function<std::string(const std::string&)>& before_move = {}) {
    std::string branch = r.branch.empty() ? "main" : r.branch;
    std::vector<std::pair<std::string, std::string>> env;
    if (!r.ssh_key.empty()) {
        auto keyfile = key_file_of(r.ssh_key, err);
        if (keyfile.empty())
            return err.empty() ? "no git ssh key" : err;
        env.emplace_back("GIT_SSH_COMMAND",
                         "ssh -i " + keyfile +
                         " -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new");
    }
    std::string out, e2;
    const bool existed = lfs::is_dir(r.dir + "/.git");
    if (!existed) {
        if (r.url.empty())
            return {};                       // somebody else's checkout, as before
        auto parent = parent_of(r.dir);
        if (!parent.empty() && !make_dirs(parent)) {
            err = "cannot create " + parent;
            return err;
        }
        int rc = run_cmd({"git", "clone", "--quiet", "--branch", branch, r.url, r.dir},
                         env, out, e2);
        if (rc != 0) {
            err = one_line(e2.empty() ? ("git clone of " + r.url + " failed") : e2);
            return err;
        }
    }
    if (!r.pull && pin.empty())
        return {};
    std::string spec = pin.empty() ? branch : pin;
    out.clear(); e2.clear();
    int rc = run_cmd({"git", "-C", r.dir, "fetch", "--quiet", "origin", spec}, env, out, e2);
    if (rc != 0 && !pin.empty() && pin != branch) {
        // a pinned rev is often not a ref origin will serve by name
        out.clear(); e2.clear();
        rc = run_cmd({"git", "-C", r.dir, "fetch", "--quiet", "origin", branch}, env, out, e2);
    }
    if (rc != 0) {
        err = one_line(e2.empty() ? "git fetch failed" : e2);
        return err;
    }
    std::string target = pin.empty() ? "FETCH_HEAD" : pin;
    if (before_move && existed) {
        out.clear(); e2.clear();
        rc = run_cmd({"git", "-C", r.dir, "rev-parse", "--verify", "--quiet", target + "^{commit}"},
                     env, out, e2);
        while (!out.empty() && (out.back() == '\n' || out.back() == '\r'))
            out.pop_back();
        if (rc != 0 || out.empty()) {
            err = "no commit " + target;
            return err;
        }
        auto stop = before_move(out);
        if (!stop.empty()) {
            err = stop;
            return err;
        }
    }
    out.clear(); e2.clear();
    rc = run_cmd({"git", "-C", r.dir, "reset", "--hard", "--quiet", target}, env, out, e2);
    if (rc != 0) {
        err = one_line(e2.empty() ? "git reset failed" : e2);
        return err;
    }
    return {};
}

std::string key_in(const std::string& prefix, const std::string& name) {
    return prefix.empty() ? name : prefix + ":" + name;
}

bool add_file(const std::string& space, const std::string& prefix, const std::string& path,
              const std::string& name, std::vector<checkout_file>& files, std::string& err) {
    checkout_file f;
    f.space = space;
    f.path = path;
    f.luau = is_luau_name(name);
    if (f.luau) {
        f.name = stem_of(name);
        if (f.name.empty())
            return true;
    } else {
        f.name = key_in(prefix, name);
    }
    if (!lfs::read_file(path, f.source)) {
        err = "could not read " + path;
        return false;
    }
    files.push_back(std::move(f));
    return true;
}

/*
 * `top` is the real path of where the walk began: a link is only followed to a
 * file inside it, never to a directory, so a checkout can't reach outside itself
 * or send the walk round in a circle - TODO 541.
 */
bool scan_tree(const std::string& top, const std::string& dir, const std::string& space,
               const std::string& prefix, std::vector<checkout_file>& files, std::string& err) {
    for (const auto& name : lfs::list_dir(dir)) {
        if (hidden_name(name))
            continue;
        std::string path = dir + "/" + name;
        const auto kind = lfs::walk_entry(top, path);
        if (kind == lfs::entry::file) {
            if (!add_file(space, prefix, path, name, files, err))
                return false;
        } else if (kind == lfs::entry::dir) {
            if (!scan_tree(top, path, space, key_in(prefix, name), files, err))
                return false;
        }
    }
    return true;
}

bool scan_tree(const std::string& dir, const std::string& space, const std::string& prefix,
               std::vector<checkout_file>& files, std::string& err) {
    return scan_tree(lfs::real_path(dir), dir, space, prefix, files, err);
}

bool scan_checkout(const std::string& root, std::vector<checkout_file>& files, std::string& err) {
    const std::string top = lfs::real_path(root);
    for (const auto& name : lfs::list_dir(root)) {
        if (hidden_name(name))
            continue;
        std::string path = root + "/" + name;
        const auto kind = lfs::walk_entry(top, path);
        if (kind == lfs::entry::file) {
            if (name == barch::package::file_name)
                continue;               // the installer, not something to install - TODO 582
            if (!add_file({}, {}, path, name, files, err))
                return false;
        } else if (kind == lfs::entry::dir) {
            if (name == "configuration")
                continue;
            if (!barch::check_ks_name(name)) {
                barch::err({"function sync skipping folder, not a space name", name});
                continue;
            }
            if (!scan_tree(top, path, name, {}, files, err))
                return false;
        }
    }
    return true;
}

/** the walk LOADKEYS borrows - see barch::scan_directory */
bool scan_as_keys(const std::string& dir, const std::string& prefix,
                  std::vector<barch::import_file>& out, std::string& err) {
    std::vector<checkout_file> files;
    if (!scan_tree(dir, {}, prefix, files, err))
        return false;
    out.reserve(out.size() + files.size());
    for (auto& f : files) {
        barch::import_file one;
        one.name = std::move(f.name);
        one.source = std::move(f.source);
        one.path = std::move(f.path);
        one.luau = f.luau;
        out.push_back(std::move(one));
    }
    return true;
}

barch::key_space_ptr dest_of(const std::string& space) {
    if (space.empty())
        return barch::get_keyspace("");
    return barch::get_keyspace(space);
}

bool set_plain(const barch::key_space_ptr& space, const std::string& key,
               const std::string& value, std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    return acc.set(key, value, err);
}

bool fill_temp(const std::vector<const checkout_file*>& luau,
               const std::vector<const checkout_file*>& data,
               barch::key_space_ptr& tmp, std::string& err) {
    tmp = barch::key_space::make_scratch();
    std::vector<const checkout_file*> left = luau;
    while (!left.empty()) {
        std::vector<const checkout_file*> next;
        std::string last;
        size_t progress = 0;
        for (auto* f : left) {
            std::string e;
            if (barch::functions::install(tmp, f->name, f->source, e)) {
                ++progress;
            } else {
                last = f->path + ": " + e;
                next.push_back(f);
            }
        }
        if (progress == 0) {
            err = last.empty() ? "function sync could not store any file" : last;
            tmp.reset();
            return false;
        }
        left.swap(next);
    }
    for (auto* f : data) {
        std::string e;
        if (!set_plain(tmp, f->name, f->source, e)) {
            err = f->path + ": " + (e.empty() ? "could not store key" : e);
            tmp.reset();
            return false;
        }
    }
    return true;
}

/**
 * Swap a space's functions and keys for what the checkout holds.
 *
 * One staged write - TODO 255 - which is where the snapshot, the apply, the putting
 * back and the retry that installs a module before the function requiring it all
 * live now. This is left with the part that is actually about a checkout: what
 * should be there, and what was there and no longer should be.
 *
 * `managed_data` is why the keys half is not simply "everything that is there now".
 * A key someone SET by hand has no checkout to be missing from, so only keys a
 * previous sync wrote are candidates for removal. Functions are different - the
 * space's whole function list is the checkout's business.
 */
bool apply_dest(const barch::key_space_ptr& tmp, const barch::key_space_ptr& dest,
                const std::vector<const checkout_file*>& data, const std::string& space,
                std::string& err) {
    barch::staged batch(dest);

    auto want = barch::functions::names(tmp);
    heap::string_set want_fn;
    heap::vector<std::string> changed;
    for (const auto& n : want) {
        std::string src;
        if (!barch::functions::source_in(tmp, n, src)) {
            err = "temp function " + n + " had no source";
            return false;
        }
        want_fn.insert(n);
        std::string was;
        if (!barch::functions::source_in(dest, n, was) || was != src)
            changed.push_back(n);
        batch.set_function(n, src);
    }
    for (const auto& n : barch::functions::names(dest)) {
        if (want_fn.find(n) == want_fn.end())
            batch.remove_function(n);
    }

    heap::string_set want_keys;
    for (auto* f : data) {
        want_keys.insert(f->name);
        batch.set(f->name, f->source);
    }
    auto mit = managed_data.find(space);
    if (mit != managed_data.end()) {
        for (const auto& k : mit->second) {
            if (want_keys.find(k) == want_keys.end())
                batch.remove(k);
        }
    }

    if (!batch.commit(err))
        return false;

    /*
     * Published, the way SETF and LOADKEYS RELOAD do it, so a running HTTP server
     * rebuilds the handlers it compiled at START. Without it a synced route kept
     * answering with the code from before the sync - found with TODO 582. Only what
     * changed: a poll that finds nothing new must not make every route recompile.
     */
    for (const auto& n : changed)
        barch::functions::publish_compiled(barch::functions::compiled_key(dest->canonical(), n));

    if (want_keys.empty())
        managed_data.erase(space);
    else
        managed_data[space] = std::move(want_keys);
    return true;
}

/*
 * Scanned files into their spaces, all or nothing per space - where the folder walk
 * and a package's `load` list both end. `seen` gets every space written.
 */
std::string import_keys(const barch::repo_conf& r, std::vector<checkout_file>& files,
                        heap::string_set& seen) {
    std::string err;
    heap::string_map<std::vector<checkout_file>> by_space;
    for (auto& f : files)
        by_space[f.space].push_back(std::move(f));

    /*
     * The folder-per-space mapping is not known until the checkout has been
     * scanned, so this is where that half of the overlap check has to happen -
     * the declared `space` half is caught earlier, in refuse_overlap.
     */
    for (const auto& [space, group] : by_space) {
        (void) group;
        auto owner = managed_by.find(space);
        if (owner != managed_by.end() && owner->second != r.name)
            return "key space " + (space.empty() ? std::string("(default)") : space)
                 + " is owned by repository " + owner->second;
    }

    for (auto& [space, group] : by_space) {
        seen.insert(space);
        std::vector<const checkout_file*> luau;
        std::vector<const checkout_file*> data;
        heap::string_set fn_names;
        heap::string_set key_names;
        for (const auto& f : group) {
            if (f.luau) {
                if (!fn_names.insert(f.name).second)
                    return "duplicate function " + f.name + " in " + (space.empty() ? "default" : space);
                luau.push_back(&f);
            } else {
                if (!key_names.insert(f.name).second)
                    return "duplicate key " + f.name + " in " + (space.empty() ? "default" : space);
                data.push_back(&f);
            }
        }
        barch::key_space_ptr tmp;
        if (!fill_temp(luau, data, tmp, err))
            return err;
        auto dest = dest_of(space);
        if (!apply_dest(tmp, dest, data, space, err)) {
            tmp.reset();
            return err;
        }
        tmp.reset();
        managed_by[space] = r.name;
    }
    return {};
}

/** the checkout imported the way a repository without a package.luau is */
std::string import_checkout(const barch::repo_conf& r) {
    std::string err;
    /*
     * `as = fs` puts the checkout in the chunked file store rather than turning it
     * into keys and functions - a repository of images, fonts or a built web
     * application is not a directory of key values. The import is LOADFS, so it is
     * the same code path a client gets, and the stale sweep afterwards is what
     * makes it a *sync*: the checkout is the truth, so a file deleted upstream
     * leaves the store, the way a deleted .luau is removed. See TODO 253.
     */
    if (r.as == "fs") {
        auto dest = dest_of(r.space);
        auto owner = managed_by.find(r.space);
        if (owner != managed_by.end() && owner->second != r.name)
            return "key space " + (r.space.empty() ? std::string("(default)") : r.space)
                 + " is owned by repository " + owner->second;
        std::vector<std::string> reply;
        std::vector<std::string> imported;
        auto failed = barch::load_fs_directory(r.dir, r.fs_root, 65536, dest, reply,
                                               true, &imported);
        if (!failed.empty())
            return failed;
        auto gone = barch::drop_fs_missing(dest, r.fs_root, imported);
        managed_by[r.space] = r.name;
        std::string line;
        for (const auto& part : reply)
            line += (line.empty() ? "" : " ") + part;
        barch::log({"function sync", r.name, "imported as files", line,
                    "removed", (uint64_t) gone});
        return {};
    }

    std::vector<checkout_file> files;
    if (!r.space.empty()) {
        if (!scan_tree(r.dir, r.space, {}, files, err))
            return err;
    } else if (!scan_checkout(r.dir, files, err)) {
        return err;
    }

    // the package is the installer, not part of what it installs - TODO 582
    const std::string self = r.dir + "/" + barch::package::file_name;
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](const checkout_file& f) { return f.path == self; }),
                files.end());

    heap::string_set seen;
    err = import_keys(r, files, seen);
    if (!err.empty())
        return err;
    heap::vector<std::string> gone;
    for (const auto& [space, owner] : managed_by) {
        if (owner == r.name && seen.find(space) == seen.end())
            gone.push_back(space);
    }
    for (const auto& space : gone) {
        barch::key_space_ptr tmp = barch::key_space::make_scratch();
        auto dest = dest_of(space);
        if (!apply_dest(tmp, dest, {}, space, err)) {
            tmp.reset();
            return err;
        }
        tmp.reset();
        managed_by.erase(space);
    }
    return {};
}

/*
 * package.luau - TODO 582.
 *
 * What a package wrote to the configuration space is listed under the repository's
 * own keys, one level deeper than read_repos looks, so the next sync can take back a
 * setting the package stopped listing. The rest is per process, like managed_by:
 * an HTTP server does not outlive a restart, and neither does the record of it.
 */
std::string applied_key(const std::string& repo) {
    return "git/repositories/" + repo + "/package/applied";
}

std::string repo_key(const std::string& repo, const std::string& setting) {
    return "git/repositories/" + repo + "/" + setting;
}

/*
 * The repositories this thread is syncing, outermost first - TODO 585. A package's
 * dependencies sync inside its own sync, so this is how deep that has gone and
 * which repositories are already being synced further up.
 */
thread_local std::vector<std::string> sync_stack;
const size_t max_depends_depth = 8;

std::string run_repo(const barch::repo_conf& r, const std::string& pin);

/** the space a package last started HTTP in */
heap::string_map<std::string> package_http;
/** the spaces a package's `load` last wrote keys into */
heap::string_map<heap::string_set> package_keys;
/**
 * set while a hook runs. A hook is a command function, so it can send FUNCTIONS
 * SYNC, which would wait on the sync that is waiting on the hook
 */
std::atomic<bool> hook_running{false};

std::string space_label(const std::string& space) {
    return space.empty() ? std::string("(default)") : space;
}

using read_state = barch::foreign::store_access::read_state;

std::string apply_settings(const barch::repo_conf& r, const barch::package::spec& p) {
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return "no configuration space";
    auto acc = barch::functions::store_for_owner(conf);
    if (!acc.get)
        return "the configuration space cannot be read";

    for (const auto& s : p.settings) {
        auto owner = managed_by.find(s.space);
        if (owner != managed_by.end() && owner->second != r.name)
            return "spaces." + s.space + ": key space " + s.space
                 + " is owned by repository " + owner->second;
        // a space keeps the shard count it was made with: written anyway, the key
        // would only stop the server loading it at the next start
        if (s.name == "shards" && barch::keyspace_exists(s.space)) {
            auto ks = barch::get_keyspace(s.space);
            auto have = ks ? std::to_string(ks->get_shard_count()) : s.value;
            if (have != s.value)
                return "spaces." + s.space + ".shards is " + s.value + ", but " + s.space
                     + " already exists with " + have
                     + " shards, and a shard count cannot change once a space is made";
        }
    }

    std::string old_list;
    if (acc.get(applied_key(r.name), old_list) != read_state::present)
        old_list.clear();
    std::set<std::string> now;
    std::vector<std::string> later;
    barch::staged batch(conf);
    for (const auto& s : p.settings) {
        auto key = s.space + "." + s.name;
        now.insert(key);
        std::string have;
        if (acc.get(key, have) == read_state::present && have == s.value)
            continue;
        batch.set(key, s.value);
        // a space reads its settings as it opens, so one already there goes on
        // with the old value until the next start
        if (barch::keyspace_exists(s.space))
            later.push_back(key);
    }
    std::istringstream in(old_list);
    for (std::string key; std::getline(in, key);) {
        if (key.empty() || now.count(key))
            continue;
        if (key.size() > 7 && key.compare(key.size() - 7, 7, ".shards") == 0)
            continue;
        std::string have;
        if (acc.get(key, have) == read_state::present)
            batch.remove(key);
    }
    std::string listed;
    for (const auto& key : now)
        listed += key + "\n";
    if (listed != old_list) {
        if (listed.empty())
            batch.remove(applied_key(r.name));
        else
            batch.set(applied_key(r.name), listed);
    }
    if (!batch.empty()) {
        std::string err;
        if (!batch.commit(err))
            return "could not write the settings: " + err;
        for (const auto& key : later)
            barch::log({"function sync", r.name, "set", key,
                        "- the space is open, so it applies at the next start"});
    }
    // a listed space exists afterwards, with its settings in place before it opens.
    // Nothing else makes one: require and barch.space only look a space up
    for (const auto& space : p.spaces) {
        if (!barch::get_keyspace(space))
            return "could not open key space " + space;
    }
    return {};
}

std::string import_listed(const barch::repo_conf& r, const barch::package::spec& p) {
    const std::string top = lfs::real_path(r.dir);
    const std::string self = r.dir + "/" + barch::package::file_name;
    auto folder = [&](const barch::package::load_entry& l) {
        return l.path == "." ? r.dir : r.dir + "/" + l.path;
    };
    std::vector<checkout_file> files;
    heap::string_set fs_spaces;
    std::string err;
    for (const auto& l : p.loads) {
        auto path = folder(l);
        auto real = lfs::real_path(path);
        if (!lfs::is_dir(path) || real.empty())
            return "load: " + l.path + " is not a folder in the repository";
        if (real != top && real.compare(0, top.size() + 1, top + "/") != 0)
            return "load: " + l.path + " leaves the repository";
        auto owner = managed_by.find(l.space);
        if (owner != managed_by.end() && owner->second != r.name)
            return "load: key space " + space_label(l.space) + " is owned by repository "
                 + owner->second;
        if (l.as == "fs")
            fs_spaces.insert(l.space);
        else if (!scan_tree(path, l.space, {}, files, err))
            return err;
    }
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](const checkout_file& f) { return f.path == self; }),
                files.end());

    heap::string_set seen;
    err = import_keys(r, files, seen);
    if (!err.empty())
        return err;

    for (const auto& l : p.loads) {
        if (l.as != "fs")
            continue;
        auto dest = dest_of(l.space);
        std::vector<std::string> reply;
        std::vector<std::string> imported;
        auto failed = barch::load_fs_directory(folder(l), l.fs_root, 65536, dest, reply,
                                               true, &imported);
        if (!failed.empty())
            return "load: " + l.path + ": " + failed;
        auto gone = barch::drop_fs_missing(dest, l.fs_root, imported);
        managed_by[l.space] = r.name;
        std::string line;
        for (const auto& part : reply)
            line += (line.empty() ? "" : " ") + part;
        barch::log({"function sync", r.name, l.path, "imported as files in",
                    space_label(l.space), line, "removed", (uint64_t) gone});
    }

    // a space the last package loaded keys into and this one does not is emptied
    // of them, the way a folder deleted from the checkout is
    auto& last = package_keys[r.name];
    for (const auto& space : last) {
        if (seen.count(space))
            continue;
        barch::key_space_ptr tmp = barch::key_space::make_scratch();
        if (!apply_dest(tmp, dest_of(space), {}, space, err))
            return err;
        if (!fs_spaces.count(space))
            managed_by.erase(space);
    }
    last = seen;
    return {};
}

std::string apply_http(const barch::repo_conf& r, const barch::package::spec& p) {
    auto was = package_http.find(r.name);
    if (was != package_http.end() && (!p.http || was->second != p.http->space)) {
        barch::stop_http_server(dest_of(was->second)->canonical());
        package_http.erase(was);
    }
    if (!p.http)
        return {};
    bool started = false;
    auto err = barch::ensure_http_server(dest_of(p.http->space), p.http->key, p.http->port,
                                         p.http->bind, started);
    if (!err.empty())
        return "http: " + err;
    package_http[r.name] = p.http->space;
    if (started)
        barch::log({"function sync", r.name, "serving HTTP in", space_label(p.http->space)});
    return {};
}

/** every key under a repository: its settings, and a package's records below them */
heap::vector<std::string> repo_keys(barch::foreign::store_access& acc, const std::string& repo) {
    std::string lo = "git/repositories/" + repo + "/";
    std::string hi = lo;
    hi.back() = (char) ('/' + 1);
    heap::vector<std::string> keys;
    if (acc.range)
        acc.range(lo, hi, 100000, keys);
    return keys;
}

void remove_repo_tree(const std::string& name, size_t depth);

/**
 * A package that was there and is gone takes its settings, its server and the
 * dependencies it added with it. `depth` only bounds a chain that removes itself.
 */
void drop_package(const barch::repo_conf& r, size_t depth = 0) {
    package_keys.erase(r.name);
    barch::package::spec none;
    (void) apply_http(r, none);
    auto err = apply_settings(r, none);
    if (!err.empty())
        barch::err({"function sync", r.name, "package.luau removed, but", err});
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return;
    auto acc = barch::functions::store_for_owner(conf);
    std::string list;
    if (acc.get(repo_key(r.name, "package/depends"), list) != read_state::present)
        return;
    std::istringstream in(list);
    for (std::string child; std::getline(in, child);) {
        std::string owner;
        if (!child.empty() &&
            acc.get(repo_key(child, "package/added_by"), owner) == read_state::present &&
            owner == r.name)
            remove_repo_tree(child, depth + 1);
    }
    barch::staged batch(conf);
    batch.remove(repo_key(r.name, "package/depends"));
    std::string e2;
    if (!batch.commit(e2))
        barch::err({"function sync", r.name, "could not forget its dependencies:", e2});
}

/** a repository a package added, gone again with everything that came with it */
void remove_repo_tree(const std::string& name, size_t depth) {
    if (depth > max_depends_depth)
        return;
    for (const auto& r : barch::read_repos()) {
        if (r.name == name) {
            drop_package(r, depth);
            break;
        }
    }
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return;
    auto acc = barch::functions::store_for_owner(conf);
    barch::staged batch(conf);
    for (const auto& k : repo_keys(acc, name))
        batch.remove(k);
    std::string err;
    if (!batch.empty() && !batch.commit(err)) {
        barch::err({"function sync", "could not remove repository", name, err});
        return;
    }
    barch::log({"function sync", "removed repository", name, "- its package no longer names it"});
}

void note_depends(const std::string& repo, const std::string& text) {
    std::lock_guard<std::mutex> g(state_mu);
    state_of(repo).depends = text;
}

/*
 * package.luau's `depends` - TODO 585. Each one becomes an ordinary repository under
 * git/repositories/, marked as added by this package, and is synced here, before
 * the package's own folders, so the package's code and hooks can rely on it. Its
 * own package.luau can name more, which is what makes it transitive.
 *
 * Only with a `user`: a dependency is the server cloning a url the repository
 * chose, which is the repository acting, the way a hook is. The dependency runs as
 * that same user, which gives it nothing the package's own hooks didn't have.
 *
 * Keys are written only when they change, so a sync that finds nothing new does
 * not wake the repository watcher (TODO 584) into another rescan.
 */
std::string apply_depends(const barch::repo_conf& r, const barch::package::spec& p) {
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return "no configuration space";
    auto acc = barch::functions::store_for_owner(conf);
    std::string old_list;
    if (acc.get(repo_key(r.name, "package/depends"), old_list) != read_state::present)
        old_list.clear();
    if (old_list.empty() && p.depends.empty()) {
        note_depends(r.name, {});
        return {};
    }
    if (!p.depends.empty() && r.user.empty()) {
        note_depends(r.name, "skipped:no_user");
        return {};
    }
    if (!p.depends.empty() && sync_stack.size() >= max_depends_depth)
        return "depends: more than " + std::to_string(max_depends_depth) + " repositories deep";

    barch::staged batch(conf);
    std::vector<std::string> ours;
    std::set<std::string> kept;
    std::string listed;
    for (const auto& dep : p.depends) {
        std::string url;
        for (const auto& kv : dep.settings) {
            if (kv.first == "url")
                url = kv.second;
        }
        auto had = repo_keys(acc, dep.name);
        std::string owner, have_url;
        const bool mine = acc.get(repo_key(dep.name, "package/added_by"), owner) ==
                          read_state::present && owner == r.name;
        if (!had.empty() && !mine) {
            // somebody else's - the operator's, or another package's. The same one is
            // a dependency already met, and theirs to sync; that is also what ends a
            // cycle, since the repository that started it is always already there
            if (acc.get(repo_key(dep.name, "url"), have_url) != read_state::present ||
                have_url != url)
                return "depends: " + dep.name + " is already a repository, with another url";
            continue;
        }
        listed += dep.name + "\n";
        kept.insert(dep.name);
        ours.push_back(dep.name);
        auto want = dep.settings;
        want.emplace_back("user", r.user);
        // it syncs with the package that names it; asynch off would sync it again at boot
        want.emplace_back("asynch", "on");
        std::set<std::string> named;
        for (const auto& kv : want)
            named.insert(kv.first);
        const auto prefix = "git/repositories/" + dep.name + "/";
        for (const auto& k : had) {
            auto setting = k.substr(prefix.size());
            if (setting.find('/') == std::string::npos && !named.count(setting))
                batch.remove(k);        // what the entry no longer says goes back to the default
        }
        want.emplace_back("package/added_by", r.name);
        for (const auto& kv : want) {
            std::string have;
            if (acc.get(repo_key(dep.name, kv.first), have) == read_state::present &&
                have == kv.second)
                continue;
            batch.set(repo_key(dep.name, kv.first), kv.second);
        }
    }
    if (listed != old_list) {
        if (listed.empty())
            batch.remove(repo_key(r.name, "package/depends"));
        else
            batch.set(repo_key(r.name, "package/depends"), listed);
    }
    std::string err;
    if (!batch.empty() && !batch.commit(err))
        return "depends: " + err;

    // the ones it no longer names, with everything that came with them
    std::istringstream in(old_list);
    for (std::string name; std::getline(in, name);) {
        if (name.empty() || kept.count(name))
            continue;
        std::string owner;
        if (acc.get(repo_key(name, "package/added_by"), owner) == read_state::present &&
            owner == r.name)
            remove_repo_tree(name, sync_stack.size());
    }

    if (!ours.empty()) {
        auto repos = barch::read_repos();
        auto conflicts = barch::refuse_overlap(repos);
        for (const auto& name : ours) {
            if (std::find(sync_stack.begin(), sync_stack.end(), name) != sync_stack.end())
                continue;               // being synced further up already
            const barch::repo_conf* found = nullptr;
            for (const auto& c : repos) {
                if (c.name == name)
                    found = &c;
            }
            if (!found)
                return "depends: " + name + " did not read back";
            if (!found->enabled) {
                std::string why = found->invalid;
                for (const auto& line : conflicts) {
                    if (line.find(name) != std::string::npos)
                        why = line;
                }
                return "depends: " + name + (why.empty() ? std::string(" is disabled") : ": " + why);
            }
            auto failed = run_repo(*found, {});
            if (!failed.empty())
                return "depends: " + name + ": " + failed;
        }
    }
    note_depends(r.name, p.depends.empty() ? std::string() : "ok");
    return {};
}

/*
 * A hook is a command function run through call_as, the way a cron job is: the
 * same rights check CALLF makes, with the space's deadline and slices. It runs as
 * the repository's `user` and never as the owner, so with no user it does not run.
 * `note` gets what FUNCTIONS STATUS says about it.
 */
std::string run_hook(const barch::repo_conf& r, const barch::package::hook& h,
                     const std::string& phase, const std::string& from, const std::string& to,
                     std::string& note) {
    auto said = [&](const std::string& what) {
        note += (note.empty() ? "" : ",") + phase + "=" + what;
    };
    if (r.user.empty()) {
        said("skipped:no_user");
        return {};
    }
    if (!h.space.empty() && !barch::keyspace_exists(h.space)) {
        said("failed");
        return phase + " hook: no key space " + h.space;
    }
    heap::vector<std::string> args{r.name, from, to};
    for (const auto& a : h.args)
        args.push_back(a);
    Variable out;
    std::string err;
    bool ok;
    {
        struct flag {
            flag() { hook_running.store(true); }
            ~flag() { hook_running.store(false); }
        } running_hook;
        ok = barch::functions::call_as(dest_of(h.space), r.user, h.call, args, out, err);
    }
    if (!ok) {
        said("failed");
        return phase + " hook " + h.call + ": " + (err.empty() ? std::string("failed") : err);
    }
    said("ok");
    return {};
}

void note_package(const std::string& repo, const std::string& package, const std::string& hooks) {
    std::lock_guard<std::mutex> g(state_mu);
    auto& st = state_of(repo);
    st.package = package;
    if (!hooks.empty())
        st.hooks = hooks;
    else if (package.empty())
        st.hooks.clear();
}

std::string do_sync_repo(const barch::repo_conf& r, const std::string& pin) {
    if (r.dir.empty())
        return "repository " + r.name + " has no checkout directory";

    std::string want = pin.empty() ? r.commit : pin;
    // where the checkout is now: the hooks run when that changes - TODO 582
    std::string old_head;
    const bool had_checkout = lfs::is_dir(r.dir + "/.git") && git_head(r.dir, old_head);
    std::string hooks;
    auto before_move = [&](const std::string& target) -> std::string {
        if (!had_checkout || target == old_head)
            return {};
        // the package being replaced names the hook, and its functions are the ones
        // in place. One that does not read is skipped rather than blocking every
        // commit after it, the one that would fix it included
        barch::package::spec current;
        bool present = false;
        std::string perr;
        if (!barch::package::read(r.dir, present, current, perr)) {
            barch::err({"function sync", r.name, "no before hook, package.luau does not read:", perr});
            return {};
        }
        if (!present || !current.before)
            return {};
        return run_hook(r, *current.before, "before", old_head, target, hooks);
    };
    {
        std::string err;
        auto failed = git_checkout(r, want, err, before_move);
        if (!failed.empty()) {
            if (!hooks.empty())         // the before hook ran, so there is a package
                note_package(r.name, "failed", hooks);
            return failed;
        }
    }
    if (!lfs::is_dir(r.dir))
        return r.dir + " is not a directory";

    barch::package::spec pkg;
    bool has_pkg = false;
    {
        std::string perr;
        if (!barch::package::read(r.dir, has_pkg, pkg, perr)) {
            note_package(r.name, "failed", {});
            return "package.luau: " + perr;
        }
    }
    if (!has_pkg) {
        // the settings it wrote are listed in the configuration space, so this
        // finds them after a restart too
        drop_package(r);
        note_package(r.name, {}, {});
        return import_checkout(r);
    }

    auto fail = [&](const std::string& err) {
        note_package(r.name, "failed", hooks);
        return err;
    };
    if (!pkg.loads.empty() && (!r.space.empty() || r.as == "fs"))
        return fail("package.luau lists what to load, so the repository's own space and as "
                    "settings have to be off");
    auto err = apply_settings(r, pkg);
    if (!err.empty())
        return fail("package.luau: " + err);
    // before the package's own folders, so what it loads can rely on them - TODO 585
    err = apply_depends(r, pkg);
    if (!err.empty())
        return fail(err);
    err = pkg.loads.empty() ? import_checkout(r) : import_listed(r, pkg);
    if (!err.empty())
        return fail(err);
    err = apply_http(r, pkg);
    if (!err.empty())
        return fail("package.luau: " + err);
    if (pkg.after) {
        std::string new_head;
        (void) git_head(r.dir, new_head);
        bool first;
        {
            std::lock_guard<std::mutex> g(state_mu);
            first = !state_of(r.name).after_ran;
        }
        if (!had_checkout || new_head != old_head || first) {
            err = run_hook(r, *pkg.after, "after", old_head, new_head, hooks);
            if (!err.empty())
                return fail(err);
            if (!r.user.empty()) {
                std::lock_guard<std::mutex> g(state_mu);
                state_of(r.name).after_ran = true;
            }
        }
    }
    note_package(r.name, "applied", hooks);
    return {};
}

/** run one repository and record what happened. Returns the error, if any */
std::string run_repo(const barch::repo_conf& r, const std::string& pin) {
    // one line, always: a luau compile error is as multi line as git's stderr, and
    // both end up in a RESP error - see one_line
    /*
     * Syncs run one at a time: every caller holds `mu`. The poller didn't, and ran a
     * repository while FUNCTIONS SYNC ran it too - two git resets in one checkout and
     * unlocked writes to managed_by. That was invisible from outside (git did not
     * trip on its index.lock, and the latches the syncs share kept ThreadSanitizer
     * quiet), so this says so if it ever happens again - TODO 583.
     */
    static std::atomic<int> in_sync{0};
    // a package's dependencies sync inside its sync, on this thread: those are one
    // sync, not two - TODO 585
    struct entered {
        bool outer;
        explicit entered(const std::string& name) : outer(sync_stack.empty()) {
            if (outer && in_sync.fetch_add(1) > 0)
                barch::err({"function sync", name, "started while another sync was running"});
            sync_stack.push_back(name);
        }
        ~entered() {
            sync_stack.pop_back();
            if (outer)
                in_sync.fetch_sub(1);
        }
    };
    std::string err;
    {
        entered here(r.name);
        err = one_line(do_sync_repo(r, pin));
    }
    std::lock_guard<std::mutex> g(state_mu);
    auto& st = state_of(r.name);
    if (err.empty()) {
        st.last_err.clear();
        st.last_ok = "ok";
        st.state = "ok";
        st.last_stamp.clear();
        std::string sha;
        if (git_head(r.dir, sha))
            st.last_stamp = sha;
    } else {
        st.last_err = err;
        st.state = "failed";
        barch::err({"function sync", r.name, err});
    }
    return err;
}

} // namespace

namespace barch {

bool scan_directory(const std::string& dir, const std::string& prefix,
                    std::vector<import_file>& out, std::string& err) {
    return scan_as_keys(dir, prefix, out, err);
}

/**
 * Sync every enabled repository. The first error is returned, but the rest still
 * run: one repository that cannot fetch is not a reason to leave the others stale.
 */
std::string sync_functions(const std::string& pin) {
    if (hook_running.load())
        return "a package.luau hook is running; a sync now would wait for it, and one sent from the hook would wait for itself";
    std::lock_guard<std::mutex> g(mu);
    auto repos = read_repos();
    if (repos.empty())
        return "no git repositories are configured";
    auto conflicts = refuse_overlap(repos);
    std::string first;
    for (const auto& line : conflicts) {
        barch::err({"git repositories", line});
        if (first.empty())
            first = line;
    }
    {
        std::lock_guard<std::mutex> sg(state_mu);
        for (const auto& r : repos) {
            if (!r.enabled)
                state_of(r.name).state = first.empty() ? "disabled" : "conflict";
        }
    }
    if (!pin.empty()) {
        /*
         * `FUNCTIONS SYNC <rev>` was unambiguous while there was one checkout to
         * mean it about. With several it is not, and quietly resetting every
         * repository to one rev is not a reasonable reading of it.
         */
        size_t live = 0;
        for (const auto& r : repos)
            live += r.enabled ? 1 : 0;
        if (live > 1)
            return "a commit needs a repository: FUNCTIONS SYNC <repo> <commit>";
    }
    size_t ran = 0;
    for (const auto& r : repos) {
        if (!r.enabled)
            continue;
        ++ran;
        auto err = run_repo(r, pin);
        if (!err.empty() && first.empty())
            first = err;
    }
    if (ran == 0 && first.empty())
        first = "every configured repository is disabled";
    return first;
}

std::string sync_repo(const std::string& name, const std::string& pin) {
    if (hook_running.load())
        return "a package.luau hook is running; a sync now would wait for it, and one sent from the hook would wait for itself";
    std::lock_guard<std::mutex> g(mu);
    auto repos = read_repos();
    auto conflicts = refuse_overlap(repos);
    for (const auto& r : repos) {
        if (r.name != name)
            continue;
        if (!r.enabled) {
            if (!r.invalid.empty())
                return r.name + ": " + r.invalid;
            for (const auto& line : conflicts) {
                if (line.find(name) != std::string::npos)
                    return line;
            }
            return "repository " + name + " is disabled";
        }
        return run_repo(r, pin);
    }
    return "no repository called " + name;
}

bool have_repo(const std::string& name) {
    for (const auto& r : read_repos()) {
        if (r.name == name)
            return true;
    }
    return false;
}

bool any_repo_configured() {
    return !read_repos().empty();
}

/**
 * The repositories that asked not to be waited for are left to the sync thread.
 * These are the ones that said `asynch off`, meaning they must be in place before
 * anything is served, so a failure here is a failure to start.
 */
std::string sync_startup_repos() {
    std::lock_guard<std::mutex> g(mu);
    auto repos = read_repos();
    auto conflicts = refuse_overlap(repos);
    for (const auto& line : conflicts)
        barch::err({"git repositories", line});
    for (const auto& r : repos) {
        if (!r.enabled || r.asynch)
            continue;
        auto err = run_repo(r, {});
        if (!err.empty())
            return r.name + ": " + err;
    }
    return {};
}

/** the package that added a repository as a dependency, or empty - TODO 585 */
static std::string added_by_of(const std::string& repo) {
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return {};
    auto acc = barch::functions::store_for_owner(conf);
    std::string owner;
    if (!acc.get || acc.get(repo_key(repo, "package/added_by"), owner) != read_state::present)
        return {};
    return owner;
}

std::string functions_sync_status() {
    auto repos = read_repos();
    auto conflicts = refuse_overlap(repos);
    std::lock_guard<std::mutex> g(state_mu);
    std::ostringstream o;
    if (repos.empty())
        return "no repositories";
    bool first = true;
    for (const auto& r : repos) {
        if (!first)
            o << "\n";
        first = false;
        auto it = states.find(r.name);
        std::string state = "pending", last = "never", stamp, package, hooks, depends;
        if (it != states.end()) {
            state = it->second.state;
            package = it->second.package;
            hooks = it->second.hooks;
            depends = it->second.depends;
            if (!it->second.last_err.empty())
                last = it->second.last_err;
            else if (!it->second.last_ok.empty())
                last = it->second.last_ok;
            stamp = it->second.last_stamp;
        }
        if (!r.enabled) {
            state = "disabled";
            if (!r.invalid.empty())
                last = r.invalid;
        }
        for (const auto& line : conflicts) {
            if (line.find(r.name) != std::string::npos) {
                state = "conflict";
                last = line;
            }
        }
        o << "name=" << r.name
          << " state=" << state
          << " dir=" << r.dir
          << " url=" << (r.url.empty() ? "off" : r.url)
          << " as=" << r.as
          << (r.as == "fs" ? " root=" + r.fs_root : std::string())
          << " space=" << (r.space.empty() ? (r.as == "fs" ? "(default)" : "folders")
                                           : r.space)
          << " pull=" << (r.pull ? "on" : "off")
          << " branch=" << r.branch
          << " pin=" << (r.commit.empty() ? "off" : r.commit)
          << " interval=" << r.ms
          << " asynch=" << (r.asynch ? "on" : "off")
          << " last=" << last;
        if (!stamp.empty())
            o << " commit=" << stamp;
        if (!r.user.empty())
            o << " user=" << r.user;
        if (!package.empty())
            o << " package=" << package;
        if (!hooks.empty())
            o << " hooks=" << hooks;
        if (!depends.empty())
            o << " depends=" << depends;
        if (!added_by_of(r.name).empty())
            o << " added_by=" << added_by_of(r.name);
    }
    return o.str();
}

void request_function_sync() {
    kick_all.store(true);
    cv.notify_all();
}

void request_repo_sync(const std::string& name) {
    {
        std::lock_guard<std::mutex> g(state_mu);
        kick_names.insert(name);
    }
    cv.notify_all();
}

void stop_function_sync() {

    running.store(false);
    cv.notify_all();
    if (worker.joinable())
        worker.join();
}

/**
 * One thread, a due time per repository. It used to be one interval for the one
 * checkout; now each repository has its own `ms`, and a repository whose interval
 * is 0 only runs when it is asked for.
 *
 * The first run of each is staggered rather than fired at once, for the same
 * reason a scheduled job carries a jitter: a fleet coming up together should not
 * arrive at the remote in one burst.
 */
}

namespace {

/*
 * Told about every write to the configuration space - TODO 584. A repository set
 * up, changed or removed while the server runs used to wait for a restart, since
 * nothing woke the poller and nothing started it. Called under the configuration
 * shard's latch, so it sets a flag and notifies, and takes no lock: the syncs hold
 * `mu` while they read that same shard. A notify that lands just before the poller
 * sleeps is lost, and then its idle minute is the bound.
 */
struct repo_watch final : barch::index_sink {
    void changed(art::value_type key, bool) override {
        if (encoded_key_as_string(key).rfind("git/repositories/", 0) != 0)
            return;
        kick_rescan.store(true);
        cv.notify_all();
    }
};

repo_watch watcher;

/*
 * The configuration space is never indexed (perm_index's never_indexed), so its
 * shards' index_to is free, and nothing else points it anywhere.
 */
void watch_repositories() {
    auto conf = barch::get_keyspace("configuration");
    if (!conf)
        return;
    for (const auto& shard : conf->get_shards()) {
        if (shard)
            shard->index_to.store(&watcher, std::memory_order_release);
    }
}

}

namespace barch {

void start_function_sync() {
    if (running.exchange(true))
        return;
    watch_repositories();
    worker = std::thread([] {
        using clock = std::chrono::steady_clock;
        while (running.load()) {
            // before the read, so a write that lands during it asks for another
            kick_rescan.store(false);
            auto repos = read_repos();
            auto conflicts = refuse_overlap(repos);
            for (const auto& line : conflicts)
                barch::err({"git repositories", line});

            auto now = clock::now();
            uint64_t wait_ms = 60000;         // an idle cap, so configuration changes land
            size_t index = 0;
            std::vector<barch::repo_conf> due;
            {
                std::lock_guard<std::mutex> g(state_mu);
                bool all = kick_all.exchange(false);
                for (auto& r : repos) {
                    auto& st = state_of(r.name);
                    bool asked = all;
                    auto k = kick_names.find(r.name);
                    if (k != kick_names.end()) {
                        kick_names.erase(k);
                        asked = true;
                    }
                    if (!r.enabled) {
                        st.state = conflicts.empty() ? "disabled" : "conflict";
                        continue;
                    }
                    /*
                     * An interval of 0 does not poll and does not run itself, which
                     * is what the single `functions_sync_ms` did when it was 0. A
                     * repository that wants to be applied at boot without polling
                     * says so with `asynch off`, which start-up runs.
                     */
                    if (r.ms == 0) {
                        if (asked)
                            due.push_back(r);
                        continue;
                    }
                    // a new interval starts a new schedule, rather than waiting
                    // out the old one first
                    if (st.ms != r.ms) {
                        st.ms = r.ms;
                        st.due_set = false;
                    }
                    if (!st.due_set) {
                        st.due = now + std::chrono::milliseconds(200 * (index++));
                        st.due_set = true;
                    }
                    if (asked || now >= st.due) {
                        due.push_back(r);
                        st.due = now + std::chrono::milliseconds(r.ms);
                        // its next run is a wait too: left at the idle cap, a
                        // repository's interval held for its first run only and it
                        // polled once a minute after that - TODO 583
                        if (r.ms < wait_ms)
                            wait_ms = r.ms;
                        continue;
                    }
                    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                        st.due - now).count();
                    if (left > 0 && (uint64_t) left < wait_ms)
                        wait_ms = (uint64_t) left;
                }
            }
            for (const auto& r : due) {
                if (!running.load())
                    break;
                // as FUNCTIONS SYNC does, one repository at a time, so a client's
                // sync can come between two of the poller's - TODO 583
                std::lock_guard<std::mutex> g(mu);
                (void) run_repo(r, {});
            }
            if (!running.load())
                break;
            {
                std::unique_lock<std::mutex> lk(mu);
                cv.wait_for(lk, std::chrono::milliseconds(wait_ms), [] {
                    return !running.load() || kick_all.load() || !kick_names.empty() ||
                           kick_rescan.load();
                });
            }
        }
    });
}

} // namespace barch
