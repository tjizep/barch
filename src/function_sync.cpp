#include "function_sync.h"

#include "configuration.h"
#include "git_repos.h"
#include "fs_api.h"
#include "staged.h"
#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstring>
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
};

std::mutex state_mu;
heap::string_map<repo_state> states;
/** which repository owns a key space, so one cannot swap away another's work */
heap::string_map<std::string> managed_by;
heap::string_map<heap::string_set> managed_data;
/** repositories asked for by name, and the "everything" flag */
heap::string_set kick_names;
std::atomic<bool> kick_all{false};

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

bool read_file(const std::string& path, std::string& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
        return false;
    out.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return true;
}

bool is_dir(const std::string& path) {
    struct stat st {};
    return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_reg(const std::string& path) {
    struct stat st {};
    return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

std::vector<std::string> list_dir(const std::string& path) {
    std::vector<std::string> out;
    DIR* d = opendir(path.c_str());
    if (!d)
        return out;
    while (auto* e = readdir(d)) {
        if (e->d_name[0] == '.' && (e->d_name[1] == 0 || (e->d_name[1] == '.' && e->d_name[2] == 0)))
            continue;
        out.emplace_back(e->d_name);
    }
    closedir(d);
    std::sort(out.begin(), out.end());
    return out;
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
            if (!at.empty() && !is_dir(at) && ::mkdir(at.c_str(), 0755) != 0 && errno != EEXIST)
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
std::string git_checkout(const barch::repo_conf& r, const std::string& pin, std::string& err) {
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
    if (!is_dir(r.dir + "/.git")) {
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
    if (!read_file(path, f.source)) {
        err = "could not read " + path;
        return false;
    }
    files.push_back(std::move(f));
    return true;
}

bool scan_tree(const std::string& dir, const std::string& space, const std::string& prefix,
               std::vector<checkout_file>& files, std::string& err) {
    for (const auto& name : list_dir(dir)) {
        if (hidden_name(name))
            continue;
        std::string path = dir + "/" + name;
        if (is_reg(path)) {
            if (!add_file(space, prefix, path, name, files, err))
                return false;
        } else if (is_dir(path)) {
            if (!scan_tree(path, space, key_in(prefix, name), files, err))
                return false;
        }
    }
    return true;
}

bool scan_checkout(const std::string& root, std::vector<checkout_file>& files, std::string& err) {
    for (const auto& name : list_dir(root)) {
        if (hidden_name(name))
            continue;
        std::string path = root + "/" + name;
        if (is_reg(path)) {
            if (!add_file({}, {}, path, name, files, err))
                return false;
        } else if (is_dir(path)) {
            if (name == "configuration")
                continue;
            if (!barch::check_ks_name(name)) {
                barch::err({"function sync skipping folder, not a space name", name});
                continue;
            }
            if (!scan_tree(path, name, {}, files, err))
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
    for (const auto& n : want) {
        std::string src;
        if (!barch::functions::source_in(tmp, n, src)) {
            err = "temp function " + n + " had no source";
            return false;
        }
        want_fn.insert(n);
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

    if (want_keys.empty())
        managed_data.erase(space);
    else
        managed_data[space] = std::move(want_keys);
    return true;
}

std::string do_sync_repo(const barch::repo_conf& r, const std::string& pin) {
    if (r.dir.empty())
        return "repository " + r.name + " has no checkout directory";

    std::string want = pin.empty() ? r.commit : pin;
    {
        std::string err;
        auto failed = git_checkout(r, want, err);
        if (!failed.empty())
            return failed;
    }
    if (!is_dir(r.dir))
        return r.dir + " is not a directory";

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

    heap::string_set seen;
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

/** run one repository and record what happened. Returns the error, if any */
std::string run_repo(const barch::repo_conf& r, const std::string& pin) {
    // one line, always: a luau compile error is as multi line as git's stderr, and
    // both end up in a RESP error - see one_line
    auto err = one_line(do_sync_repo(r, pin));
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
        std::string state = "pending", last = "never", stamp;
        if (it != states.end()) {
            state = it->second.state;
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
void start_function_sync() {
    if (running.exchange(true))
        return;
    worker = std::thread([] {
        using clock = std::chrono::steady_clock;
        while (running.load()) {
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
                    if (!st.due_set) {
                        st.due = now + std::chrono::milliseconds(200 * (index++));
                        st.due_set = true;
                    }
                    if (asked || now >= st.due) {
                        due.push_back(r);
                        st.due = now + std::chrono::milliseconds(r.ms);
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
                (void) run_repo(r, {});
            }
            if (!running.load())
                break;
            {
                std::unique_lock<std::mutex> lk(mu);
                cv.wait_for(lk, std::chrono::milliseconds(wait_ms), [] {
                    return !running.load() || kick_all.load() || !kick_names.empty();
                });
            }
        }
    });
}

} // namespace barch
