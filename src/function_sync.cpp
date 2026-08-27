#include "function_sync.h"

#include "configuration.h"
#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"
#include "shard.h"
#include "sharded_store.h"

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
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

struct luau_file {
    std::string space; // empty is the default space
    std::string name;
    std::string source;
    std::string path;
};

std::mutex mu;
std::condition_variable cv;
std::atomic<bool> running{false};
std::atomic<bool> kick{false};
std::thread worker;
std::string last_ok;
std::string last_err;
std::string last_stamp;
heap::string_set managed;

std::string fold_name(std::string s) {
    for (auto& ch : s)
        ch = (char) toupper((unsigned char) ch);
    return s;
}

std::string stem_of(const std::string& file) {
    auto slash = file.find_last_of('/');
    std::string base = slash == std::string::npos ? file : file.substr(slash + 1);
    if (base.size() < 6 || base.substr(base.size() - 5) != ".luau")
        return {};
    if (base[0] == '.')
        return {};
    return fold_name(base.substr(0, base.size() - 5));
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

std::string resolve_secret(const std::string& raw, std::string& err) {
    if (raw.size() >= 5 && raw.compare(0, 5, "file:") == 0) {
        std::string body;
        if (!read_file(raw.substr(5), body) || body.empty()) {
            err = "cannot read git ssh key file";
            return {};
        }
        while (!body.empty() && (body.back() == '\n' || body.back() == '\r' || body.back() == ' '))
            body.pop_back();
        return body;
    }
    if (raw.size() >= 4 && raw.compare(0, 4, "env:") == 0) {
        const char* v = std::getenv(raw.substr(4).c_str());
        if (!v || !*v) {
            err = "git ssh key env not set";
            return {};
        }
        return v;
    }
    return raw;
}

int run_cmd(const std::vector<std::string>& args,
            const std::vector<std::pair<std::string, std::string>>& extra_env,
            std::string& out, std::string& err) {
    int outp[2], errp[2];
    if (pipe(outp) != 0 || pipe(errp) != 0)
        return -1;
    pid_t pid = fork();
    if (pid < 0)
        return -1;
    if (pid == 0) {
        dup2(outp[1], STDOUT_FILENO);
        dup2(errp[1], STDERR_FILENO);
        close(outp[0]); close(outp[1]);
        close(errp[0]); close(errp[1]);
        for (const auto& [k, v] : extra_env)
            setenv(k.c_str(), v.c_str(), 1);
        std::vector<char*> argv;
        for (const auto& a : args)
            argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);
        execvp(argv[0], argv.data());
        _exit(127);
    }
    close(outp[1]);
    close(errp[1]);
    char buf[4096];
    ssize_t n;
    while ((n = read(outp[0], buf, sizeof buf)) > 0)
        out.append(buf, (size_t) n);
    while ((n = read(errp[0], buf, sizeof buf)) > 0)
        err.append(buf, (size_t) n);
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

std::string git_pull(const std::string& dir, std::string& err) {
    if (!is_dir(dir + "/.git"))
        return {};
    std::string branch = barch::get_functions_git_branch();
    if (branch.empty())
        branch = "main";
    std::vector<std::pair<std::string, std::string>> env;
    std::string keyspec = barch::get_functions_git_ssh_key();
    if (!keyspec.empty()) {
        std::string key = resolve_secret(keyspec, err);
        if (key.empty())
            return err;
        // a path: file: already returned file contents which is not a path.
        // if it looks like a path on disk, use it; otherwise write a temp key.
        std::string keyfile = keyspec;
        if (keyspec.size() >= 5 && keyspec.compare(0, 5, "file:") == 0)
            keyfile = keyspec.substr(5);
        else if (keyspec.size() >= 4 && keyspec.compare(0, 4, "env:") == 0) {
            err = "env git ssh key must be a path to a key file";
            return err;
        }
        env.emplace_back("GIT_SSH_COMMAND",
                         "ssh -i " + keyfile +
                         " -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new");
    }
    std::string out, e2;
    int rc = run_cmd({"git", "-C", dir, "fetch", "--quiet", "origin", branch}, env, out, e2);
    if (rc != 0) {
        err = e2.empty() ? "git fetch failed" : e2;
        return err;
    }
    out.clear(); e2.clear();
    rc = run_cmd({"git", "-C", dir, "reset", "--hard", "--quiet", "FETCH_HEAD"}, env, out, e2);
    if (rc != 0) {
        err = e2.empty() ? "git reset failed" : e2;
        return err;
    }
    return {};
}

bool scan_luau(const std::string& root, std::vector<luau_file>& files, std::string& err) {
    auto add = [&](const std::string& space, const std::string& path) {
        std::string name = stem_of(path);
        if (name.empty())
            return true;
        luau_file f;
        f.space = space;
        f.name = name;
        f.path = path;
        if (!read_file(path, f.source)) {
            err = "could not read " + path;
            return false;
        }
        files.push_back(std::move(f));
        return true;
    };
    for (const auto& name : list_dir(root)) {
        std::string path = root + "/" + name;
        if (is_reg(path)) {
            if (!add({}, path))
                return false;
        } else if (is_dir(path)) {
            if (name == ".git" || name == "configuration")
                continue;
            if (!barch::check_ks_name(name)) {
                barch::err({"function sync skipping folder, not a space name", name});
                continue;
            }
            for (const auto& f : list_dir(path)) {
                std::string fp = path + "/" + f;
                if (is_reg(fp) && !add(name, fp))
                    return false;
            }
        }
    }
    return true;
}

barch::key_space_ptr dest_of(const std::string& space) {
    if (space.empty())
        return barch::get_keyspace("");
    return barch::get_keyspace(space);
}

bool fill_temp(const std::vector<luau_file>& files, barch::key_space_ptr& tmp, std::string& err) {
    tmp = barch::key_space::make_scratch();
    std::vector<const luau_file*> left;
    left.reserve(files.size());
    for (const auto& f : files)
        left.push_back(&f);
    while (!left.empty()) {
        std::vector<const luau_file*> next;
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
    return true;
}

bool install_all(const barch::key_space_ptr& dest,
                 const heap::string_map<std::string>& sources,
                 const heap::vector<std::string>& want,
                 std::string& err) {
    std::vector<const std::string*> left;
    left.reserve(want.size());
    for (const auto& n : want)
        left.push_back(&n);
    while (!left.empty()) {
        std::vector<const std::string*> next;
        std::string last;
        size_t progress = 0;
        for (auto* n : left) {
            auto it = sources.find(*n);
            std::string e;
            if (it != sources.end() && barch::functions::install(dest, *n, it->second, e)) {
                ++progress;
            } else {
                last = e.empty() ? ("missing source for " + *n) : e;
                next.push_back(n);
            }
        }
        if (progress == 0) {
            err = last.empty() ? "function sync could not apply" : last;
            return false;
        }
        left.swap(next);
    }
    return true;
}

void restore_functions(const barch::key_space_ptr& dest,
                       const heap::vector<std::string>& previous,
                       const heap::string_map<std::string>& previous_src) {
    for (const auto& n : barch::functions::names(dest))
        barch::functions::remove(dest, n);
    std::string ignored;
    (void) install_all(dest, previous_src, previous, ignored);
}

bool apply_dest(const barch::key_space_ptr& tmp, const barch::key_space_ptr& dest,
                std::string& err) {
    auto want = barch::functions::names(tmp);
    heap::string_map<std::string> sources;
    for (const auto& n : want) {
        std::string src;
        if (!barch::functions::source_in(tmp, n, src)) {
            err = "temp function " + n + " had no source";
            return false;
        }
        sources[n] = std::move(src);
    }
    auto had = barch::functions::names(dest);
    heap::string_map<std::string> had_src;
    for (const auto& n : had) {
        std::string src;
        if (barch::functions::source_in(dest, n, src))
            had_src[n] = std::move(src);
    }
    const bool one_shard = dest->get_shard_count() == 1;
    barch::sharded_store store(dest);
    if (one_shard)
        store.each_shard([](const barch::shard_ptr& t) { t->begin(); });
    if (!install_all(dest, sources, want, err)) {
        if (one_shard)
            store.each_shard([](const barch::shard_ptr& t) { t->rollback(); });
        else
            restore_functions(dest, had, had_src);
        return false;
    }
    for (const auto& n : had) {
        if (sources.find(n) == sources.end())
            barch::functions::remove(dest, n);
    }
    if (one_shard)
        store.each_shard([](const barch::shard_ptr& t) { t->commit(); });
    return true;
}

std::string do_sync() {
    std::string dir = barch::get_functions_dir();
    if (dir.empty())
        return "functions_dir is not set";
    if (!is_dir(dir))
        return "functions_dir is not a directory";

    if (barch::get_functions_git_pull()) {
        std::string err;
        auto failed = git_pull(dir, err);
        if (!failed.empty())
            return failed;
    }

    std::vector<luau_file> files;
    std::string err;
    if (!scan_luau(dir, files, err))
        return err;

    heap::string_map<std::vector<luau_file>> by_space;
    for (auto& f : files)
        by_space[f.space].push_back(std::move(f));

    heap::string_set seen;
    for (auto& [space, group] : by_space) {
        seen.insert(space);
        barch::key_space_ptr tmp;
        if (!fill_temp(group, tmp, err))
            return err;
        auto dest = dest_of(space);
        if (!apply_dest(tmp, dest, err)) {
            tmp.reset();
            return err;
        }
        tmp.reset();
        managed.insert(space);
    }
    heap::vector<std::string> gone;
    for (const auto& space : managed) {
        if (seen.find(space) == seen.end())
            gone.push_back(space);
    }
    for (const auto& space : gone) {
        barch::key_space_ptr tmp = barch::key_space::make_scratch();
        auto dest = dest_of(space);
        if (!apply_dest(tmp, dest, err)) {
            tmp.reset();
            return err;
        }
        tmp.reset();
        managed.erase(space);
    }
    return {};
}

} // namespace

namespace barch {

std::string sync_functions() {
    std::lock_guard<std::mutex> g(mu);
    auto err = do_sync();
    if (err.empty()) {
        last_err.clear();
        last_ok = "ok";
        last_stamp.clear();
        std::string dir = get_functions_dir();
        std::string sha;
        if (!dir.empty() && git_head(dir, sha))
            last_stamp = sha;
    } else {
        last_err = err;
        barch::err({"function sync", err});
    }
    return err;
}

std::string functions_sync_status() {
    std::lock_guard<std::mutex> g(mu);
    std::ostringstream o;
    o << "dir=" << get_functions_dir()
      << " pull=" << (get_functions_git_pull() ? "on" : "off")
      << " branch=" << get_functions_git_branch()
      << " interval=" << get_functions_sync_ms()
      << " last=" << (last_err.empty() ? (last_ok.empty() ? "never" : last_ok) : last_err);
    if (!last_stamp.empty())
        o << " commit=" << last_stamp;
    return o.str();
}

void request_function_sync() {
    kick.store(true);
    cv.notify_all();
}

void stop_function_sync() {
    running.store(false);
    cv.notify_all();
    if (worker.joinable())
        worker.join();
}

void start_function_sync() {
    if (running.exchange(true))
        return;
    worker = std::thread([] {
        while (running.load()) {
            uint64_t ms = barch::get_functions_sync_ms();
            {
                std::unique_lock<std::mutex> lk(mu);
                if (ms == 0) {
                    cv.wait(lk, [] { return !running.load() || kick.load(); });
                } else {
                    cv.wait_for(lk, std::chrono::milliseconds(ms),
                                [] { return !running.load() || kick.load(); });
                }
            }
            if (!running.load())
                break;
            bool run_now = kick.exchange(false) || barch::get_functions_sync_ms() > 0;
            if (!run_now)
                continue;
            if (barch::get_functions_dir().empty())
                continue;
            (void) sync_functions();
        }
    });
}

} // namespace barch
