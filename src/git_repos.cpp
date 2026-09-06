#include "git_repos.h"

#include "configuration.h"
#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"

#include <algorithm>
#include <cstdlib>

namespace {

const char* PREFIX = "git/repositories/";

barch::key_space_ptr conf_space() {
    return barch::get_keyspace("configuration");
}

/** the key just past everything starting with `prefix` */
std::string upper_bound_of(const std::string& prefix) {
    std::string hi = prefix;
    while (!hi.empty()) {
        auto c = (unsigned char) hi.back();
        if (c != 0xff) {
            hi.back() = (char) (c + 1);
            return hi;
        }
        hi.pop_back();
    }
    return {};
}

std::string trimmed(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == ' ' || s[a] == '\t' || s[a] == '\n' || s[a] == '\r'))
        ++a;
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\n' || s[b - 1] == '\r'))
        --b;
    return s.substr(a, b - a);
}

bool truth(const std::string& v, bool dflt) {
    auto s = trimmed(v);
    for (auto& c : s)
        c = (char) tolower((unsigned char) c);
    if (s.empty())
        return dflt;
    if (s == "1" || s == "on" || s == "true" || s == "yes")
        return true;
    if (s == "0" || s == "off" || s == "false" || s == "no")
        return false;
    return dflt;
}

/*
 * `off` is how the old settings said "not set", and a value written by hand is as
 * likely to say that as to be absent. Treated as absent everywhere here so the two
 * spellings cannot mean different things.
 */
bool is_off(const std::string& v) {
    auto s = trimmed(v);
    for (auto& c : s)
        c = (char) tolower((unsigned char) c);
    return s == "off";
}

} // namespace

namespace barch {

std::string repo_default_dir(const std::string& name) {
    auto base = get_functions_dir();
    if (base.empty())
        base = "functions";
    return base + "/" + name;
}

std::string check_repo_setting(const std::string& setting, const std::string& value) {
    static const char* known[] = {"url", "dir", "branch", "commit", "pull", "ms",
                                  "ssh_key", "space", "enabled", "asynch",
                                  "as", "fs_root"};
    bool found = false;
    for (auto* k : known)
        found = found || setting == k;
    if (!found)
        return "'" + setting + "' is not a repository setting";
    auto v = trimmed(value);
    if (setting == "ms" && !v.empty() && !is_off(v)) {
        for (char c : v)
            if (!isdigit((unsigned char) c))
                return "ms is a number of milliseconds";
    }
    if (setting == "as" && !v.empty() && v != "keys" && v != "fs")
        return "as is 'keys' or 'fs'";
    if (setting == "space" && !v.empty() && !is_off(v) && !check_ks_name(v))
        return "'" + v + "' is not a key space name";
    /*
     * A deploy key is a reference, never the key. Stored here it would be in the
     * saved shards and, per the configuration space being replicated, on every
     * replica too - which is not what "read only deploy key on this host" means.
     */
    if (setting == "ssh_key" && v.rfind("-----BEGIN", 0) == 0)
        return "ssh_key holds a path or file:/path, not the key itself";
    return {};
}

heap::vector<repo_conf> read_repos() {
    heap::vector<repo_conf> out;
    auto space = conf_space();
    if (!space)
        return out;
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.range)
        return out;

    std::string lo = PREFIX;
    std::string hi = upper_bound_of(lo);
    heap::vector<std::string> keys;
    acc.range(lo, hi, 100000, keys);

    heap::string_map<size_t> at;
    for (const auto& key : keys) {
        // git/repositories/<name>/<setting>
        auto rest = key.substr(lo.size());
        auto slash = rest.find('/');
        if (slash == std::string::npos || slash == 0 || slash + 1 >= rest.size())
            continue;
        auto name = rest.substr(0, slash);
        auto setting = rest.substr(slash + 1);
        if (setting.find('/') != std::string::npos)
            continue;                       // nothing nests below a setting
        std::string value;
        if (acc.get(key, value) != barch::foreign::store_access::read_state::present)
            continue;
        value = trimmed(value);

        auto it = at.find(name);
        if (it == at.end()) {
            at[name] = out.size();
            repo_conf fresh;
            fresh.name = name;
            out.push_back(std::move(fresh));
            it = at.find(name);
        }
        auto& r = out[it->second];
        auto bad = check_repo_setting(setting, value);
        if (!bad.empty()) {
            if (r.invalid.empty())
                r.invalid = bad;
            barch::err({"git repository", name, bad});
            continue;
        }
        if (setting == "url")            r.url = is_off(value) ? std::string() : value;
        else if (setting == "dir")       r.dir = is_off(value) ? std::string() : value;
        else if (setting == "branch")    r.branch = value.empty() ? "main" : value;
        else if (setting == "commit")    r.commit = is_off(value) ? std::string() : value;
        else if (setting == "pull")      r.pull = truth(value, false);
        else if (setting == "ms")        r.ms = is_off(value) ? 0 : strtoull(value.c_str(), nullptr, 10);
        else if (setting == "ssh_key")   r.ssh_key = is_off(value) ? std::string() : value;
        else if (setting == "space")     r.space = is_off(value) ? std::string() : value;
        else if (setting == "enabled")   r.enabled = truth(value, true);
        else if (setting == "asynch")    r.asynch = truth(value, true);
        else if (setting == "as")        r.as = value.empty() ? "keys" : value;
        else if (setting == "fs_root")   r.fs_root = value.empty() ? "/" : value;

    }

    /*
     * Nothing configured the new way falls back to the old six. A url is not among
     * them, so `default` never clones - which is exactly what it did before, where
     * git_pull returns early unless the checkout is already there.
     */
    if (out.empty() && !get_functions_dir().empty()) {
        repo_conf legacy;
        legacy.name = "default";
        legacy.dir = get_functions_dir();
        legacy.branch = get_functions_git_branch().empty() ? "main" : get_functions_git_branch();
        legacy.commit = get_functions_git_commit();
        legacy.pull = get_functions_git_pull();
        legacy.ms = get_functions_sync_ms();
        legacy.ssh_key = get_functions_git_ssh_key();
        out.push_back(std::move(legacy));
        return out;
    }

    for (auto& r : out) {
        if (r.dir.empty())
            r.dir = repo_default_dir(r.name);
        if (!r.invalid.empty())
            r.enabled = false;
    }
    std::sort(out.begin(), out.end(),
              [](const repo_conf& a, const repo_conf& b) { return a.name < b.name; });
    return out;
}

heap::vector<std::string> refuse_overlap(heap::vector<repo_conf>& repos) {
    heap::vector<std::string> conflicts;
    heap::string_map<std::string> space_owner;   // declared space -> repo
    heap::string_map<std::string> dir_owner;
    heap::string_set doomed;

    for (const auto& r : repos) {
        if (!r.enabled)
            continue;
        if (!r.space.empty()) {
            auto it = space_owner.find(r.space);
            if (it != space_owner.end()) {
                conflicts.push_back(it->second + " and " + r.name
                                    + " both want key space " + r.space);
                doomed.insert(it->second);
                doomed.insert(r.name);
            } else {
                space_owner[r.space] = r.name;
            }
        }
        auto d = dir_owner.find(r.dir);
        if (d != dir_owner.end()) {
            conflicts.push_back(d->second + " and " + r.name + " share the checkout " + r.dir);
            doomed.insert(d->second);
            doomed.insert(r.name);
        } else {
            dir_owner[r.dir] = r.name;
        }
    }
    for (auto& r : repos) {
        if (doomed.find(r.name) != doomed.end())
            r.enabled = false;
    }
    return conflicts;
}

}
