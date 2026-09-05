//
// LOADFS - import a directory into the key space as the chunked file store.
//
// The layout is DONE 226 and 228: `fs:m:<path>` is the metadata as json and
// `fs:d:<path>|<n>` is one chunk of the content, the index zero padded so a range
// scan and a counted loop agree about the order. `kind = "files"` HTTP routes serve
// straight out of these keys.
//
// The whole import lands or none of it does. That is done by reading and chunking
// everything first, snapshotting whatever the destination keys hold now, and rolling
// that snapshot back if any write fails - so the cost of the guarantee is holding the
// import in memory while it is in flight. TODO 239 is the way out of that ceiling:
// hash_arena backing its pages with a named memory mapped file instead of anonymous
// memory, so a set of files larger than RAM can be held.
//
// See TODO 238.
//
#include "fs_api.h"

#include <cstdio>
#include <string>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>

#include "caller.h"
#include "configuration.h"
#include <set>

#include "function_api.h"
#include "function_sync.h"
#include "module.h"
#include "vk_caller.h"

namespace {

/** the default chunk, well under maximum_allocation_size and a round number of pages */
constexpr size_t default_chunk = 65536;

std::string as_text(art::value_type v) {
    return {v.chars(), v.size};
}

bool is_dir(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_reg(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/** a dot file is skipped, and so are the two directory entries that are not files */
bool skipped_name(const std::string& name) {
    return name.empty() || name[0] == '.';
}

bool read_file(const std::string& path, std::string& out) {
    FILE* f = ::fopen(path.c_str(), "rb");
    if (!f)
        return false;
    out.clear();
    char buf[65536];
    size_t n;
    while ((n = ::fread(buf, 1, sizeof buf, f)) > 0)
        out.append(buf, n);
    bool ok = ::ferror(f) == 0;
    ::fclose(f);
    return ok;
}

std::vector<std::string> list_dir(const std::string& path) {
    std::vector<std::string> names;
    DIR* d = ::opendir(path.c_str());
    if (!d)
        return names;
    while (auto* e = ::readdir(d))
        names.emplace_back(e->d_name);
    ::closedir(d);
    // so an import of the same tree twice writes in the same order, which makes a
    // failure reproducible rather than depending on what the filesystem hands back
    std::sort(names.begin(), names.end());
    return names;
}

/** a guess from the extension, the same short table the HTTP side serves with */
std::string type_of(const std::string& name) {
    auto dot = name.find_last_of('.');
    if (dot == std::string::npos)
        return "application/octet-stream";
    std::string ext = name.substr(dot + 1);
    for (auto& c : ext)
        c = (char) ::tolower((unsigned char) c);
    static const std::map<std::string, std::string> known = {
        {"html", "text/html"},   {"htm", "text/html"},    {"css", "text/css"},
        {"js", "text/javascript"}, {"mjs", "text/javascript"},
        {"json", "application/json"}, {"txt", "text/plain"},  {"csv", "text/csv"},
        {"xml", "application/xml"},  {"svg", "image/svg+xml"},
        {"png", "image/png"},    {"jpg", "image/jpeg"},   {"jpeg", "image/jpeg"},
        {"gif", "image/gif"},    {"webp", "image/webp"},  {"avif", "image/avif"},
        {"ico", "image/x-icon"}, {"woff", "font/woff"},   {"woff2", "font/woff2"},
        {"pdf", "application/pdf"}, {"wasm", "application/wasm"},
        {"mp4", "video/mp4"},    {"webm", "video/webm"},  {"mp3", "audio/mpeg"},
        {"luau", "text/plain"},  {"lua", "text/plain"},   {"md", "text/markdown"},
    };
    auto it = known.find(ext);
    return it == known.end() ? "application/octet-stream" : it->second;
}

std::string chunk_key(const std::string& path, size_t n) {
    char idx[32];
    std::snprintf(idx, sizeof idx, "%08zu", n);
    return "fs:d:" + path + "|" + idx;
}

/** every key one file becomes: the metadata, then a chunk at a time */
void spread(const std::string& stored_path, const std::string& content,
            const std::string& type, size_t chunk,
            std::vector<std::pair<std::string, std::string>>& into) {
    size_t chunks = chunk ? (content.size() + chunk - 1) / chunk : 0;
    for (size_t i = 0; i < chunks; ++i)
        into.emplace_back(chunk_key(stored_path, i),
                          content.substr(i * chunk, chunk));
    // written the way the luau side writes it, since both have to read the other's
    std::string meta = "{\"size\":" + std::to_string(content.size()) +
                       ",\"type\":\"" + type + "\"" +
                       ",\"chunk\":" + std::to_string(chunk) +
                       ",\"chunks\":" + std::to_string(chunks) + "}";
    into.emplace_back("fs:m:" + stored_path, std::move(meta));
}

/** walk `dir`, adding the keys every file under it becomes */
bool gather(const std::string& dir, const std::string& at, size_t chunk,
            std::vector<std::pair<std::string, std::string>>& into,
            size_t& files, uint64_t& bytes, std::string& err) {
    for (const auto& name : list_dir(dir)) {
        if (skipped_name(name))
            continue;
        std::string path = dir + "/" + name;
        // always rooted: `at` is "" for the root, so this is "/name" there and
        // "/sub/name" below it - the same shape the luau side writes
        std::string stored = at + "/" + name;
        if (is_dir(path)) {
            if (!gather(path, stored, chunk, into, files, bytes, err))
                return false;
            continue;
        }
        if (!is_reg(path))
            continue;                       // a socket or a device is not a file here
        std::string content;
        if (!read_file(path, content)) {
            err = "could not read " + path;
            return false;
        }
        bytes += content.size();
        ++files;
        spread(stored, content, type_of(name), chunk, into);
    }
    return true;
}

}

/**
 * The import itself, without a caller, so `barchd --load-fs` and LOADFS are the same
 * code. Empty return is success and `reply` is filled with the summary lines;
 * anything else is the reason it did not happen.
 */
std::string barch::load_fs_directory(const std::string& into_dir, const std::string& into_root,
                                     size_t chunk, const barch::key_space_ptr& space,
                                     std::vector<std::string>& reply) {
    std::string dir = into_dir;
    std::string root = into_root;
    if (chunk == 0 || chunk > (size_t) maximum_allocation_size - 1024)
        return "chunk size must be between 1 and the maximum allocation";
    while (dir.size() > 1 && dir.back() == '/')
        dir.pop_back();
    if (!is_dir(dir))
        return "no such directory";
    if (root.empty())
        root = "/";
    if (root.front() != '/')
        root.insert(root.begin(), '/');
    while (root.size() > 1 && root.back() == '/')
        root.pop_back();

    std::vector<std::pair<std::string, std::string>> writes;
    size_t files = 0;
    uint64_t bytes = 0;
    std::string err;
    // everything is read and chunked before a single key is written, so a directory
    // that cannot be read does not leave half an import behind
    if (!gather(dir, root == "/" ? std::string() : root, chunk, writes, files, bytes, err))
        return err;
    if (writes.empty())
        return "nothing to import";

    auto acc = barch::functions::store_for_owner(space);
    if (!acc.set || !acc.get)
        return "this key space cannot be written";

    // what those keys hold now, so a failure half way can be put back
    std::vector<std::pair<std::string, std::string>> had;
    std::vector<std::string> was_absent;
    had.reserve(writes.size());
    for (const auto& [k, v] : writes) {
        std::string prev;
        if (acc.get(k, prev) == barch::foreign::store_access::read_state::present)
            had.emplace_back(k, std::move(prev));
        else
            was_absent.push_back(k);
    }

    auto roll_back = [&]() {
        for (const auto& [k, v] : had) {
            std::string e;
            acc.set(k, v, e);
        }
        for (const auto& k : was_absent)
            acc.remove(k);
    };

    for (const auto& [k, v] : writes) {
        std::string e;
        if (!acc.set(k, v, e)) {
            roll_back();
            std::string why = "could not write " + k;
            if (!e.empty())
                why += ": " + e;
            return why;
        }
    }

    reply.push_back("files=" + std::to_string(files));
    reply.push_back("bytes=" + std::to_string(bytes));
    reply.push_back("keys=" + std::to_string(writes.size()));
    reply.push_back("root=" + root);
    reply.push_back("chunk=" + std::to_string(chunk));
    return {};
}

/** the same, into the default key space, for a caller that has no space of its own */
std::string barch::load_fs_directory(const std::string& dir, const std::string& root,
                                     std::vector<std::string>& reply) {
    return load_fs_directory(dir, root, default_chunk, get_default_ks(), reply);
}

/**
 * LOADKEYS - the same directory, as discrete keys and stored functions.
 *
 * The walk is the function sync's, through barch::scan_directory, so a directory
 * means the same thing here as it does under `functions_dir`: `.luau` becomes a
 * stored function named after its stem, everything else becomes a key named
 * `prefix:sub:file`, and dot files are skipped.
 *
 * The apply is this file's, not the sync's, on purpose. `apply_dest` records what it
 * wrote in `managed_data` so that a file removed from the checkout is removed from
 * the space on the next pass - and keys imported by hand have no checkout to be
 * missing from, so they would be deleted by the next sync. An import is a one time
 * write, and this keeps it one.
 *
 * Atomic the same way LOADFS is: everything read first, the previous value of every
 * name remembered, and the lot put back if a write fails. See TODO 238.
 */
std::string barch::load_keys_directory(const std::string& into_dir, const std::string& prefix,
                                       const barch::key_space_ptr& space,
                                       std::vector<std::string>& reply) {
    std::string dir = into_dir;
    while (dir.size() > 1 && dir.back() == '/')
        dir.pop_back();
    if (!is_dir(dir))
        return "no such directory";

    std::vector<barch::import_file> files;
    std::string err;
    if (!barch::scan_directory(dir, prefix, files, err))
        return err;
    if (files.empty())
        return "nothing to import";

    auto acc = barch::functions::store_for_owner(space);
    if (!acc.set || !acc.get)
        return "this key space cannot be written";

    // two names for the same file is a directory that cannot be imported as it is,
    // and finding out after half of it is written is no use to anybody
    std::set<std::string> seen_fn, seen_key;
    for (const auto& f : files) {
        auto& into = f.luau ? seen_fn : seen_key;
        if (!into.insert(f.name).second)
            return "two files map to " + std::string(f.luau ? "function " : "key ") + f.name;
    }

    struct previous {
        std::string name;
        std::string value;
        bool luau{false};
        bool had{false};
    };
    std::vector<previous> before;
    before.reserve(files.size());
    for (const auto& f : files) {
        previous p;
        p.name = f.name;
        p.luau = f.luau;
        if (f.luau)
            p.had = barch::functions::source_in(space, f.name, p.value);
        else
            p.had = acc.get(f.name, p.value) == barch::foreign::store_access::read_state::present;
        before.push_back(std::move(p));
    }
    auto roll_back = [&]() {
        for (const auto& p : before) {
            std::string e;
            if (p.luau) {
                if (p.had) barch::functions::install(space, p.name, p.value, e);
                else barch::functions::remove(space, p.name);
            } else {
                if (p.had) acc.set(p.name, p.value, e);
                else acc.remove(p.name);
            }
        }
    };

    size_t functions = 0, keys = 0;
    uint64_t bytes = 0;
    for (const auto& f : files) {
        std::string e;
        bool ok = f.luau ? barch::functions::install(space, f.name, f.source, e)
                         : acc.set(f.name, f.source, e);
        if (!ok) {
            roll_back();
            return f.path + ": " + (e.empty() ? "could not be stored" : e);
        }
        bytes += f.source.size();
        if (f.luau) ++functions; else ++keys;
    }

    reply.push_back("functions=" + std::to_string(functions));
    reply.push_back("keys=" + std::to_string(keys));
    reply.push_back("bytes=" + std::to_string(bytes));
    reply.push_back("prefix=" + (prefix.empty() ? std::string("(none)") : prefix));
    return {};
}

std::string barch::load_keys_directory(const std::string& dir, const std::string& prefix,
                                       std::vector<std::string>& reply) {
    return load_keys_directory(dir, prefix, get_default_ks(), reply);
}

int LOADKEYS(caller& call, const arg_t& argv) {
    if (argv.size() < 2 || argv.size() > 3)
        return call.wrong_arity();
    std::string dir = as_text(argv[1]);
    std::string prefix = argv.size() > 2 ? as_text(argv[2]) : std::string();
    std::vector<std::string> reply;
    auto err = barch::load_keys_directory(dir, prefix, call.kspace(), reply);
    if (!err.empty())
        return call.push_error(err.c_str());
    call.start_array();
    for (const auto& line : reply)
        call.push_string(line);
    return call.end_array();
}

int cmd_LOADKEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LOADKEYS);
}

int LOADFS(caller& call, const arg_t& argv) {
    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();
    std::string dir = as_text(argv[1]);
    std::string root = argv.size() > 2 ? as_text(argv[2]) : std::string("/");
    size_t chunk = default_chunk;
    if (argv.size() > 3)
        chunk = (size_t) strtoull(as_text(argv[3]).c_str(), nullptr, 10);
    std::vector<std::string> reply;
    auto err = barch::load_fs_directory(dir, root, chunk, call.kspace(), reply);
    if (!err.empty())
        return call.push_error(err.c_str());
    call.start_array();
    for (const auto& line : reply)
        call.push_string(line);
    return call.end_array();
}

// cmd_ prefixed like every other module entry point: the name has to differ from
// the caller& overload or the two are ambiguous at the point of use
int cmd_LOADFS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LOADFS);
}

void register_fs_api(function_map& r) {
    r["LOADFS"] = {::LOADFS, {"write", "keys", "data", "admin", "dangerous"}};
    // `function` as well as the rest: a .luau file in the directory becomes a
    // stored function, so this writes more than data
    r["LOADKEYS"] = {::LOADKEYS, {"write", "keys", "data", "function", "admin", "dangerous"}};
}
