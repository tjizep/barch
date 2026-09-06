//
// LOADFS - import a directory into the key space as the chunked file store.
//
// The layout itself now lives in fs.h - see TODO 256. This file is the commands and
// the directory walks that feed them: LOADFS and LOADKEYS, and the boot equivalents
// barchd calls. What a stored file *is* is not decided here any more.
//
// The whole import lands or none of it does. That is done by reading everything
// first and handing it to one `fs::batch`, which is a `staged` underneath - so the
// cost of the guarantee is holding the import in memory while it is in flight.
// TODO 239 is the way out of that ceiling:
// hash_arena backing its pages with a named memory mapped file instead of anonymous
// memory, so a set of files larger than RAM can be held.
//
// See TODO 238.
//
#include "fs_api.h"

#include "fs.h"
#include "staged.h"

#include <cstdio>
#include <string>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>

#include "caller.h"
#include "configuration.h"
#include <set>

#ifdef BARCH_HAS_SIMDJSON
#include <simdjson.h>
#endif

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

/** the optional trailing RELOAD, the same word the writes take - see TODO 245 */
bool takes_reload(const arg_t& argv, size_t& used) {
    if (argv.size() <= used)
        return false;
    std::string word(argv[used].chars(), argv[used].size);
    for (auto& c : word)
        c = (char) toupper((unsigned char) c);
    if (word != "RELOAD")
        return false;
    --used;                     // it is not one of the positional arguments
    return true;
}

/** one file the walk found, held until the whole directory has been read */
struct found {
    std::string path;
    std::string type;
    std::string content;
};

/** walk `dir`, collecting the files under it */
bool gather(const std::string& dir, const std::string& at,
            std::vector<found>& into, size_t& files, uint64_t& bytes, std::string& err) {
    for (const auto& name : list_dir(dir)) {
        if (skipped_name(name))
            continue;
        std::string path = dir + "/" + name;
        // always rooted: `at` is "" for the root, so this is "/name" there and
        // "/sub/name" below it
        std::string stored = at + "/" + name;
        if (is_dir(path)) {
            if (!gather(path, stored, into, files, bytes, err))
                return false;
            continue;
        }
        if (!is_reg(path))
            continue;                       // a socket or a device is not a file here
        found f;
        f.path = stored;
        f.type = type_of(name);
        if (!read_file(path, f.content)) {
            err = "could not read " + path;
            return false;
        }
        bytes += f.content.size();
        ++files;
        into.push_back(std::move(f));
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
                                     std::vector<std::string>& reply, bool publish,
                                     std::vector<std::string>* imported) {
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

    std::vector<found> files_found;
    size_t files = 0;
    uint64_t bytes = 0;
    std::string err;
    // everything is read before a single key is written, so a directory that cannot
    // be read does not leave half an import behind
    if (!gather(dir, root == "/" ? std::string() : root, files_found, files, bytes, err))
        return err;
    if (files_found.empty())
        return "nothing to import";

    barch::fs::batch batch(space);
    for (auto& f : files_found)
        batch.write(f.path, std::move(f.content), f.type, chunk);
    std::string failed;
    if (!batch.commit(failed))
        return failed;

    if (publish) {
        // the paths this import wrote, so a module compiled from one of them is
        // rebuilt and everything else staged stays staged - TODO 245
        for (const auto& path : batch.written())
            barch::functions::publish_compiled(
                barch::functions::compiled_path_key(space->canonical(), path));
    }
    if (imported)
        imported->insert(imported->end(), batch.written().begin(), batch.written().end());
    reply.push_back("files=" + std::to_string(files));
    reply.push_back("bytes=" + std::to_string(bytes));
    reply.push_back("keys=" + std::to_string(batch.written().size()));
    reply.push_back("root=" + root);
    reply.push_back("chunk=" + std::to_string(chunk));
    return {};
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
                                       std::vector<std::string>& reply, bool publish) {
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

    /*
     * One staged write for the lot - TODO 255. The dependency order a `.luau` that
     * requires another needs is `staged`'s retry rather than anything here: this
     * only has to say what should end up in the space.
     */
    barch::staged batch(space);
    size_t functions = 0, keys = 0;
    uint64_t bytes = 0;
    for (const auto& f : files) {
        if (f.luau) {
            batch.set_function(f.name, f.source);
            ++functions;
        } else {
            batch.set(f.name, f.source);
            ++keys;
        }
        bytes += f.source.size();
    }
    std::string failed;
    if (!batch.commit(failed))
        return failed;

    if (publish) {
        for (const auto& f : files) {
            if (f.luau)
                barch::functions::publish_compiled(
                    barch::functions::compiled_key(space->canonical(), f.name));
        }
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

/**
 * The whole content of a stored file. False when there is no such file, or when the
 * metadata cannot be read. Shared so that everything reading the file store - the
 * HTTP routes, `require`, whatever comes next - agrees about the layout.
 *
 * The read goes through the store_access it is handed, so a caller with no rights in
 * the space gets nothing. That is the whole of the access control: files are keys.
 */
bool barch::read_fs_file(const barch::foreign::store_access& acc, const std::string& path,
                         std::string& out, std::string& type) {
    barch::fs::entry meta;
    if (!barch::fs::read(acc, path, out, meta))
        return false;
    type = meta.type;
    return true;
}

uint64_t barch::fs_file_version(const barch::foreign::store_access& acc,
                                const std::string& path) {
    barch::fs::entry e;
    if (!barch::fs::stat(acc, path, e))
        return 0;
    return e.version;
}

size_t barch::drop_fs_missing(const barch::key_space_ptr& space, const std::string& root,
                              const std::vector<std::string>& keep) {
    return barch::fs::drop_missing(space, root, keep);
}

int LOADKEYS(caller& call, const arg_t& argv) {
    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();
    size_t last = argv.size() - 1;
    const bool reload = takes_reload(argv, last);
    const size_t positional = reload ? argv.size() - 1 : argv.size();
    if (positional > 3)
        return call.push_error("LOADKEYS path [prefix] [RELOAD]");
    std::string dir = as_text(argv[1]);
    std::string prefix = positional > 2 ? as_text(argv[2]) : std::string();
    std::vector<std::string> reply;
    auto err = barch::load_keys_directory(dir, prefix, call.kspace(), reply, reload);
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
    if (argv.size() < 2 || argv.size() > 5)
        return call.wrong_arity();
    size_t last = argv.size() - 1;
    const bool reload = takes_reload(argv, last);
    const size_t positional = reload ? argv.size() - 1 : argv.size();
    if (positional > 4)
        return call.push_error("LOADFS path [root] [chunk] [RELOAD]");
    std::string dir = as_text(argv[1]);
    std::string root = positional > 2 ? as_text(argv[2]) : std::string("/");
    size_t chunk = default_chunk;
    if (positional > 3)
        chunk = (size_t) strtoull(as_text(argv[3]).c_str(), nullptr, 10);
    std::vector<std::string> reply;
    auto err = barch::load_fs_directory(dir, root, chunk, call.kspace(), reply, reload);
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

/*
 * FS - the file store over RESP, TODO 254 and 256.
 *
 *   FS LS <path> [AFTER name] [LIMIT n]     a line per entry, one level
 *   FS STAT <path>                          k=v, the way FUNCTIONS STATUS reads
 *   FS GET <path> [FROM off] [LEN n]        the content, or nil
 *   FS PUT <path> <content> [TYPE t] [CHUNK n]
 *   FS MV <path> <to>                       a file or a whole directory
 *   FS CP <path> <to>
 *   FS RM <path>                            1, or 0 when there was nothing
 *   FS MKDIR <path>
 *   FS RMDIR <path> [RECURSIVE]
 *
 * Flat replies on purpose. A nested array would be the obvious shape for LS, and
 * the module caller cannot send one: `vk_caller` counts a single array with one
 * counter, so an inner end_array sets the outer's length. A line per row is what
 * FUNCTIONS COMMANDS already does for the same reason.
 */
namespace {

/** an optional NAME VALUE pair after the positional arguments */
bool option_at(const arg_t& argv, size_t at, const char* name, std::string& value) {
    if (at + 1 >= argv.size())
        return false;
    std::string word = as_text(argv[at]);
    for (auto& c : word)
        c = (char) toupper((unsigned char) c);
    if (word != name)
        return false;
    value = as_text(argv[at + 1]);
    return true;
}

std::string ls_line(const barch::fs::entry& e) {
    // three fields and then the name, which is the only one that can hold a space
    return std::string(e.dir ? "dir" : "file") + " " + std::to_string(e.size) + " " +
           std::to_string(e.version) + " " + e.name;
}

}

int FS(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    std::string sub = as_text(argv[1]);
    for (auto& c : sub)
        c = (char) toupper((unsigned char) c);
    std::string path = as_text(argv[2]);
    auto space = call.kspace();
    auto acc = barch::functions::store_for_owner(space);

    if (sub == "LS") {
        std::string after, limit;
        for (size_t at = 3; at + 1 < argv.size(); at += 2) {
            if (!option_at(argv, at, "AFTER", after) && !option_at(argv, at, "LIMIT", limit))
                return call.push_error("FS LS path [AFTER name] [LIMIT n]");
        }
        std::vector<barch::fs::entry> got;
        if (!barch::fs::list(acc, path, got, after,
                             limit.empty() ? 0 : (size_t) strtoull(limit.c_str(), nullptr, 10)))
            return call.push_error("not a path");
        call.start_array();
        for (const auto& e : got)
            call.push_string(ls_line(e));
        return call.end_array();
    }
    if (sub == "STAT") {
        barch::fs::entry e;
        if (!barch::fs::stat_full(acc, path, e))
            return call.push_null();
        std::string line = "path=" + e.path +
                           " kind=" + (e.dir ? "dir" : "file");
        if (!e.dir) {
            line += " size=" + std::to_string(e.size) +
                    " chunk=" + std::to_string(e.chunk) +
                    " chunks=" + std::to_string(e.chunks) +
                    " version=" + std::to_string(e.version) +
                    " type=" + e.type;
        }
        return call.push_string(line);
    }
    if (sub == "GET") {
        std::string from, len;
        for (size_t at = 3; at + 1 < argv.size(); at += 2) {
            if (!option_at(argv, at, "FROM", from) && !option_at(argv, at, "LEN", len))
                return call.push_error("FS GET path [FROM off] [LEN n]");
        }
        barch::fs::file f;
        std::string err;
        if (!barch::fs::file::open(acc, path, f, err))
            return call.push_null();
        uint64_t at_off = from.empty() ? 0 : strtoull(from.c_str(), nullptr, 10);
        uint64_t want = len.empty() ? f.meta().size : strtoull(len.c_str(), nullptr, 10);
        std::string body;
        if (!f.read_at(at_off, want, body, err))
            return call.push_error(err.c_str());
        return call.push_string(body);
    }
    if (sub == "PUT") {
        if (argv.size() < 4)
            return call.wrong_arity();
        std::string type, chunk;
        for (size_t at = 4; at + 1 < argv.size(); at += 2) {
            if (!option_at(argv, at, "TYPE", type) && !option_at(argv, at, "CHUNK", chunk))
                return call.push_error("FS PUT path content [TYPE t] [CHUNK n]");
        }
        barch::fs::batch b(space);
        b.write(path, as_text(argv[3]), type,
                chunk.empty() ? 0 : (size_t) strtoull(chunk.c_str(), nullptr, 10));
        std::string err;
        if (!b.commit(err))
            return call.push_error(err.c_str());
        barch::fs::entry e;
        return call.push_int(barch::fs::stat_full(acc, path, e) ? (int64_t) e.chunks : 0);
    }
    if (sub == "MV" || sub == "CP") {
        if (argv.size() != 4)
            return call.wrong_arity();
        std::string to = as_text(argv[3]);
        std::string err;
        bool ok = sub == "MV" ? barch::fs::rename(space, path, to, err)
                              : barch::fs::copy(space, path, to, err);
        if (!ok)
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "RM") {
        std::string err;
        return call.push_int(barch::fs::erase(space, path, err) ? 1 : 0);
    }
    if (sub == "MKDIR") {
        std::string err;
        if (!barch::fs::mkdir(space, path, err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "RMDIR") {
        bool recursive = false;
        if (argv.size() == 4) {
            std::string word = as_text(argv[3]);
            for (auto& c : word)
                c = (char) toupper((unsigned char) c);
            if (word != "RECURSIVE")
                return call.push_error("FS RMDIR path [RECURSIVE]");
            recursive = true;
        } else if (argv.size() > 4) {
            return call.wrong_arity();
        }
        std::string err;
        if (!barch::fs::rmdir(space, path, recursive, err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    return call.push_error("FS LS|STAT|GET|PUT|RM|MV|CP|MKDIR|RMDIR");
}

int cmd_FS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, FS);
}

void register_fs_api(function_map& r) {
    // one command for both halves, so the categories are the widest of what it can
    // do - a read only caller cannot have FS at all. Splitting FSGET from FSPUT is
    // the way out of that if it turns out to matter
    r["FS"] = {::FS, {"read", "write", "keys", "data"}};
    r["LOADFS"] = {::LOADFS, {"write", "keys", "data", "admin", "dangerous"}};
    // `function` as well as the rest: a .luau file in the directory becomes a
    // stored function, so this writes more than data
    r["LOADKEYS"] = {::LOADKEYS, {"write", "keys", "data", "function", "admin", "dangerous"}};
}
