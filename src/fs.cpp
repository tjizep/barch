#include "fs.h"

#include "constants.h"
#include "function_api.h"
#include "ids.h"
#include "lzr_log.h"
#include "staged.h"

#include <cctype>
#include <chrono>
#include <cstdio>
#include <unordered_map>
#include <cstdlib>

#ifdef BARCH_HAS_SIMDJSON
#include <simdjson.h>
#endif

namespace {

const char* NAMES = "fs:n:";
const char* INODES = "fs:i:";
const char* CHUNKS = "fs:c:";
const char* LAYOUT_KEY = "fs:layout";
const char* LAYOUT = "2";

std::string hex16(uint64_t v) {
    char buf[24];
    std::snprintf(buf, sizeof buf, "%016llx", (unsigned long long) v);
    return buf;
}

std::string hex8(uint64_t v) {
    char buf[16];
    std::snprintf(buf, sizeof buf, "%08llx", (unsigned long long) v);
    return buf;
}

/** the key just past everything starting with `prefix` */
std::string past(const std::string& prefix) {
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

std::string field_str(const std::string& raw, const char* name) {
#ifdef BARCH_HAS_SIMDJSON
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(raw).get(doc) != simdjson::SUCCESS)
        return {};
    std::string_view v;
    if (doc[name].get(v) == simdjson::SUCCESS)
        return std::string(v);
#else
    (void) raw; (void) name;
#endif
    return {};
}

bool parse_record(const std::string& raw, barch::fs::entry& into, bool& is_dir) {
    is_dir = false;
#ifdef BARCH_HAS_SIMDJSON
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(raw).get(doc) != simdjson::SUCCESS)
        return false;
    bool d = false;
    if (doc["dir"].get(d) == simdjson::SUCCESS && d) {
        is_dir = true;
        return true;
    }
    uint64_t n = 0;
    if (doc["id"].get(n) == simdjson::SUCCESS) into.id = n;
    if (doc["size"].get(n) == simdjson::SUCCESS) into.size = n;
    if (doc["chunk"].get(n) == simdjson::SUCCESS) into.chunk = n;
    if (doc["chunks"].get(n) == simdjson::SUCCESS) into.chunks = n;
    if (doc["version"].get(n) == simdjson::SUCCESS) into.version = n;
    if (doc["src"].get(n) == simdjson::SUCCESS) into.sourced = n != 0;
    std::string_view t;
    if (doc["type"].get(t) == simdjson::SUCCESS) into.type.assign(t);
    return true;
#else
    (void) raw; (void) into;
    return false;
#endif
}

std::string quoted(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\')
            out.push_back('\\');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::string name_json(const barch::fs::entry& e) {
    // `src` says the space's source can produce this again, which is what makes it
    // safe to evict. Absent means the only copy is this one - TODO 263
    return "{\"id\":" + std::to_string(e.id) +
           ",\"size\":" + std::to_string(e.size) +
           ",\"type\":" + quoted(e.type) +
           ",\"version\":" + std::to_string(e.version) +
           (e.sourced ? ",\"src\":1" : "") + "}";
}

std::string inode_json(const barch::fs::entry& e) {
    return "{\"size\":" + std::to_string(e.size) +
           ",\"type\":" + quoted(e.type) +
           ",\"chunk\":" + std::to_string(e.chunk) +
           ",\"chunks\":" + std::to_string(e.chunks) +
           ",\"version\":" + std::to_string(e.version) + "}";
}

std::string last_segment(const std::string& path) {
    auto at = path.rfind('/');
    return at == std::string::npos ? path : path.substr(at + 1);
}

}

namespace barch::fs {

std::string type_from_path(const std::string& path) {
    auto lower_copy = [](std::string v) {
        for (auto& ch : v)
            ch = (char) std::tolower((unsigned char) ch);
        return v;
    };
    auto dot = path.find_last_of('.');
    if (dot == std::string::npos)
        return "application/octet-stream";
    auto ext = lower_copy(path.substr(dot + 1));
    static const std::unordered_map<std::string, std::string> known = {
        {"html", "text/html"},   {"htm", "text/html"},    {"css", "text/css"},
        {"js", "text/javascript"}, {"mjs", "text/javascript"},
        {"json", "application/json"}, {"txt", "text/plain"},  {"csv", "text/csv"},
        {"xml", "application/xml"},  {"svg", "image/svg+xml"},
        {"png", "image/png"},    {"jpg", "image/jpeg"},   {"jpeg", "image/jpeg"},
        {"gif", "image/gif"},    {"webp", "image/webp"},  {"avif", "image/avif"},
        {"ico", "image/x-icon"}, {"woff", "font/woff"},   {"woff2", "font/woff2"},
        {"pdf", "application/pdf"}, {"wasm", "application/wasm"},
        {"mp4", "video/mp4"},    {"webm", "video/webm"},  {"mp3", "audio/mpeg"},
    };
    auto it = known.find(ext);
    return it == known.end() ? "application/octet-stream" : it->second;
}

std::string name_key(const std::string& path)          { return NAMES + path; }
std::string inode_key(file_id id)                      { return INODES + hex16(id); }
std::string chunk_key(file_id id, uint64_t n)          { return CHUNKS + hex16(id) + ":" + hex8(n); }

bool normalise(const std::string& in, std::string& out, std::string& err) {
    if (in.find('\0') != std::string::npos) {
        err = "a path cannot contain a NUL";
        return false;
    }
    std::vector<std::string> segs;
    size_t at = 0;
    while (at <= in.size()) {
        auto end = in.find('/', at);
        if (end == std::string::npos)
            end = in.size();
        auto seg = in.substr(at, end - at);
        at = end + 1;
        if (seg.empty() || seg == ".")
            continue;                       // "//" and "/./" are nothing
        if (seg == "..") {
            err = "a path cannot climb with ..";
            return false;
        }
        segs.push_back(std::move(seg));
    }
    out.clear();
    for (const auto& s : segs)
        out += "/" + s;
    if (out.empty())
        out = "/";
    return true;
}

bool stat(const access& acc, const std::string& path, entry& out) {
    std::string clean, err;
    if (!normalise(path, clean, err) || !acc.get)
        return false;
    std::string raw;
    bool dir = false;
    if (acc.get(name_key(clean), raw) == access::read_state::present) {
        if (!parse_record(raw, out, dir))
            return false;
        out.path = clean;
        out.name = last_segment(clean);
        out.dir = dir;
        return true;
    }
    // a directory marker, when one was written for it
    if (acc.get(name_key(clean) + "/", raw) == access::read_state::present) {
        out = entry{};
        out.path = clean;
        out.name = last_segment(clean);
        out.dir = true;
        return true;
    }
    return false;
}

bool stat_full(const access& acc, const std::string& path, entry& out) {
    if (!stat(acc, path, out))
        return false;
    if (out.dir || out.id == 0)
        return true;
    file f;
    std::string err;
    if (!file::open_id(acc, out.id, f, err))
        return true;                       // the name is there and the inode is not
    auto full = f.meta();
    out.chunk = full.chunk;
    out.chunks = full.chunks;
    return true;
}

bool resolve(const access& acc, const std::string& path, file_id& out) {
    entry e;
    if (!stat(acc, path, e) || e.dir || e.id == 0)
        return false;
    out = e.id;
    return true;
}

bool file::open(const access& acc, const std::string& path, file& out, std::string& err) {
    entry named;
    if (!stat(acc, path, named)) {
        err = "no such file";
        return false;
    }
    if (named.dir) {
        err = "that is a directory";
        return false;
    }
    return open_id(acc, named.id, out, err);
}

bool file::open_id(const access& acc, file_id id, file& out, std::string& err) {
    if (!acc.get) {
        err = "this key space cannot be read";
        return false;
    }
    std::string raw;
    if (acc.get(inode_key(id), raw) != access::read_state::present) {
        err = "no such file";
        return false;
    }
    entry e;
    bool dir = false;
    if (!parse_record(raw, e, dir)) {
        err = "unreadable file metadata";
        return false;
    }
    e.id = id;
    out.acc = acc;
    out.e = e;
    return true;
}

bool file::read_at(uint64_t at, uint64_t len, std::string& out, std::string& err) const {
    out.clear();
    if (at >= e.size || len == 0)
        return true;
    if (at + len > e.size)
        len = e.size - at;
    if (e.chunk == 0) {
        err = "the file has no chunk size";
        return false;
    }
    uint64_t first = at / e.chunk;
    uint64_t last = (at + len - 1) / e.chunk;
    out.reserve((size_t) len);
    for (uint64_t i = first; i <= last; ++i) {
        std::string part;
        if (!acc.get || acc.get(chunk_key(e.id, i), part) != access::read_state::present) {
            // the metadata promised a chunk that is not there: a file rewritten
            // underneath an open handle looks exactly like this
            err = "the file changed underneath";
            return false;
        }
        uint64_t start = i * e.chunk;
        uint64_t from = at > start ? at - start : 0;
        uint64_t upto = (at + len) < (start + part.size()) ? (at + len) - start : part.size();
        if (from < upto)
            out.append(part, (size_t) from, (size_t) (upto - from));
    }
    return true;
}

bool read(const access& acc, const std::string& path, std::string& out, entry& meta) {
    file f;
    std::string err;
    if (!file::open(acc, path, f, err))
        return false;
    meta = f.meta();
    return f.read_at(0, meta.size, out, err);
}

/*
 * Paging without a successor key.
 *
 * The obvious "next key after this one" is the key with a NUL appended, and it is
 * not available: keys go through `encode_key` on the way in and a NUL does not
 * survive that. So a page is re-ranged from the last key it saw, which comes back
 * again and is skipped by name. A child directory is different - there the jump is
 * to `name` followed by one past '/', which is the printable '0', so stepping over
 * a whole subtree needs no successor at all.
 */
constexpr size_t page = 256;

bool list(const access& acc, const std::string& dir, std::vector<entry>& out,
          const std::string& after, size_t limit) {
    std::string clean, err;
    if (!normalise(dir, clean, err) || !acc.range || !acc.get)
        return false;
    std::string prefix = name_key(clean == "/" ? std::string() : clean) + "/";
    std::string hi = past(prefix);
    std::string at = after.empty() ? prefix : prefix + after;
    std::string seen = after.empty() ? std::string() : at;

    while (limit == 0 || out.size() < limit) {
        heap::vector<std::string> got;
        acc.range(at, hi, (int64_t) page, got);
        if (got.empty())
            break;
        bool jumped = false;
        for (const auto& key : got) {
            if (key == seen)
                continue;                   // the key this page was re-ranged from
            if (key.compare(0, prefix.size(), prefix) != 0)
                return true;
            std::string rest = key.substr(prefix.size());
            if (rest.empty()) {
                // the directory's own marker sorts first inside its own prefix -
                // it says this directory exists, not that it contains something
                seen = key;
                at = key;
                continue;
            }
            auto slash = rest.find('/');
            entry e;
            if (slash == std::string::npos) {
                std::string raw;
                bool is_dir = false;
                if (acc.get(key, raw) == access::read_state::present)
                    parse_record(raw, e, is_dir);
                e.name = rest;
                e.path = (clean == "/" ? "" : clean) + "/" + rest;
                e.dir = is_dir;
                out.push_back(std::move(e));
                seen = key;
                at = key;
            } else {
                // a child directory: named once, and then stepped over entirely.
                // '/' is 0x2f, so everything under `name/` sorts below `name0`
                std::string child = rest.substr(0, slash);
                e.name = child;
                e.path = (clean == "/" ? "" : clean) + "/" + child;
                e.dir = true;
                out.push_back(std::move(e));
                at = prefix + child;
                at.push_back((char) ('/' + 1));
                seen.clear();
                jumped = true;
            }
            if (limit && out.size() >= limit)
                return true;
            if (jumped)
                break;
        }
        if (!jumped && got.size() < page)
            break;
    }
    return true;
}

bool list_with_source(const key_space_ptr& space, const std::string& dir,
                      std::vector<entry>& out, const std::string& after, size_t limit) {
    auto acc = barch::functions::store_for_owner(space);
    if (!list(acc, dir, out, after, limit))
        return false;
    if (!space || space->fs_source_list.empty())
        return true;

    std::string clean, err;
    if (!normalise(dir, clean, err))
        return true;                        // the local half is still an answer

    Variable answer;
    if (!barch::functions::call_named(space, space->fs_source_list, {clean}, answer, err)) {
        barch::err({"fs source listing", clean, err});
        return true;                        // what is stored is better than nothing
    }
    if (answer.index() != var_array)
        return true;                        // no names is a legitimate answer

    heap::string_set already;
    for (const auto& e : out)
        already.insert(e.name);

    /*
     * The source's half is sorted and paged the same way the stored half was, and
     * the limit is applied to the merge rather than to each side. Taking the first
     * `limit` names as the source happened to give them would make a page whose
     * contents depended on which half an entry came from.
     */
    std::vector<std::string> names;
    for (const auto& item : std::get<heap::vector<wrapped_variable_t>>(answer)) {
        const Variable& v = static_cast<const variable_t&>(item);
        if (v.index() != var_string)
            continue;
        auto name = v.to_string();
        if (name.empty() || name.find('/') != std::string::npos)
            continue;                       // a name, not a path: one level at a time
        if (!after.empty() && name <= after)
            continue;                       // the page starts after this
        if (already.find(name) != already.end())
            continue;                       // stored already, and what is stored wins
        names.push_back(std::move(name));
    }
    std::sort(names.begin(), names.end());

    const std::string base = (clean == "/" ? std::string() : clean) + "/";
    for (const auto& name : names) {
        entry e;
        e.name = name;
        e.path = base + name;
        e.remote = true;
        out.push_back(std::move(e));
    }
    // one order whichever half an entry came from, and then the page
    std::sort(out.begin(), out.end(),
              [](const entry& a, const entry& b) { return a.name < b.name; });
    if (limit && out.size() > limit)
        out.resize(limit);
    return true;
}

bool walk(const access& acc, const std::string& dir, std::vector<std::string>& out) {
    std::string clean, err;
    if (!normalise(dir, clean, err) || !acc.range)
        return false;
    std::string prefix = name_key(clean == "/" ? std::string() : clean) + "/";
    std::string hi = past(prefix);
    std::string at = prefix;
    std::string seen;
    for (;;) {
        heap::vector<std::string> got;
        acc.range(at, hi, (int64_t) page, got);
        if (got.empty())
            break;
        size_t took = 0;
        for (const auto& key : got) {
            if (key == seen)
                continue;
            ++took;
            if (key.size() > prefix.size() && key.back() != '/')
                out.push_back(key.substr(5));      // past "fs:n:", files only
            seen = key;
        }
        at = got.back();
        if (took == 0 || got.size() < page)
            break;
    }
    return true;
}

batch::batch(const key_space_ptr& space) : space(space) {
}

void batch::write(const std::string& path, std::string body, const std::string& type,
                  size_t chunk) {
    item i;
    i.path = path;
    staged_bytes += body.size();
    i.body = std::move(body);
    i.type = type;
    i.chunk = chunk ? chunk : default_chunk;
    pending.push_back(std::move(i));
}

void batch::write_sourced(const std::string& path, std::string body,
                          const std::string& type, size_t chunk) {
    write(path, std::move(body), type, chunk);
    pending.back().sourced = true;
}

void batch::mkdir(const std::string& path) {
    item i;
    i.path = path;
    i.mkdir = true;
    pending.push_back(std::move(i));
}

void batch::relink(const std::string& from, const std::string& to) {
    item i;
    i.path = from;
    i.body = to;                            // the destination rides in the body slot
    i.relink = true;
    pending.push_back(std::move(i));
}

void batch::erase(const std::string& path) {
    item i;
    i.path = path;
    i.erase = true;
    pending.push_back(std::move(i));
}

bool batch::commit(std::string& err) {
    done.clear();
    if (pending.empty())
        return true;
    if (!space) {
        err = "no key space";
        return false;
    }
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.get || !acc.set) {
        err = "this key space cannot be written";
        return false;
    }

    // what is there now, and how many ids this needs
    struct plan {
        entry now;
        bool had{false};
    };
    std::vector<plan> before(pending.size());
    uint64_t wanted = 0;
    for (size_t i = 0; i < pending.size(); ++i) {
        std::string clean;
        if (!normalise(pending[i].path, clean, err))
            return false;
        pending[i].path = clean;
        before[i].had = stat(acc, clean, before[i].now);
        /*
         * A path is one thing or the other. Without this a file could be written
         * over a directory that still has files under it, and they would go on
         * existing with nothing above them able to name them.
         */
        if (!pending[i].erase && !pending[i].relink) {
            bool want_dir = pending[i].mkdir;
            bool is_dir = (before[i].had && before[i].now.dir) || has_children(acc, clean);
            if (want_dir && before[i].had && !before[i].now.dir) {
                err = clean + " is a file";
                return false;
            }
            if (!want_dir && is_dir) {
                err = clean + " is a directory";
                return false;
            }
        }
        if (!pending[i].erase && !pending[i].mkdir && !pending[i].relink
            && (!before[i].had || before[i].now.id == 0))
            ++wanted;
    }

    /*
     * One reservation for the whole batch, taken before a single latch is held -
     * the counter lives on another shard and asking for it mid write is how a lock
     * order inversion gets built. See ids.h.
     */
    uint64_t next_id = 0;
    if (wanted && !reserve_ids(space, "fs", wanted, next_id, err))
        return false;

    barch::staged ops(space);
    ops.set(LAYOUT_KEY, LAYOUT);
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& it = pending[i];
        if (it.erase) {
            if (!before[i].had)
                continue;
            if (before[i].now.dir) {
                ops.remove(name_key(it.path) + "/");
                continue;
            }
            // the inode has the chunk count, and the name record does not
            entry full;
            uint64_t chunks = stat_full(acc, it.path, full) ? full.chunks : 0;
            for (uint64_t n = 0; n < chunks; ++n)
                ops.remove(chunk_key(before[i].now.id, n));
            ops.remove(inode_key(before[i].now.id));
            ops.remove(name_key(it.path));
            continue;
        }
        if (it.relink) {
            if (!before[i].had)
                continue;
            std::string to;
            if (!normalise(it.body, to, err))
                return false;
            const char* tail = before[i].now.dir ? "/" : "";
            std::string from_key = name_key(it.path) + tail;
            std::string raw;
            if (acc.get(from_key, raw) != access::read_state::present)
                continue;
            ops.set(name_key(to) + tail, raw);
            ops.remove(from_key);
            done.push_back(to);
            continue;
        }
        if (it.mkdir) {
            ops.set(name_key(it.path) + "/", "{\"dir\":true}");
            done.push_back(it.path);
            continue;
        }
        entry e;
        e.id = (before[i].had && before[i].now.id) ? before[i].now.id : next_id++;
        e.path = it.path;
        e.name = last_segment(it.path);
        e.type = it.type;
        e.size = it.body.size();
        e.chunk = it.chunk;
        e.chunks = it.chunk ? (it.body.size() + it.chunk - 1) / it.chunk : 0;
        e.version = before[i].had ? before[i].now.version + 1 : 1;
        e.sourced = it.sourced;

        // chunks, then the inode, then the name: nothing reaches a chunk except
        // through metadata, so a half applied write is invisible rather than wrong
        for (uint64_t n = 0; n < e.chunks; ++n)
            ops.set(chunk_key(e.id, n), it.body.substr((size_t) (n * e.chunk), e.chunk));
        // an overwrite that shrank leaves the tail behind otherwise. The count is
        // the inode's, since the name record does not carry one
        if (before[i].had) {
            entry was;
            if (stat_full(acc, it.path, was)) {
                for (uint64_t n = e.chunks; n < was.chunks; ++n)
                    ops.remove(chunk_key(e.id, n));
            }
        }
        ops.set(inode_key(e.id), inode_json(e));
        ops.set(name_key(it.path), name_json(e));
        done.push_back(it.path);
    }
    if (!ops.commit(err))
        return false;
    pending.clear();
    return true;
}

/** everything stored under a path - files and directory markers alike, as paths */
static bool walk_entries(const access& acc, const std::string& dir,
                         std::vector<std::string>& out) {
    std::string clean, err;
    if (!normalise(dir, clean, err) || !acc.range)
        return false;
    std::string prefix = name_key(clean == "/" ? std::string() : clean) + "/";
    std::string hi = past(prefix);
    std::string at = prefix;
    std::string seen;
    for (;;) {
        heap::vector<std::string> got;
        acc.range(at, hi, (int64_t) page, got);
        if (got.empty())
            break;
        size_t took = 0;
        for (const auto& key : got) {
            if (key == seen)
                continue;
            ++took;
            seen = key;
            if (key.size() == prefix.size())
                continue;                              // the directory's own marker
            std::string path = key.substr(5);          // past "fs:n:"
            if (!path.empty() && path.back() == '/')
                path.pop_back();                       // a marker names its directory
            out.push_back(std::move(path));
        }
        at = got.back();
        if (took == 0 || got.size() < page)
            break;
    }
    return true;
}

/*
 * The eviction index and the running total - TODO 263.
 *
 * `fs:lru:<016x fetched at>:<path>` orders the fetched files by when they arrived,
 * so finding the oldest is a seek from the low end rather than a walk of every
 * file. `fs:cache` is what they add up to, kept as a number so a budget check does
 * not have to add them up.
 *
 * The index is only an ordering. Whether a file may actually go is the `src` mark
 * on its own name record, checked when it is about to be evicted - an entry can
 * outlive the file it points at, and a file that was fetched and then written over
 * by hand is nobody's copy but ours.
 */
static std::string lru_key(uint64_t when, const std::string& path) {
    char stamp[24];
    std::snprintf(stamp, sizeof stamp, "%016llx", (unsigned long long) when);
    return std::string("fs:lru:") + stamp + ":" + path;
}

static const char* CACHE_BYTES = "fs:cache";

static uint64_t read_counter(const access& acc, const char* key) {
    std::string raw;
    if (!acc.get || acc.get(key, raw) != access::read_state::present)
        return 0;
    return strtoull(raw.c_str(), nullptr, 10);
}

uint64_t cached_bytes(const key_space_ptr& space) {
    if (!space)
        return 0;
    auto acc = barch::functions::store_for_owner(space);
    return read_counter(acc, CACHE_BYTES);
}

size_t evict_to_budget(const key_space_ptr& space) {
    if (!space || space->fs_cache_bytes == 0)
        return 0;
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.range || !acc.get || !acc.set)
        return 0;
    uint64_t held = read_counter(acc, CACHE_BYTES);
    if (held <= space->fs_cache_bytes)
        return 0;

    const std::string lo = "fs:lru:";
    const std::string hi = past(lo);
    size_t gone = 0;
    std::string at = lo;
    std::string seen;
    while (held > space->fs_cache_bytes) {
        heap::vector<std::string> got;
        acc.range(at, hi, (int64_t) page, got);
        bool moved = false;
        for (const auto& key : got) {
            if (key == seen)
                continue;
            seen = key;
            at = key;
            moved = true;
            // fs:lru:<16 hex>:<path>
            if (key.size() < lo.size() + 17)
                continue;
            std::string path = key.substr(lo.size() + 17);
            entry e;
            const bool have = stat(acc, path, e);
            if (!have || !e.sourced) {
                // gone already, or written over by hand since - either way this
                // index entry is stale and the file is not ours to drop
                acc.remove(key);
                continue;
            }
            batch b(space);
            b.erase(path);
            std::string err;
            if (!b.commit(err)) {
                barch::err({"fs eviction", path, err});
                continue;
            }
            acc.remove(key);
            held = e.size < held ? held - e.size : 0;
            ++gone;
            if (held <= space->fs_cache_bytes)
                break;
        }
        if (!moved || got.empty())
            break;
    }
    std::string err;
    acc.set(CACHE_BYTES, std::to_string(held), err);
    return gone;
}

bool has_source(const key_space_ptr& space) {
    return space && !space->fs_source.empty();
}

/*
 * A miss is a key of its own rather than anything in the file layout: a listing
 * scans `fs:n:` and never sees these, and an expiry is what `missing_ttl` already
 * means everywhere else in the server.
 */
static std::string miss_key(const std::string& path) {
    return "fs:miss:" + path;
}

/*
 * What a fetch has to do besides writing the file: mark it, index it by when it
 * arrived, add it to the running total, and then drop the oldest if that put the
 * space over its budget. See TODO 263.
 */
static bool store_fetched(const key_space_ptr& space, const std::string& clean,
                          const std::string& body, const std::string& type,
                          entry& out, std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    batch b(space);
    b.write_sourced(clean, body, type);
    if (!b.commit(err))
        return false;
    if (space->fs_cache_bytes) {
        const auto now = (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        std::string e;
        acc.set(lru_key(now, clean), clean, e);
        acc.set(CACHE_BYTES, std::to_string(read_counter(acc, CACHE_BYTES) + body.size()), e);
        evict_to_budget(space);
    }
    return stat(acc, clean, out);
}

bool fetch(const key_space_ptr& space, const std::string& path, entry& out,
           std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    if (stat(acc, path, out))
        return true;
    if (!has_source(space))
        return false;

    std::string clean;
    if (!normalise(path, clean, err))
        return false;

    const uint64_t ttl = space->missing_ttl;
    const auto now = (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string when;
    if (ttl && acc.get && acc.get(miss_key(clean), when) == access::read_state::present) {
        auto at = strtoull(when.c_str(), nullptr, 10);
        if (at && now - at < ttl) {
            err = "no such file";               // asked recently and it was not there
            return false;
        }
    }

    Variable answer;
    if (!barch::functions::call_named(space, space->fs_source, {clean}, answer, err))
        return false;

    /*
     * Two shapes, because a path is not always enough to say what a file is. A
     * string is the body, and the type is guessed from the extension - which
     * `/img/<asin>` has none of, and then everything is octet-stream. A list of two
     * is `{body, type}`, and the source says what it made.
     *
     * A list and not `{body = ..., type = ...}`: a table's string keys do not
     * survive the return, only its array part does, and that shape is fixed on
     * purpose - see to_variable in luau_driver.cpp.
     */
    if (answer.index() == var_array) {
        const auto& items = std::get<heap::vector<wrapped_variable_t>>(answer);
        if (!items.empty()) {
            const Variable& first = static_cast<const variable_t&>(items[0]);
            if (first.index() == var_string) {
                std::string type_str;
                if (items.size() > 1) {
                    const Variable& second = static_cast<const variable_t&>(items[1]);
                    if (second.index() == var_string)
                        type_str = second.to_string();
                }
                return store_fetched(space, clean, first.to_string(),
                                     type_str.empty() ? type_from_path(clean) : type_str,
                                     out, err);
            }
        }
    }
    if (answer.index() != var_string) {
        // nil is the source saying it has no such file, which is an answer
        std::string e;
        if (ttl && acc.set)
            acc.set(miss_key(clean), std::to_string(now), e);
        err = "no such file";
        return false;
    }
    const auto& raw = std::get<std::string>(answer);
    auto body = answer.bulk_vt(raw);

    return store_fetched(space, clean, std::string(body.chars(), body.size),
                         type_from_path(clean), out, err);
}

bool has_children(const access& acc, const std::string& path) {
    std::string clean, e;
    if (!normalise(path, clean, e) || !acc.range)
        return false;
    std::string prefix = name_key(clean == "/" ? std::string() : clean) + "/";
    heap::vector<std::string> got;
    // two, because the first may be the directory's own marker, which says it
    // exists rather than that anything is in it
    acc.range(prefix, past(prefix), 2, got);
    for (const auto& k : got) {
        if (k.size() != prefix.size())
            return true;
    }
    return false;
}

bool mkdir(const key_space_ptr& space, const std::string& path, std::string& err) {
    batch b(space);
    b.mkdir(path);
    return b.commit(err);
}

bool rmdir(const key_space_ptr& space, const std::string& path, bool recursive,
           std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    std::string clean;
    if (!normalise(path, clean, err))
        return false;
    entry e;
    bool marked = stat(acc, clean, e) && e.dir;
    // markers as well as files: a nested empty directory is a key too, and a
    // recursive remove that left them behind would leave directories with nothing
    // above them able to name them
    std::vector<std::string> under;
    walk_entries(acc, clean, under);
    if (!recursive) {
        if (!under.empty()) {
            err = clean + " is not empty";
            return false;
        }
        if (!marked) {
            err = "no such directory";
            return false;
        }
        batch b(space);
        b.erase(clean);
        return b.commit(err);
    }
    if (!marked && under.empty()) {
        err = "no such directory";
        return false;
    }
    batch b(space);
    for (const auto& p : under)
        b.erase(p);
    if (marked)
        b.erase(clean);
    return b.commit(err);
}

/** `under` translated so that `from` becomes `to` */
static std::string moved_path(const std::string& under, const std::string& from,
                              const std::string& to) {
    return to + under.substr(from.size());
}

/** what a move or a copy has to refuse before it starts */
static bool movable(const access& acc, const std::string& from, const std::string& to,
                    bool& from_is_dir, std::string& err) {
    if (from == to) {
        err = "the source and the destination are the same";
        return false;
    }
    if (to.compare(0, from.size(), from) == 0 && to.size() > from.size()
        && to[from.size()] == '/') {
        err = "cannot move a directory into itself";
        return false;
    }
    entry src;
    bool marked = stat(acc, from, src) && src.dir;
    from_is_dir = marked || has_children(acc, from);
    if (!marked && !from_is_dir && !stat(acc, from, src)) {
        err = "no such file or directory";
        return false;
    }
    entry dst;
    if (stat(acc, to, dst) || has_children(acc, to)) {
        err = to + " exists";
        return false;
    }
    return true;
}

bool rename(const key_space_ptr& space, const std::string& raw_from,
            const std::string& raw_to, std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    std::string from, to;
    if (!normalise(raw_from, from, err) || !normalise(raw_to, to, err))
        return false;
    bool is_dir = false;
    if (!movable(acc, from, to, is_dir, err))
        return false;

    batch b(space);
    if (!is_dir) {
        b.relink(from, to);
        return b.commit(err);
    }
    std::vector<std::string> under;
    walk_entries(acc, from, under);
    for (const auto& p : under)
        b.relink(p, moved_path(p, from, to));
    entry marker;
    if (stat(acc, from, marker) && marker.dir)
        b.relink(from, to);                 // the directory's own marker, if it has one
    return b.commit(err);
}

bool copy(const key_space_ptr& space, const std::string& raw_from,
          const std::string& raw_to, std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    std::string from, to;
    if (!normalise(raw_from, from, err) || !normalise(raw_to, to, err))
        return false;
    bool is_dir = false;
    if (!movable(acc, from, to, is_dir, err))
        return false;

    batch b(space);
    auto copy_one = [&](const std::string& src, const std::string& dst) -> bool {
        std::string body;
        entry meta;
        if (!read(acc, src, body, meta)) {
            err = "could not read " + src;
            return false;
        }
        b.write(dst, std::move(body), meta.type, meta.chunk);
        return true;
    };
    if (!is_dir)
        return copy_one(from, to) && b.commit(err);

    std::vector<std::string> under;
    walk_entries(acc, from, under);
    for (const auto& p : under) {
        entry e;
        if (stat(acc, p, e) && e.dir)
            b.mkdir(moved_path(p, from, to));
        else if (!copy_one(p, moved_path(p, from, to)))
            return false;
    }
    entry marker;
    if (stat(acc, from, marker) && marker.dir)
        b.mkdir(to);
    return b.commit(err);
}

bool erase(const key_space_ptr& space, const std::string& path, std::string& err) {
    auto acc = barch::functions::store_for_owner(space);
    entry e;
    if (!stat(acc, path, e))
        return false;
    batch b(space);
    b.erase(path);
    return b.commit(err);
}

size_t drop_missing(const key_space_ptr& space, const std::string& root,
                    const std::vector<std::string>& keep) {
    auto acc = barch::functions::store_for_owner(space);
    std::vector<std::string> have;
    if (!walk(acc, root, have))
        return 0;
    heap::string_set wanted;
    for (const auto& k : keep) {
        std::string clean, err;
        if (normalise(k, clean, err))
            wanted.insert(clean);
    }
    batch b(space);
    size_t gone = 0;
    for (const auto& path : have) {
        if (wanted.find(path) != wanted.end())
            continue;
        b.erase(path);
        ++gone;
    }
    std::string err;
    if (gone && !b.commit(err)) {
        barch::err({"fs drop_missing", err});
        return 0;
    }
    return gone;
}

}
