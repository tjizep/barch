// The graph store: nodes joined by edges, as ids rather than as paths.
// See TODO 382, TODO 407 and graph.h.
//
// FS stores a path literally in the key name, so a file can only live at one
// path. Here the edge table maps (parent, name) to a node id and the node
// table says what each node is, so one node answers at any number of paths.
// Leaf bytes reuse the FS inode/chunk keys verbatim - graph.cpp stages chunk
// and inode keys in the exact JSON `fs::file::open_id` parses, so FS reads a
// graph leaf without knowing it is one. Only the node and edge tables and
// their two indexes are new.
//
// Ordering inside one commit follows the FS batch: chunks, then the inode,
// then the node, then the edges. Nothing reaches a chunk except through the
// node, and nothing reaches a node except through an edge, so a half applied
// write is invisible rather than wrong. Deletes go the other way: edges,
// then the node, then its payload.
//
// Node and edge ids come from one `ids.h` sequence ("graph"), so the two can
// never collide. Leaf inode ids come from FS's sequence ("fs"), because the
// inode keys are FS's and FS files take their ids from there too - two
// counters handing out ids for one set of keys is how a GRAPH PUT and an FS
// PUT ended up writing each other's bytes (TODO 407). Both are reserved once,
// before any latch, the way fs::batch does: a counter lives on whatever shard
// its key hashes to, and reserving mid-write is a lock order inversion.
//
#include "graph.h"

#include "art/art.h"
#include "conversion.h"
#include "fs.h"
#include "function_api.h"
#include "ids.h"
#include "key_range.h"
#include "keys.h"
#include "lzr_log.h"
#include "sharded_store.h"
#include "staged.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace graph_anon {

const char* NODES = "graph:n:";
const char* EDGES = "graph:e:";
const char* REV = "graph:r:";
const char* NAME_INDEX = "graph:x:";
const char* LAYOUT_KEY = "graph:layout";
const char* LAYOUT = "3";
const char* UNINDEXED_LAYOUT = "2";
const char* SEQ = "graph";
const char* INODE_SEQ = "fs";

std::string ghex16(uint64_t v) {
    char buf[24];
    std::snprintf(buf, sizeof buf, "%016llx", (unsigned long long) v);
    return buf;
}

uint64_t gunhex(const std::string& s) {
    uint64_t v = 0;
    for (char c : s) {
        v <<= 4;
        if (c >= '0' && c <= '9') v |= (uint64_t) (c - '0');
        else if (c >= 'a' && c <= 'f') v |= (uint64_t) (c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') v |= (uint64_t) (c - 'A' + 10);
        else return UINT64_MAX;
    }
    return v;
}

/**
 * FNV-1a over a name, for the name index. It ends up in stored keys, so it has
 * to come out the same on every build and every platform, which std::hash
 * doesn't promise.
 */
uint64_t gname_hash(const std::string& s) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ull;
    }
    return h;
}

std::string gquoted(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\')
            out.push_back('\\');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

/** the raw value after `"key":`, or empty when the key is absent. */
std::string field_raw(const std::string& doc, const char* key) {
    std::string want = std::string("\"") + key + "\":";
    auto at = doc.find(want);
    if (at == std::string::npos)
        return {};
    return doc.substr(at + want.size());
}

bool field_u64(const std::string& doc, const char* key, uint64_t& out) {
    std::string raw = field_raw(doc, key);
    if (raw.empty())
        return false;
    char* end = nullptr;
    unsigned long long v = std::strtoull(raw.c_str(), &end, 10);
    if (end == raw.c_str())
        return false;
    out = (uint64_t) v;
    return true;
}

/** a quoted string field, honouring the \" and \\ escapes `quoted` writes. */
bool field_str(const std::string& doc, const char* key, std::string& out) {
    std::string raw = field_raw(doc, key);
    if (raw.empty() || raw.front() != '"')
        return false;
    out.clear();
    for (size_t i = 1; i < raw.size(); ++i) {
        char c = raw[i];
        if (c == '"')
            return true;
        if (c == '\\' && i + 1 < raw.size())
            c = raw[++i];
        out.push_back(c);
    }
    return false;
}

std::string node_json(const barch::graph::node& e) {
    return std::string("{\"kind\":\"") + (e.dir ? "dir" : "leaf") + "\"" +
           ",\"refs\":" + std::to_string(e.refs) +
           ",\"size\":" + std::to_string(e.size) +
           ",\"chunk\":" + std::to_string(e.chunk) +
           ",\"chunks\":" + std::to_string(e.chunks) +
           ",\"version\":" + std::to_string(e.version) +
           ",\"type\":" + gquoted(e.type) +
           ",\"inode\":" + std::to_string(e.inode) + "}";
}

/**
 * The inode JSON the FS reader parses - the same document fs::batch writes,
 * so `fs::file::open_id` opens a graph leaf without knowing it is one. Kept
 * as a copy of the shape rather than a shared builder because the FS one is
 * file-static; if its fields move this has to move with it.
 */
std::string leaf_inode_json(const barch::graph::node& e) {
    return std::string("{\"size\":") + std::to_string(e.size) +
           ",\"type\":" + gquoted(e.type) +
           ",\"chunk\":" + std::to_string(e.chunk) +
           ",\"chunks\":" + std::to_string(e.chunks) +
           ",\"version\":" + std::to_string(e.version) + "}";
}

std::string edge_json(barch::graph::node_id child, const std::string& name) {
    return "{\"node\":" + std::to_string(child) + ",\"name\":" + gquoted(name) + "}";
}

bool parse_node(barch::graph::node_id id, const std::string& raw, barch::graph::node& out) {
    std::string kind;
    if (!field_str(raw, "kind", kind) || (kind != "dir" && kind != "leaf"))
        return false;
    out = barch::graph::node{};
    out.id = id;
    out.dir = kind == "dir";
    field_u64(raw, "refs", out.refs);
    field_u64(raw, "size", out.size);
    field_u64(raw, "chunk", out.chunk);
    field_u64(raw, "chunks", out.chunks);
    field_u64(raw, "version", out.version);
    field_str(raw, "type", out.type);
    field_u64(raw, "inode", out.inode);
    return true;
}

bool parse_edge(const std::string& key, const std::string& raw, barch::graph::edge& out) {
    // graph:e:<16 hex parent>:<16 hex edge>
    if (key.size() < 8 + 16 + 1 + 16)
        return false;
    uint64_t parent = gunhex(key.substr(8, 16));
    uint64_t id = gunhex(key.substr(8 + 16 + 1, 16));
    if (parent == UINT64_MAX || id == UINT64_MAX)
        return false;
    uint64_t child = 0;
    if (!field_u64(raw, "node", child))
        return false;
    std::string name;
    if (!field_str(raw, "name", name))
        return false;
    out.id = id;
    out.parent = parent;
    out.child = child;
    out.name = std::move(name);
    return true;
}

constexpr size_t walk_page = 256;

} // namespace graph_anon

namespace barch::graph {

using namespace ::graph_anon;

/**
 * Plain-key reads: graph key names are exact. The store_access range is
 * text_range's merge - the same walk RANGE-over-RESP answers through, and
 * the same one barch.store.range answers through from inside a stored
 * function (both probed 20-09-2026). raw_get hands the text straight to the
 * shard: search_state takes unfiltered keys and the shard frames them
 * (s_filter_key) the same way the write path did.
 */
/** the key just past everything starting with `prefix` (text form). */
std::string gpast(const std::string& prefix) {
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

bool raw_get(const key_space_ptr& space, const std::string& key, std::string& value) {
    if (!space)
        return false;
    // Through the owner's store_access get, the way every FS read goes: it
    // encodes the key the way the write path did (key_split folding
    // included) and routes it to the shard the write chose. Hand-rolling the
    // lookup with an unfiltered key routes the framed bytes to a *different*
    // shard than the folded write went to - probed 20-09-2026, where this
    // read null while barch.store.get answered 85 bytes.
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.get)
        return false;
    return acc.get(key, value) == barch::foreign::store_access::read_state::present;
}

/**
 * Every key starting with `prefix`, in key order, from `from` on (the prefix
 * itself when `from` is empty). `visit` answers false to stop early.
 *
 * Through the owner's store_access range, the way DIR LS walks - text_range's
 * merge, which is exactly what RANGE-over-RESP answers through (probed
 * 20-09-2026, including from inside a stored function via barch.store). It
 * pages: each page starts again at the last key it saw, which the next page
 * hands back first, so that one key is skipped. A key the range names can
 * still be gone by the time it's read, which is why every caller reads the
 * value and skips what isn't there instead of trusting the key alone.
 */
static void gscan(const key_space_ptr& space, const std::string& prefix, const std::string& from,
                  const std::function<bool(const std::string&)>& visit) {
    if (!space)
        return;
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.range)
        return;
    std::string at = from.empty() ? prefix : from;
    const std::string hi = gpast(prefix);
    std::string seen;
    for (;;) {
        heap::vector<std::string> got;
        acc.range(at, hi, (int64_t) walk_page, got);
        if (got.empty())
            break;
        size_t took = 0;
        for (const auto& key : got) {
            if (key == seen)
                continue;
            ++took;
            seen = key;
            at = key;
            if (key.compare(0, prefix.size(), prefix) != 0)
                return;
            if (!visit(key))
                return;
        }
        if (took == 0 || got.size() < walk_page)
            break;
    }
}

void raw_range(const key_space_ptr& space, const std::string& lo, const std::string& hi,
               int64_t limit, heap::vector<std::string>& out) {
    // Precondition: `lo` is a whole prefix and `hi` is gpast(lo).
    (void) hi;
    gscan(space, lo, {}, [&](const std::string& key) {
        out.push_back(key);
        return !(limit > 0 && (int64_t) out.size() >= limit);
    });
}

std::string node_key(node_id id)            { return NODES + ghex16(id); }
std::string edge_key(node_id parent, edge_id edge) {
    // Edges of one parent share a prefix, so children_of is one range; the
    // edge id sorts creation order, so first-created wins by construction.
    return EDGES + ghex16(parent) + ":" + ghex16(edge);
}
/**
 * The reverse index: every edge naming `child`. Keyed child-first so RM and
 * the reachability sweep find a node's parents without scanning the whole
 * edge table - the value is the parent, ghex16 of it. The edge id joins the
 * two halves back up.
 */
std::string rev_key(node_id child, edge_id edge) {
    return REV + ghex16(child) + ":" + ghex16(edge);
}
/**
 * The name index: edges of one parent with one name share this prefix, in
 * edge id order, so a path step is a short range rather than a read of every
 * sibling. The hash is only where to look - two names can share one, and the
 * edge record settles which is which.
 */
static std::string gidx_prefix(node_id parent, const std::string& name) {
    return NAME_INDEX + ghex16(parent) + ":" + ghex16(gname_hash(name)) + ":";
}
static std::string gidx_key(node_id parent, const std::string& name, edge_id edge) {
    return gidx_prefix(parent, name) + ghex16(edge);
}

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

std::vector<std::string> split(const std::string& clean) {
    std::vector<std::string> segs;
    size_t at = 1;                          // past the leading slash
    while (at <= clean.size()) {
        auto end = clean.find('/', at);
        if (end == std::string::npos)
            end = clean.size();
        if (end > at)
            segs.push_back(clean.substr(at, end - at));
        at = end + 1;
    }
    return segs;
}

/** the parent path of a clean path that isn't the root. */
static std::string parent_path(const std::string& clean) {
    auto slash = clean.find_last_of('/');
    return (slash == 0 || slash == std::string::npos) ? "/" : clean.substr(0, slash);
}

/**
 * The read view the graph resolves through. Graph key names are exact: the
 * edge keys themselves are space-free hex, so the plain-region scan finds
 * every one of them - the composite encoding (key_split folding on spaces)
 * only matters for user *names* inside edge values, which are never bounds.
 * The struct itself lives in graph.h; the member bodies are here beside
 * raw_get/raw_range.
 */
bool view::get(const std::string& key, std::string& value) const {
    std::string raw;
    if (!raw_get(space, key, raw))
        return false;
    value = std::move(raw);
    return true;
}

void view::range(const std::string& lo, const std::string& hi, int64_t limit,
                 heap::vector<std::string>& out) const {
    raw_range(space, lo, hi, limit, out);
}

static view view_of(const key_space_ptr& space) {
    view v;
    v.space = space;
    auto acc = barch::functions::store_for_owner(space);
    v.may_read = acc.may_read;
    v.may_write = acc.may_write;
    return v;
}

/**
 * Whether the space has the name index. A space that was never written has
 * no marker and nothing to index, and a layout "2" one hasn't been reindexed
 * yet; both are read by scanning a parent's edges, which is always right,
 * just slower. Anything else is a layout this build doesn't know.
 */
enum class layout { indexed, unindexed, unknown };

static layout layout_of(const view& acc, std::string* stamp = nullptr) {
    std::string v;
    if (!acc.get(graph_anon::LAYOUT_KEY, v))
        return layout::unindexed;
    if (v == graph_anon::LAYOUT)
        return layout::indexed;
    if (v == UNINDEXED_LAYOUT)
        return layout::unindexed;
    if (stamp)
        *stamp = v;
    return layout::unknown;
}

bool stat_node(const view& acc, node_id id, node& out) {
    if (id == 0) {
        // the root: interior, always there, never stored.
        out = node{};
        out.id = 0;
        out.dir = true;
        out.refs = 1;
        return true;
    }
    std::string raw;
    if (!acc.get(node_key(id), raw))
        return false;
    return parse_node(id, raw, out);
}

static bool read_edge(const view& acc, node_id parent, edge_id id, edge& out) {
    std::string key = edge_key(parent, id), raw;
    return acc.get(key, raw) && parse_edge(key, raw, out);
}

/**
 * The edges under a parent from edge id `after` on, oldest first, at most
 * `limit` of them (0 for all). There is no cap otherwise: a directory that
 * outgrew one page used to stop at 1024 edges without a word, and everything
 * that asks "what is under here" - resolve, LS, MKDIR's exists check, RM's
 * empty check - quietly missed the rest (TODO 407).
 */
static void scan_children(const view& acc, node_id parent, std::vector<edge>& out,
                          edge_id after, size_t limit) {
    if (after == UINT64_MAX)
        return;
    std::string prefix = EDGES + ghex16(parent) + ":";
    std::string from = after ? edge_key(parent, after + 1) : std::string();
    size_t got = 0;
    gscan(acc.space, prefix, from, [&](const std::string& key) {
        std::string raw;
        edge e;
        if (acc.get(key, raw) && parse_edge(key, raw, e)) {
            out.push_back(std::move(e));
            ++got;
        }
        return !(limit && got >= limit);
    });
}

bool children_of(const view& acc, node_id parent, std::vector<edge>& out, edge_id after) {
    scan_children(acc, parent, out, after, 0);
    return true;
}

/** every edge naming `child`, as (edge id, parent), off the reverse index. */
static std::vector<std::pair<edge_id, node_id>> parents_of(const view& acc, node_id child) {
    std::vector<std::pair<edge_id, node_id>> out;
    if (child == 0)
        return out;                         // nothing names the root
    std::string prefix = REV + ghex16(child) + ":";
    gscan(acc.space, prefix, {}, [&](const std::string& key) {
        if (key.size() != prefix.size() + 16)
            return true;
        uint64_t id = gunhex(key.substr(prefix.size(), 16));
        std::string raw;
        if (id == UINT64_MAX || !acc.get(key, raw))
            return true;
        uint64_t parent = gunhex(raw);
        if (parent != UINT64_MAX)
            out.emplace_back(id, parent);
        return true;
    });
    return out;
}

uint64_t refcount(const view& acc, node_id id) {
    return id == 0 ? 1 : (uint64_t) parents_of(acc, id).size();
}

/**
 * The edges of `parent` called `name`, oldest first. Through the name index
 * when the space has one; otherwise the parent's edges are read and filtered,
 * which is what every lookup cost before the index existed.
 */
static void edges_named(const view& acc, node_id parent, const std::string& name,
                        bool indexed, std::vector<edge>& out) {
    if (!indexed) {
        std::vector<edge> kids;
        scan_children(acc, parent, kids, 0, 0);
        for (auto& e : kids) {
            if (e.name == name)
                out.push_back(std::move(e));
        }
        return;
    }
    std::string prefix = gidx_prefix(parent, name);
    gscan(acc.space, prefix, {}, [&](const std::string& key) {
        if (key.size() != prefix.size() + 16)
            return true;
        uint64_t id = gunhex(key.substr(prefix.size(), 16));
        edge e;
        // the edge record is the truth: an index key whose edge is gone, or
        // whose hash belongs to another name, is passed over
        if (id != UINT64_MAX && read_edge(acc, parent, id, e) && e.name == name)
            out.push_back(std::move(e));
        return true;
    });
}

bool edges_for(const view& acc, node_id parent, const std::string& name,
               std::vector<edge>& out) {
    edges_named(acc, parent, name, layout_of(acc) == layout::indexed, out);
    return true;
}

/**
 * Walk a clean path from the root, first-created edge winning at every step
 * unless `pick` names the last one by id. `trail`, when given, collects every
 * node the walk passed through before the last, root first.
 */
static bool walk_path(const view& acc, const std::string& clean, edge_id pick, node& out,
                      edge* edge_out, std::vector<node_id>* trail) {
    auto lay = layout_of(acc);
    if (lay == layout::unknown)
        return false;
    const bool indexed = lay == layout::indexed;
    node cur;
    stat_node(acc, 0, cur);
    edge last{};
    bool have_edge = false;
    auto segs = split(clean);
    for (size_t i = 0; i < segs.size(); ++i) {
        if (!cur.dir)
            return false;                   // a leaf is terminal
        if (trail)
            trail->push_back(cur.id);
        if (pick && i + 1 == segs.size()) {
            if (!read_edge(acc, cur.id, pick, last) || last.name != segs[i])
                return false;
        } else {
            std::vector<edge> got;
            edges_named(acc, cur.id, segs[i], indexed, got);
            if (got.empty())
                return false;
            last = got.front();             // duplicates: first-created wins
        }
        have_edge = true;
        if (!stat_node(acc, last.child, cur))
            return false;                   // an edge whose node is gone
    }
    out = cur;
    if (edge_out && have_edge)
        *edge_out = last;
    return true;
}

bool resolve(const view& acc, const std::string& clean, node& out, edge* edge_out,
             edge_id pick) {
    return walk_path(acc, clean, pick, out, edge_out, nullptr);
}

bool resolve_parent(const view& acc, const std::string& clean, node& out,
                    std::string& name) {
    auto segs = split(clean);
    if (segs.empty())
        return false;                       // the root has no parent
    name = segs.back();
    return resolve(acc, parent_path(clean), out);
}

batch::batch(const key_space_ptr& space) : space(space) {
}

node_id batch::mkdir(const std::string& path) {
    item i;
    i.what = item::kind::make_dir;
    i.path = path;
    pending.push_back(std::move(i));
    return 0;                               // ids are handed out at commit
}

node_id batch::write(const std::string& path, std::string body, const std::string& type,
                     size_t chunk) {
    item i;
    i.what = item::kind::put;
    i.path = path;
    i.body = std::move(body);
    i.type = type;
    i.chunk = chunk ? chunk : default_chunk;
    pending.push_back(std::move(i));
    return 0;
}

bool batch::link(node_id child, const std::string& path, std::string& err) {
    if (child == 0) {
        err = "cannot link the root";
        return false;
    }
    item i;
    i.what = item::kind::link;
    i.child = child;
    i.path = path;
    pending.push_back(std::move(i));
    return true;
}

bool batch::unlink(const std::string& path, std::string& err, edge_id pick) {
    std::string clean;
    if (!normalise(path, clean, err))
        return false;
    if (clean == "/") {
        err = "cannot unlink the root";
        return false;
    }
    item i;
    i.what = item::kind::unlink;
    i.path = clean;
    i.pick = pick;
    pending.push_back(std::move(i));
    return true;
}

bool batch::remove(const std::string& path, bool recursive, std::string& err, edge_id pick) {
    std::string clean;
    if (!normalise(path, clean, err))
        return false;
    if (clean == "/") {
        err = "cannot remove the root";
        return false;
    }
    item i;
    i.what = item::kind::remove;
    i.path = clean;
    i.recursive = recursive;
    i.pick = pick;
    pending.push_back(std::move(i));
    return true;
}

bool batch::rename(const std::string& from, const std::string& to, std::string& err,
                   edge_id pick) {
    std::string clean_from, clean_to;
    if (!normalise(from, clean_from, err) || !normalise(to, clean_to, err))
        return false;
    if (clean_from == "/") {
        err = "cannot move the root";
        return false;
    }
    if (clean_to == "/") {
        err = "a move needs a name to move to";
        return false;
    }
    if (clean_from == clean_to) {
        err = "the source and the destination are the same";
        return false;
    }
    item i;
    i.what = item::kind::rename;
    i.path = clean_from;
    i.to = clean_to;
    i.pick = pick;
    pending.push_back(std::move(i));
    return true;
}

bool batch::copy(const std::string& from, const std::string& to, std::string& err) {
    std::string clean_from, clean_to;
    if (!normalise(from, clean_from, err) || !normalise(to, clean_to, err))
        return false;
    if (clean_to == "/") {
        err = "/ already exists";
        return false;
    }
    item i;
    i.what = item::kind::copy;
    i.path = clean_from;
    i.to = clean_to;
    pending.push_back(std::move(i));
    return true;
}

/**
 * Drop one node and its leaf payload, staged. The caller has already decided
 * the node may go. Chunks are counted off the node record, which carries what
 * the FS inode would have said.
 */
void drop_node(staged& ops, const node& n) {
    ops.remove(node_key(n.id));
    if (!n.dir && n.inode) {
        for (uint64_t c = 0; c < n.chunks; ++c)
            ops.remove(barch::fs::chunk_key(n.inode, c));
        ops.remove(barch::fs::inode_key(n.inode));
    }
}

/** a new edge: the record, the reverse entry, then the name index that finds it. */
static void stage_edge(staged& ops, node_id parent, edge_id id, node_id child,
                       const std::string& name) {
    ops.set(edge_key(parent, id), edge_json(child, name));
    ops.set(rev_key(child, id), ghex16(parent));
    ops.set(gidx_key(parent, name, id), ghex16(child));
}

/** an edge going: the name index first, so nothing is found half removed. */
static void unstage_edge(staged& ops, const edge& e) {
    ops.remove(gidx_key(e.parent, e.name, e.id));
    ops.remove(edge_key(e.parent, e.id));
    ops.remove(rev_key(e.child, e.id));
}

/**
 * One writer per space at a time, from the first read of a commit's plan to
 * its last staged write. The plan reads refs and reachability and the stage
 * writes what follows from them, so a second writer in between loses one of
 * the two: eight clients LINKing one node fifty times each left 401 edges and
 * refs=128 before this, and the UNLINK that took refs to 0 would have freed
 * bytes 273 edges still named (TODO 407).
 */
static std::mutex& write_lock(const key_space_ptr& space) {
    static std::mutex guard;
    static std::unordered_map<std::string, std::unique_ptr<std::mutex>> locks;
    std::lock_guard<std::mutex> g(guard);
    auto& slot = locks[space->get_canonical_name()];
    if (!slot)
        slot = std::make_unique<std::mutex>();
    return *slot;
}

/**
 * Everything reachable from `start` along child edges: the nodes in visit
 * order, each one's record, and each directory's edges out (a dangling one
 * included, so a sweep can take it away too).
 */
static void subtree(const view& acc, node_id start, std::vector<node_id>& order,
                    std::unordered_map<node_id, node>& info,
                    std::unordered_map<node_id, std::vector<edge>>& kids) {
    node first;
    if (!stat_node(acc, start, first))
        return;
    info[start] = first;
    std::vector<node_id> stack{start};
    while (!stack.empty()) {
        node_id cur = stack.back();
        stack.pop_back();
        order.push_back(cur);
        if (!info[cur].dir)
            continue;
        auto& out = kids[cur];
        scan_children(acc, cur, out, 0, 0);
        for (const auto& e : out) {
            if (info.count(e.child))
                continue;
            node n;
            if (!stat_node(acc, e.child, n))
                continue;                   // an edge whose node is gone
            info[e.child] = n;
            stack.push_back(e.child);
        }
    }
}

/**
 * Whether `id` still has a route from the root once the edges in `cut` are
 * gone. Walks up the reverse index, so it costs the node's ancestors - one
 * path for a node in an ordinary tree - rather than the node's subtree.
 */
static bool rooted(const view& acc, node_id id, const std::unordered_set<edge_id>& cut) {
    std::unordered_set<node_id> seen{id};
    std::vector<node_id> stack{id};
    while (!stack.empty()) {
        node_id cur = stack.back();
        stack.pop_back();
        for (const auto& [eid, from] : parents_of(acc, cur)) {
            if (cut.count(eid))
                continue;
            if (from == 0)
                return true;
            if (seen.insert(from).second)
                stack.push_back(from);
        }
    }
    return false;
}

/**
 * What goes when `start` loses the edges in `cut`, staged: every node under
 * it that no longer has a route from the root, with its edges out and its
 * payload, and a fresh count for every node under it that survives. With
 * `start_goes` (RM), `start` dies whatever else still holds it.
 *
 * A count can't decide this once cycles are allowed - a directory linked
 * under itself never reaches zero, and RM RECURSIVE over one used to take
 * the self-link for a hold from outside and leave the directory behind,
 * bytes and all (TODO 407). Reachability can. Only nodes under `start` can
 * lose their route, since a route that ran through a removed edge ran
 * through `start`. So a node under `start` is held when an edge that stays
 * comes from outside that set, whose parent keeps its route; everything a
 * held node reaches is held too, and the rest is unreachable.
 */
static void sweep(const view& acc, staged& ops, node_id start,
                  const std::unordered_set<edge_id>& cut, bool start_goes) {
    std::vector<node_id> order;
    std::unordered_map<node_id, node> info;
    std::unordered_map<node_id, std::vector<edge>> kids;
    subtree(acc, start, order, info, kids);

    std::unordered_map<node_id, std::vector<std::pair<edge_id, node_id>>> parents;
    std::unordered_set<node_id> live;
    std::vector<node_id> stack;
    for (node_id id : order) {
        auto& ps = parents[id];
        ps = parents_of(acc, id);
        if (start_goes && id == start)
            continue;
        for (const auto& [eid, from] : ps) {
            if (cut.count(eid) || info.count(from))
                continue;                   // going, or from inside: proves nothing
            live.insert(id);
            stack.push_back(id);
            break;
        }
    }
    while (!stack.empty()) {
        node_id cur = stack.back();
        stack.pop_back();
        auto k = kids.find(cur);
        if (k == kids.end())
            continue;
        for (const auto& e : k->second) {
            if (cut.count(e.id) || !info.count(e.child))
                continue;
            if (start_goes && e.child == start)
                continue;
            if (live.insert(e.child).second)
                stack.push_back(e.child);
        }
    }

    for (node_id id : order) {
        auto k = kids.find(id);
        if (live.count(id)) {
            // RM takes every edge naming the node; one the reverse index never
            // had would otherwise dangle from a survivor
            if (start_goes && k != kids.end()) {
                for (const auto& e : k->second)
                    if (e.child == start)
                        unstage_edge(ops, e);
            }
            continue;
        }
        if (k != kids.end()) {
            for (const auto& e : k->second)
                unstage_edge(ops, e);
        }
        drop_node(ops, info[id]);
    }
    // a survivor keeps the edges that stay and the ones from outside the set
    for (node_id id : order) {
        if (!live.count(id))
            continue;
        uint64_t refs = 0;
        for (const auto& [eid, from] : parents[id]) {
            if (cut.count(eid))
                continue;
            if (info.count(from) && !live.count(from))
                continue;
            ++refs;
        }
        node n = info[id];
        if (n.refs != refs) {
            n.refs = refs;
            ops.set(node_key(id), node_json(n));
        }
    }
}

/**
 * Rebuild the name index, staged, for a space written before it existed. The
 * index keys that are there go first - an older build stamps layout "2" over
 * a "3" and leaves them stale - then one per edge. Counts are recounted off
 * the edge table on the way past, since LINKs that raced before the write
 * lock existed could leave them short.
 */
static void reindex(const view& acc, staged& ops) {
    gscan(acc.space, NAME_INDEX, {}, [&](const std::string& key) {
        ops.remove(key);
        return true;
    });
    std::unordered_map<node_id, uint64_t> named;
    gscan(acc.space, EDGES, {}, [&](const std::string& key) {
        std::string raw;
        edge e;
        if (acc.get(key, raw) && parse_edge(key, raw, e)) {
            ops.set(gidx_key(e.parent, e.name, e.id), ghex16(e.child));
            ++named[e.child];
        }
        return true;
    });
    const std::string prefix = NODES;
    gscan(acc.space, prefix, {}, [&](const std::string& key) {
        if (key.size() != prefix.size() + 16)
            return true;
        uint64_t id = gunhex(key.substr(prefix.size()));
        node n;
        if (id == UINT64_MAX || !stat_node(acc, id, n))
            return true;
        uint64_t refs = named.count(id) ? named[id] : 0;
        if (n.refs != refs) {
            n.refs = refs;
            ops.set(key, node_json(n));
        }
        return true;
    });
}

bool batch::commit(std::string& err) {
    done.clear();
    if (pending.empty())
        return true;
    if (!space) {
        err = "no key space";
        return false;
    }
    auto owner = barch::functions::store_for_owner(space);
    if (!owner.get || !owner.set) {
        err = "this key space cannot be written";
        return false;
    }
    // Several creates can land together, and a later one may go under a
    // directory an earlier one makes. The other kinds each plan against the
    // store as it stands and work out counts and reachability from it, so
    // two of them in one commit would each miss what the other does.
    if (pending.size() > 1) {
        for (const auto& it : pending) {
            if (it.what != item::kind::make_dir && it.what != item::kind::put) {
                err = "a link, unlink, remove, move or copy is a batch of its own";
                return false;
            }
        }
    }

    std::lock_guard<std::mutex> hold(write_lock(space));
    view acc = view_of(space);
    std::string stamp;
    const auto lay = layout_of(acc, &stamp);
    if (lay == layout::unknown) {
        err = "graph layout " + stamp + " is not one this build can write";
        return false;
    }

    // The plan pass: read every before-image and count the fresh ids, so one
    // reservation of each kind covers the whole commit. The write lock is
    // held from here on, so what this reads is what the stage pass writes over.
    struct plan {
        node parent{};
        std::string name;
        bool parent_pending{false};
        size_t parent_item{0};
        node target{};
        edge target_edge{};
        bool have_target{false};
        node child{};
        // copy: the subtree, as read here, and each leaf's bytes
        std::vector<node_id> order;
        std::unordered_map<node_id, node> info;
        std::unordered_map<node_id, std::vector<edge>> kids;
        std::unordered_map<node_id, std::string> bodies;
    };
    std::vector<plan> before(pending.size());
    std::unordered_map<std::string, size_t> made;   // a directory this batch makes -> its item
    std::unordered_set<std::string> planned;        // every path this batch creates or writes
    uint64_t wanted = 0;                            // node and edge ids, from "graph"
    uint64_t wanted_inodes = 0;                     // leaf inode ids, from "fs"
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& it = pending[i];
        auto& p = before[i];
        std::string clean;
        switch (it.what) {
        case item::kind::make_dir:
        case item::kind::put: {
            if (!normalise(it.path, clean, err))
                return false;
            it.path = clean;
            if (clean == "/") {
                err = "/ already exists";
                return false;
            }
            if (it.what == item::kind::put && it.chunk > barch::fs::max_chunk) {
                err = "CHUNK can be at most " + std::to_string(barch::fs::max_chunk);
                return false;
            }
            if (!planned.insert(clean).second) {
                err = clean + " is written twice in one batch";
                return false;
            }
            p.name = split(clean).back();
            std::string up = parent_path(clean);
            auto mine = made.find(up);
            if (mine != made.end()) {
                // under a directory an earlier item makes: nothing is there yet
                p.parent_pending = true;
                p.parent_item = mine->second;
            } else {
                if (!resolve(acc, up, p.parent))
                    return false;           // err left empty: caller says why
                if (!p.parent.dir) {
                    err = "that parent is a leaf";
                    return false;
                }
                if (resolve(acc, clean, p.target, &p.target_edge)) {
                    if (it.what == item::kind::make_dir) {
                        err = clean + " already exists";
                        return false;
                    }
                    if (p.target.dir) {
                        err = clean + " is a directory";
                        return false;
                    }
                    p.have_target = true;   // PUT overwrites what resolves
                }
            }
            if (it.what == item::kind::make_dir)
                made[clean] = i;
            if (!p.have_target) {
                wanted += 2;                // the node and its edge
                if (it.what == item::kind::put)
                    ++wanted_inodes;        // and a new leaf's inode, from "fs"
            } else if (!p.target.inode) {
                ++wanted_inodes;
            }
            break;
        }
        case item::kind::link:
            if (!normalise(it.path, clean, err))
                return false;
            it.path = clean;
            if (!stat_node(acc, it.child, p.child)) {
                err = "no such node";
                return false;
            }
            if (!resolve_parent(acc, clean, p.parent, p.name))
                return false;
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            wanted += 1;                    // the edge only; the node exists
            break;
        case item::kind::unlink:
        case item::kind::remove:
            // already normalised; resolve the edge that names it
            if (!resolve(acc, it.path, p.target, &p.target_edge, it.pick)) {
                err = it.pick ? "no such edge" : "no such path";
                return false;
            }
            p.have_target = true;
            if (it.what == item::kind::remove && p.target.dir && !it.recursive) {
                std::vector<edge> kids;
                scan_children(acc, p.target.id, kids, 0, 1);
                if (!kids.empty()) {
                    err = it.path + " is not empty";
                    return false;
                }
            }
            break;
        case item::kind::rename: {
            if (!resolve(acc, it.path, p.target, &p.target_edge, it.pick)) {
                err = it.pick ? "no such edge" : "no such path";
                return false;
            }
            p.have_target = true;
            p.name = split(it.to).back();
            // every node the destination's parent path stands on, root first
            std::vector<node_id> trail;
            if (!walk_path(acc, parent_path(it.to), 0, p.parent, nullptr, &trail))
                return false;
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            // A directory moved under itself would be reachable only through
            // its own subtree, which is to say not at all. The old check only
            // looked two levels up, so MV /m /m/b/c/x went through (TODO 407).
            trail.push_back(p.parent.id);
            if (p.target.dir &&
                std::find(trail.begin(), trail.end(), p.target.id) != trail.end()) {
                err = "cannot move a directory into itself";
                return false;
            }
            wanted += 1;                    // the edge at its new name
            break;
        }
        case item::kind::copy: {
            if (!resolve(acc, it.path, p.target)) {
                err = "no such path";
                return false;
            }
            if (!p.target.dir) {
                err = "a leaf copy is a write, not a copy";
                return false;
            }
            if (!resolve_parent(acc, it.to, p.parent, p.name))
                return false;
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            node there;
            if (resolve(acc, it.to, there)) {
                err = it.to + " already exists";
                return false;
            }
            subtree(acc, p.target.id, p.order, p.info, p.kids);
            auto files = barch::functions::store_for_owner(space);
            for (node_id id : p.order) {
                const node& n = p.info[id];
                ++wanted;                   // a fresh node for each
                if (n.dir) {
                    auto k = p.kids.find(id);
                    if (k != p.kids.end()) {
                        for (const auto& e : k->second)
                            if (p.info.count(e.child))
                                ++wanted;   // and a fresh edge for each edge between them
                    }
                    continue;
                }
                ++wanted_inodes;
                std::string body;
                if (n.inode) {
                    barch::fs::file f;
                    if (!barch::fs::file::open_id(files, n.inode, f, err))
                        return false;
                    if (!f.read_at(0, f.meta().size, body, err))
                        return false;
                }
                p.bodies[id] = std::move(body);
            }
            wanted += 1;                    // the edge that hangs the copy up
            break;
        }
        }
    }

    // One reservation of each kind for the whole batch, before a single latch
    // is held - a counter lives on another shard and asking for it mid write
    // is how a lock order inversion gets built. See ids.h. The inode ids come
    // from "fs" with "graph" as the floor: leaves written before TODO 407 took
    // theirs from "graph", and a fresh FS block must not land on one.
    uint64_t first = 0, first_inode = 0;
    if (wanted && !reserve_ids(space, SEQ, wanted, first, err))
        return false;
    if (wanted_inodes && !reserve_ids(space, INODE_SEQ, wanted_inodes, first_inode, err, SEQ))
        return false;
    // Handing out an id past the reservation is how a new-leaf PUT, which
    // counted two ids and staged three, gave its edge id to whatever wrote
    // next - and a LINK into the same directory wrote over the edge. Running
    // past is now an error instead of someone else's id.
    uint64_t next_id = first, next_inode = first_inode;
    bool over = false;
    auto take = [&]() -> uint64_t {
        if (next_id >= first + wanted) {
            over = true;
            return 0;
        }
        return next_id++;
    };
    auto take_inode = [&]() -> uint64_t {
        if (next_inode >= first_inode + wanted_inodes) {
            over = true;
            return 0;
        }
        return next_inode++;
    };

    staged ops(space);
    if (lay == layout::unindexed)
        reindex(acc, ops);
    std::vector<node_id> assigned(pending.size(), 0);
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& it = pending[i];
        auto& p = before[i];
        const node_id parent = p.parent_pending ? assigned[p.parent_item] : p.parent.id;
        switch (it.what) {
        case item::kind::make_dir: {
            node n;
            n.id = take();
            n.dir = true;
            n.refs = 1;
            ops.set(node_key(n.id), node_json(n));
            stage_edge(ops, parent, take(), n.id, p.name);
            assigned[i] = n.id;
            done.push_back(n.id);
            break;
        }
        case item::kind::put: {
            node n;
            if (p.have_target) {
                n = p.target;               // overwrite: the node stays shared
            } else {
                n.id = take();
                n.dir = false;
                n.refs = 1;
            }
            // a shared node keeps its inode and grows a new version, the way
            // an FS overwrite keeps its id and drops its tail.
            if (!n.inode)
                n.inode = take_inode();
            n.size = it.body.size();
            n.chunk = it.chunk;
            n.chunks = (it.body.size() + it.chunk - 1) / it.chunk;
            n.version = (p.have_target ? p.target.version : 0) + 1;
            n.type = it.type;
            for (uint64_t c = 0; c < n.chunks; ++c)
                ops.set(barch::fs::chunk_key(n.inode, c),
                        it.body.substr((size_t) (c * it.chunk), it.chunk));
            if (p.have_target && p.target.inode == n.inode) {
                for (uint64_t c = n.chunks; c < p.target.chunks; ++c)
                    ops.remove(barch::fs::chunk_key(n.inode, c));
            }
            ops.set(barch::fs::inode_key(n.inode), leaf_inode_json(n));
            ops.set(node_key(n.id), node_json(n));
            if (!p.have_target) {
                stage_edge(ops, parent, take(), n.id, p.name);
                assigned[i] = n.id;
                done.push_back(n.id);
            }
            break;
        }
        case item::kind::link: {
            stage_edge(ops, parent, take(), it.child, p.name);
            node n = p.child;
            n.refs = (uint64_t) parents_of(acc, it.child).size() + 1;
            ops.set(node_key(n.id), node_json(n));
            break;
        }
        case item::kind::unlink: {
            unstage_edge(ops, p.target_edge);
            std::unordered_set<edge_id> cut{p.target_edge.id};
            if (p.target.dir && !rooted(acc, p.target.id, cut)) {
                // that was its last route from the root: it goes, and so does
                // whatever under it nothing else holds
                sweep(acc, ops, p.target.id, cut, false);
                break;
            }
            uint64_t left = 0;
            for (const auto& [eid, from] : parents_of(acc, p.target.id)) {
                (void) from;
                if (!cut.count(eid))
                    ++left;
            }
            node n = p.target;
            if (left == 0) {
                drop_node(ops, n);          // a leaf's last edge
            } else if (n.refs != left) {
                n.refs = left;
                ops.set(node_key(n.id), node_json(n));
            }
            break;
        }
        case item::kind::remove: {
            // every edge naming the node goes, wherever it hangs - RM takes the
            // node everywhere, not just under the path that found it
            std::unordered_set<edge_id> cut;
            for (const auto& [eid, from] : parents_of(acc, p.target.id)) {
                edge e;
                if (read_edge(acc, from, eid, e))
                    unstage_edge(ops, e);
                else
                    ops.remove(rev_key(p.target.id, eid));  // an entry with no edge behind it
                cut.insert(eid);
            }
            if (!cut.count(p.target_edge.id)) {
                unstage_edge(ops, p.target_edge);
                cut.insert(p.target_edge.id);
            }
            if (p.target.dir && it.recursive)
                sweep(acc, ops, p.target.id, cut, true);
            else
                drop_node(ops, p.target);
            break;
        }
        case item::kind::rename:
            // the new edge before the old one goes, so the node is never
            // without a name in between
            stage_edge(ops, parent, take(), p.target.id, p.name);
            unstage_edge(ops, p.target_edge);
            break;
        case item::kind::copy: {
            std::unordered_map<node_id, node_id> fresh;
            std::unordered_map<node_id, uint64_t> named;    // edges naming it inside the copy
            for (node_id id : p.order)
                fresh[id] = take();
            for (node_id id : p.order) {
                auto k = p.kids.find(id);
                if (k == p.kids.end())
                    continue;
                for (const auto& e : k->second)
                    if (p.info.count(e.child))
                        ++named[e.child];
            }
            for (node_id id : p.order) {
                const node& src = p.info[id];
                node n;
                n.id = fresh[id];
                n.dir = src.dir;
                n.refs = named[id] + (id == p.target.id ? 1 : 0);
                if (!src.dir) {
                    const std::string& body = p.bodies[id];
                    n.inode = take_inode();
                    n.size = body.size();
                    n.chunk = (src.chunk && src.chunk <= barch::fs::max_chunk) ? src.chunk
                                                                               : default_chunk;
                    n.chunks = (body.size() + n.chunk - 1) / n.chunk;
                    n.version = 1;
                    n.type = src.type;
                    for (uint64_t c = 0; c < n.chunks; ++c)
                        ops.set(barch::fs::chunk_key(n.inode, c),
                                body.substr((size_t) (c * n.chunk), n.chunk));
                    ops.set(barch::fs::inode_key(n.inode), leaf_inode_json(n));
                }
                ops.set(node_key(n.id), node_json(n));
            }
            for (node_id id : p.order) {
                auto k = p.kids.find(id);
                if (k == p.kids.end())
                    continue;
                for (const auto& e : k->second)
                    if (p.info.count(e.child))
                        stage_edge(ops, fresh[id], take(), fresh[e.child], e.name);
            }
            // hung up last, so the copy is whole before anything can reach it
            stage_edge(ops, parent, take(), fresh[p.target.id], p.name);
            done.push_back(fresh[p.target.id]);
            break;
        }
        }
    }
    if (over) {
        err = "a graph commit asked for more ids than it reserved";
        return false;
    }
    // last, so a rebuilt index is all there before anything reads through it
    ops.set(graph_anon::LAYOUT_KEY, graph_anon::LAYOUT);
    if (!ops.commit(err))
        return false;
    pending.clear();
    return true;
}

/**
 * The walk both traversals share - TODO 382.
 *
 * The visited set and the queue (BFS) or stack (DFS) of pending nodes live
 * in a private one-shard scratch space, not in heap containers. The space
 * is anonymous, unregistered and maintenance-free (key_space::make_scratch),
 * and `opt_drop_on_release` removes its files when it goes out of scope at
 * the end of the walk. What `visit` keeps is its own business - bfs and dfs
 * collect their hits in memory, which LIMIT and OFFSET are there to page.
 *
 * Keys: `v:<16 hex>` marks a visited node, `q:`/`s:` are the BFS queue /
 * DFS stack in push order. Values carry `<20 decimal id>:<10 decimal
 * depth>:<10 decimal path len>:<path>` - decimal because Luau's tonumber(x,
 * 16) rounds 16-digit hex through float; space-free so the composite
 * encoding never folds the value.
 */
bool walk_impl(const key_space_ptr& space, node_id start_id, const std::string& start_path,
               uint64_t maxdepth, bool breadth,
               std::function<bool(const edge&, const node&, uint64_t, const std::string&)> visit,
               std::string& err) {
    view acc = view_of(space);
    if (!acc.may_read) {
        err = "this key space cannot be read";
        return false;
    }
    node start;
    if (!stat_node(acc, start_id, start)) {
        err = "no such node";
        return false;
    }
    key_space_ptr tmp = key_space::make_scratch();
    auto sacc = barch::functions::store_for_owner(tmp);
    if (!sacc.get || !sacc.set || !sacc.remove) {
        err = "the walk has nowhere to stand";
        return false;
    }
    // Scratch keys are plain hex under short prefixes - no separator, never
    // folded - so the merge answers them the way barch.store.range does from
    // inside a stored function (probed 20-09-2026).
    auto vkey = [](node_id id) { return "v:" + ghex16(id); };
    auto slotkey = [&](const char* slot, uint64_t seq) {
        return std::string(slot) + ghex16(seq);
    };
    auto encode = [](node_id id, uint64_t depth, const std::string& path) {
        // Fixed-width decimal header, whatever the path holds: Luau's
        // tonumber(x, 16) refuses 16-digit hex (it reads them as float and
        // rounds), so hex ids would come back wrong. Decimal ids, depths and
        // lengths parse exactly. Space-free, so the composite encoding never
        // folds the value.
        char buf[64];
        std::snprintf(buf, sizeof buf, "%020llu:%010llu:%010zu:", (unsigned long long) id,
                      (unsigned long long) depth, path.size());
        return std::string(buf) + path;
    };
    auto decode = [](const std::string& raw, node_id& id, uint64_t& depth,
                     std::string& path) {
        if (raw.size() < 42)
            return false;
        char* end = nullptr;
        unsigned long long v = std::strtoull(raw.c_str(), &end, 10);
        if (!end || end != raw.c_str() + 20 || raw[20] != ':')
            return false;
        id = (node_id) v;
        unsigned long long d = std::strtoull(raw.c_str() + 21, &end, 10);
        if (!end || end != raw.c_str() + 31 || raw[31] != ':')
            return false;
        depth = d;
        unsigned long long n = std::strtoull(raw.c_str() + 32, &end, 10);
        if (!end || end != raw.c_str() + 42 || raw[42] != ':')
            return false;
        if (raw.size() < 43 + (size_t) n)
            return false;
        path = raw.substr(43, (size_t) n);
        return true;
    };
    std::string e;
    std::string scratch_mark;
    auto visited = [&](node_id id) {
        return sacc.get(vkey(id), scratch_mark) ==
               barch::foreign::store_access::read_state::present;
    };
    const char* slot = breadth ? "q:" : "s:";
    // Slot ids only ever grow: a slot is written once and never reused.
    // Reusing a popped slot id replays a tombstone the merge still counts:
    // the get correctly answers nil while the range still walks the tomb, so
    // the pop after a reuse reads the tomb back as a phantom entry and every
    // node is visited twice. Probed 20-09-2026: BFS over /t listed 9 hits for
    // 5 nodes until the slot ids went monotonic.
    //
    // BFS pops the oldest slot and DFS the newest. The old walk popped the
    // oldest for both, which made DFS a breadth first walk with each level's
    // siblings reversed (TODO 407). DFS keeps the ids of the slots it has yet
    // to pop in memory - eight bytes a pending entry, the paths stay in
    // scratch - since a stack that never reuses an id can't find its top by
    // counting.
    uint64_t tail = 0, head = 0;
    std::vector<uint64_t> stack;
    auto push = [&](node_id id, uint64_t depth, const std::string& path) {
        uint64_t at = tail++;
        sacc.set(slotkey(slot, at), encode(id, depth, path), e);
        if (!breadth)
            stack.push_back(at);
    };
    // BFS marks a node when it's queued: the first path to reach it is the
    // shortest, and nothing is queued twice. DFS marks it when it pops, since
    // pre-order meets a node along whichever path pops first - a node queued
    // twice before then is simply passed over the second time.
    if (breadth)
        sacc.set(vkey(start_id), "1", e);
    push(start_id, 0, start_path);
    for (;;) {
        uint64_t at = 0;
        if (breadth) {
            if (head >= tail)
                break;
            at = head++;
        } else {
            if (stack.empty())
                break;
            at = stack.back();
            stack.pop_back();
        }
        std::string key = slotkey(slot, at);
        std::string raw;
        if (sacc.get(key, raw) != barch::foreign::store_access::read_state::present)
            break;
        sacc.remove(key);
        node_id id = 0;
        uint64_t depth = 0;
        std::string path;
        if (!decode(raw, id, depth, path))
            break;
        if (!breadth) {
            if (visited(id))
                continue;                   // reached along an earlier path
            sacc.set(vkey(id), "1", e);
        }
        node cur;
        if (!stat_node(acc, id, cur))
            continue;                       // a node removed mid-walk
        edge here{};
        here.child = id;
        if (!visit(here, cur, depth, path))
            return true;
        if (maxdepth && depth >= maxdepth)
            continue;
        std::vector<edge> kids;
        if (!children_of(acc, id, kids))
            return false;
        auto child_path = [&](const edge& c) {
            return (path == "/") ? "/" + c.name : path + "/" + c.name;
        };
        if (breadth) {
            for (const auto& c : kids) {
                if (visited(c.child))
                    continue;               // first visit wins; cycles end here
                sacc.set(vkey(c.child), "1", e);
                push(c.child, depth + 1, child_path(c));
            }
        } else {
            // the newest pops first, so the first child goes on last
            for (size_t k = kids.size(); k > 0; --k) {
                const auto& c = kids[k - 1];
                if (!visited(c.child))
                    push(c.child, depth + 1, child_path(c));
            }
        }
    }
    return true;
}

static bool start_of(const key_space_ptr& space, const std::string& start, node_id& id,
                     std::string& path, std::string& err) {
    view acc = view_of(space);
    if (!start.empty() && start.front() == '/') {
        std::string clean;
        if (!normalise(start, clean, err))
            return false;
        node n;
        if (!resolve(acc, clean, n)) {
            err = "no such path";
            return false;
        }
        id = n.id;
        path = clean;
        return true;
    }
    if (!start.empty() && start.find_first_not_of("0123456789") == std::string::npos) {
        char* end = nullptr;
        unsigned long long v = std::strtoull(start.c_str(), &end, 10);
        if (end && !*end) {
            node n;
            if (!stat_node(acc, (node_id) v, n)) {
                err = "no such node";
                return false;
            }
            // An id has no canonical path - one node answers at many - so
            // the walk reports paths rooted at the id itself.
            id = (node_id) v;
            path = start;
            return true;
        }
    }
    err = "a start is a path or a node id";
    return false;
}

bool bfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err) {
    node_id id = 0;
    std::string path;
    if (!start_of(space, start, id, path, err))
        return false;
    size_t skipped = 0;
    bool ok = walk_impl(space, id, path, depth, true,
                        [&](const edge&, const node& n, uint64_t, const std::string& p) {
        if (skipped < offset) {
            ++skipped;
            return true;
        }
        if (limit && out.size() >= limit)
            return false;
        out.emplace_back(n.id, p);
        return true;
    }, err);
    return ok;
}

bool dfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err) {
    node_id id = 0;
    std::string path;
    if (!start_of(space, start, id, path, err))
        return false;
    size_t skipped = 0;
    bool ok = walk_impl(space, id, path, depth, false,
                        [&](const edge&, const node& n, uint64_t, const std::string& p) {
        if (skipped < offset) {
            ++skipped;
            return true;
        }
        if (limit && out.size() >= limit)
            return false;
        out.emplace_back(n.id, p);
        return true;
    }, err);
    return ok;
}

}
