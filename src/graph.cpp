// The graph store: nodes joined by edges, as ids rather than as paths.
// See TODO 382 and graph.h.
//
// FS stores a path literally in the key name, so a file can only live at one
// path. Here the edge table maps (parent, name) to a node id and the node
// table says what each node is, so one node answers at any number of paths.
// Leaf bytes reuse the FS inode/chunk keys verbatim - graph.cpp stages chunk
// and inode keys in the exact JSON `fs::file::open_id` parses, and never
// touches a chunk itself. Only the node/edge tables are new.
//
// Ordering inside one commit follows the FS batch: chunks, then the inode,
// then the node, then the edges. Nothing reaches a chunk except through the
// node, and nothing reaches a node except through an edge, so a half applied
// write is invisible rather than wrong. Deletes go the other way: edges,
// then the node, then its payload.
//
// Ids come from one `ids.h` sequence ("graph") shared by nodes and edges, so
// the two can never collide - a traversal's visited set is keyed by node id
// and an edge id can never masquerade as one. Reserved once, before any
// latch, the way fs::batch does: the counter lives on whatever shard
// "ids:graph" hashes to, and reserving mid-write is a lock order inversion.
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

#include <cstdio>
#include <cstring>
#include <unordered_map>
#include <unordered_set>

namespace graph_anon {

const char* NODES = "graph:n:";
const char* EDGES = "graph:e:";
const char* REV = "graph:r:";
const char* LAYOUT_KEY = "graph:layout";
const char* LAYOUT = "2";
const char* SEQ = "graph";

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

void raw_range(const key_space_ptr& space, const std::string& lo, const std::string& hi,
               int64_t limit, heap::vector<std::string>& out) {
    if (!space)
        return;
    // By edge prefix through the owner's store_access range, the way DIR LS
    // walks: every edge of one parent shares the "graph:e:<16 hex>:" prefix
    // and sorts by edge id (creation order). The access range is text_range's
    // merge, which is exactly what RANGE-over-RESP answers through - probed
    // 20-09-2026, including from inside a stored function via barch.store.
    //
    // Precondition: `lo` is a whole edge prefix and `hi` is gpast(lo).
    auto acc = barch::functions::store_for_owner(space);
    std::string at = lo;
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
            if (key.compare(0, lo.size(), lo) != 0)
                return;
            out.push_back(key);
            if (limit > 0 && (int64_t) out.size() >= limit)
                return;
        }
        if (took == 0 || got.size() < walk_page)
            break;
    }
}

std::string node_key(node_id id)            { return NODES + ghex16(id); }
std::string edge_key(node_id parent, edge_id edge) {
    // Edges of one parent share a prefix, so children_of is one range; the
    // edge id sorts creation order, so first-created wins by construction.
    return EDGES + ghex16(parent) + ":" + ghex16(edge);
}
/**
 * The reverse index: every edge naming `child`. Keyed child-first so RM can
 * delete a node everywhere without scanning the whole edge table - the value
 * is the parent, ghex16 of it. The edge id joins the two halves back up.
 */
std::string rev_key(node_id child, edge_id edge) {
    return REV + ghex16(child) + ":" + ghex16(edge);
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

bool edges_for(const view& acc, node_id parent, const std::string& name,
               std::vector<edge>& out) {
    // One range, like children_of: the re-range loop replays tombstones.
    std::vector<edge> kids;
    if (!children_of(acc, parent, kids))
        return false;
    for (auto& e : kids) {
        if (e.name == name)
            out.push_back(std::move(e));
    }
    return true;
}

bool children_of(const view& acc, node_id parent, std::vector<edge>& out) {
    std::string prefix = EDGES + ghex16(parent) + ":";
    std::string hi = gpast(prefix);
    // One range, not a re-range loop: re-ranging from the last key hands the
    // tombstone back and the walk counts it as a fresh entry (probed
    // 20-09-2026: BFS listed 9 hits for 5 nodes). A parent's edge list fits
    // one page - edges are small and parents are narrow - and a parent that
    // outgrows the page is a correctness fallback away, not a wrong answer.
    // Until that fallback exists the cap is explicit rather than silent.
    static const size_t edge_page = 1024;
    heap::vector<std::string> got;
    acc.range(prefix, hi, (int64_t) edge_page, got);
    for (const auto& key : got) {
        if (key.compare(0, prefix.size(), prefix) != 0)
            break;
        std::string raw;
        if (!acc.get(key, raw))
            continue;
        edge e;
        if (!parse_edge(key, raw, e))
            continue;
        out.push_back(std::move(e));
    }
    return true;
}

uint64_t refcount(const view& acc, node_id id) {
    if (id == 0)
        return 1;
    node n;
    return stat_node(acc, id, n) ? n.refs : 0;
}

bool resolve(const view& acc, const std::string& clean, node& out, edge* edge_out) {
    node cur;
    if (!stat_node(acc, 0, cur))
        return false;
    edge last{};
    bool have_edge = false;
    for (const auto& seg : split(clean)) {
        if (!cur.dir)
            return false;                   // a leaf is terminal
        std::vector<edge> got;
        if (!edges_for(acc, cur.id, seg, got) || got.empty())
            return false;
        last = got.front();                 // duplicates: first-created wins
        have_edge = true;
        if (!stat_node(acc, last.child, cur))
            return false;                   // an edge whose node is gone
    }
    out = cur;
    if (edge_out && have_edge)
        *edge_out = last;
    return true;
}

bool resolve_parent(const view& acc, const std::string& clean, node& out,
                    std::string& name) {
    auto segs = split(clean);
    if (segs.empty())
        return false;                       // the root has no parent
    name = segs.back();
    // the parent path is everything but the last segment of `clean`
    auto slash = clean.find_last_of('/');
    std::string parent = (slash == 0) ? "/" : clean.substr(0, slash);
    return resolve(acc, parent, out);
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

bool batch::unlink(const std::string& path, std::string& err) {
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
    pending.push_back(std::move(i));
    return true;
}

bool batch::remove(const std::string& path, bool recursive, std::string& err) {
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
    pending.push_back(std::move(i));
    return true;
}

bool batch::rename(const std::string& from, const std::string& to, std::string& err) {
    std::string clean_from, clean_to;
    if (!normalise(from, clean_from, err) || !normalise(to, clean_to, err))
        return false;
    if (clean_from == "/") {
        err = "cannot move the root";
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
    pending.push_back(std::move(i));
    return true;
}

/**
 * Drop one node and its leaf payload, staged. The caller has already decided
 * the node may go - refs reached zero, or RM named it. Chunks are counted off
 * the node record, which carries what the FS inode would have said.
 */
void drop_node(staged& ops, const node& n) {
    ops.remove(node_key(n.id));
    if (!n.dir && n.inode) {
        for (uint64_t c = 0; c < n.chunks; ++c)
            ops.remove(barch::fs::chunk_key(n.inode, c));
        ops.remove(barch::fs::inode_key(n.inode));
    }
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
    view acc = view_of(space);

    // The plan pass: read every before-image and count the fresh ids, so the
    // one reservation below covers the whole commit. Anything that resolves a
    // path does it here, against the store as it stands - a concurrent writer
    // between planning and staging can move what a path names, the same
    // exposure every staged importer already carries.
    struct plan {
        node parent{};
        bool have_parent{false};
        std::string name;
        node target{};
        edge target_edge{};
        bool have_target{false};
        node child{};
        bool have_child{false};
    };
    std::vector<plan> before(pending.size());
    uint64_t wanted = 0;                    // fresh node ids + fresh edge ids
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& it = pending[i];
        auto& p = before[i];
        std::string clean;
        switch (it.what) {
        case item::kind::make_dir:
        case item::kind::put:
            if (!normalise(it.path, clean, err))
                return false;
            it.path = clean;
            if (!resolve_parent(acc, clean, p.parent, p.name))
                return false;               // err left empty: caller says why
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            if (resolve(acc, clean, p.target, &p.target_edge)) {
                if (it.what == item::kind::make_dir) {
                    err = clean + " already exists";
                    return false;
                }
                p.have_target = true;       // PUT overwrites what resolves
                if (p.target.dir) {
                    err = clean + " is a directory";
                    return false;
                }
            } else if (it.what == item::kind::put) {
                p.have_target = false;      // PUT creates what does not resolve
            }
            if (!p.have_target)
                wanted += 2;                // a node and its edge
            break;
        case item::kind::link:
            if (!normalise(it.path, clean, err))
                return false;
            it.path = clean;
            if (!stat_node(acc, it.child, p.child) || it.child == 0) {
                err = "no such node";
                return false;
            }
            if (!resolve_parent(acc, clean, p.parent, p.name))
                return false;
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            p.have_child = true;
            wanted += 1;                    // the edge only; the node exists
            break;
        case item::kind::unlink:
            // already normalised by unlink(); resolve the edge to drop.
            if (!resolve(acc, it.path, p.target, &p.target_edge)) {
                err = "no such path";
                return false;
            }
            p.have_target = true;
            break;
        case item::kind::remove:
            if (!resolve(acc, it.path, p.target, &p.target_edge)) {
                err = "no such path";
                return false;
            }
            p.have_target = true;
            break;
        case item::kind::rename: {
            if (!resolve(acc, it.path, p.target, &p.target_edge)) {
                err = "no such path";
                return false;
            }
            p.have_target = true;
            std::string name;
            if (!resolve_parent(acc, it.to, p.parent, name))
                return false;
            if (!p.parent.dir) {
                err = "that parent is a leaf";
                return false;
            }
            // a directory cannot be moved into itself: the destination's
            // parent chain must not pass through the node being moved.
            if (!p.target.dir) {
                p.name = name;
                break;
            }
            node walk = p.parent;
            std::unordered_set<node_id> seen;
            // walk up by re-resolving: cheap, renames are rare. The chain is
            // found by resolving the destination parent path segment by
            // segment - each prefix must resolve, else the parent would not
            // have resolved either.
            std::string chain = it.to;
            auto slash = chain.find_last_of('/');
            chain = (slash == 0 || slash == std::string::npos) ? "/" : chain.substr(0, slash);
            for (;;) {
                if (walk.id == p.target.id) {
                    err = "cannot move a directory into itself";
                    return false;
                }
                if (walk.id == 0 || !seen.insert(walk.id).second)
                    break;
                auto upslash = chain.find_last_of('/');
                std::string up_path = (upslash == 0 || upslash == std::string::npos)
                    ? "/" : chain.substr(0, upslash);
                node up;
                if (!resolve(acc, up_path, up))
                    break;
                walk = up;
                if (chain == "/")
                    break;
            }
            p.name = name;
            wanted += 1;                    // the edge at its new name
            break;
        }
        }
    }

    // One reservation for the whole batch, before a single latch is held -
    // the counter lives on another shard and asking for it mid write is how a
    // lock order inversion gets built. See ids.h.
    uint64_t next_id = 0;
    if (wanted && !reserve_ids(space, SEQ, wanted, next_id, err))
        return false;

    staged ops(space);
    ops.set(graph_anon::LAYOUT_KEY, graph_anon::LAYOUT);
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& it = pending[i];
        auto& p = before[i];
        switch (it.what) {
        case item::kind::make_dir: {
            node_id id = next_id++;
            edge_id eid = next_id++;
            node n;
            n.id = id;
            n.dir = true;
            n.refs = 1;
            ops.set(node_key(id), node_json(n));
            ops.set(edge_key(p.parent.id, eid), edge_json(id, p.name));
            ops.set(rev_key(id, eid), ghex16(p.parent.id));
            done.push_back(id);
            break;
        }
        case item::kind::put: {
            node n;
            if (p.have_target) {
                n = p.target;               // overwrite: the node stays shared
                n.inode = p.target.inode;
            } else {
                n.id = next_id++;
                n.dir = false;
                n.refs = 1;
                n.inode = 0;
            }
            // a shared node keeps its inode and grows a new version, the way
            // an FS overwrite keeps its id and drops its tail.
            uint64_t inode = n.inode ? n.inode : next_id++;
            n.size = it.body.size();
            n.chunk = it.chunk;
            n.chunks = it.chunk ? (it.body.size() + it.chunk - 1) / it.chunk : 0;
            n.version = (p.have_target ? p.target.version : 0) + 1;
            n.type = it.type;
            n.inode = inode;
            for (uint64_t c = 0; c < n.chunks; ++c)
                ops.set(barch::fs::chunk_key(inode, c),
                        it.body.substr((size_t) (c * it.chunk), it.chunk));
            if (p.have_target && p.target.chunks > n.chunks) {
                for (uint64_t c = n.chunks; c < p.target.chunks; ++c)
                    ops.remove(barch::fs::chunk_key(inode, c));
            }
            ops.set(barch::fs::inode_key(inode), leaf_inode_json(n));
            ops.set(node_key(n.id), node_json(n));
            if (!p.have_target) {
                edge_id eid = next_id++;
                ops.set(edge_key(p.parent.id, eid), edge_json(n.id, p.name));
                ops.set(rev_key(n.id, eid), ghex16(p.parent.id));
                done.push_back(n.id);
            }
            break;
        }
        case item::kind::link: {
            edge_id eid = next_id++;
            node_id parent_hint = p.parent.id;
            ops.set(edge_key(parent_hint, eid), edge_json(it.child, p.name));
            ops.set(rev_key(it.child, eid), ghex16(parent_hint));
            node n = p.child;
            ++n.refs;
            ops.set(node_key(n.id), node_json(n));
            break;
        }
        case item::kind::unlink: {
            ops.remove(edge_key(p.target_edge.parent, p.target_edge.id));
            ops.remove(rev_key(p.target.id, p.target_edge.id));
            node n = p.target;
            if (n.refs > 0)
                --n.refs;
            if (n.refs == 0 && n.id != 0)
                drop_node(ops, n);
            else
                ops.set(node_key(n.id), node_json(n));
            break;
        }
        case item::kind::remove: {
            // every edge naming the node, from the reverse index - RM deletes
            // the node everywhere, not just under the path that named it.
            std::string prefix = REV + ghex16(p.target.id) + ":";
            std::string hi = gpast(prefix);
            std::string at = prefix;
            std::string seen;
            std::vector<std::pair<node_id, edge_id>> edges;
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
                    if (key.size() < prefix.size() + 16)
                        continue;
                    uint64_t eid = gunhex(key.substr(prefix.size(), 16));
                    if (eid == UINT64_MAX)
                        continue;
                    std::string raw;
                    if (!acc.get(key, raw))
                        continue;
                    uint64_t parent = gunhex(raw);
                    if (parent == UINT64_MAX)
                        continue;
                    edges.emplace_back((node_id) parent, (edge_id) eid);
                }
                if (took == 0 || got.size() < walk_page)
                    break;
            }
            if (p.target.dir && !it.recursive) {
                std::vector<edge> kids;
                if (children_of(acc, p.target.id, kids) && !kids.empty()) {
                    err = it.path + " is not empty";
                    return false;
                }
            }
            // Recursive: the subtree rooted here, in reachability order, so a
            // cycle is a finite list rather than a loop. Reachability is
            // path-scoped: following an edge out of the subtree does not pull
            // its target in. /t/loopback names /a, but /a/hello.txt is not
            // under /t and RM /t must not take it. So the walk does not
            // follow edges to nodes that are reachable from outside the
            // doomed set: a child with a reverse edge under a live parent
            // stays out, and only its edge from the doomed parent goes.
            // Collected with heap sets - the walk's own visited set lives in
            // scratch (see walk_impl), but the doomed set is the primitive
            // the commit reasons about and stays beside it.
            std::unordered_set<node_id> doomed;
            std::vector<node_id> order;
            if (it.recursive && p.target.dir) {
                std::vector<node_id> stack;
                stack.push_back(p.target.id);
                doomed.insert(p.target.id);
                while (!stack.empty()) {
                    node_id cur = stack.back();
                    stack.pop_back();
                    order.push_back(cur);
                    std::vector<edge> kids;
                    if (!children_of(acc, cur, kids))
                        return false;
                    for (const auto& k : kids) {
                        if (doomed.find(k.child) != doomed.end())
                            continue;
                        // shared from outside? then the edge goes but the
                        // node stays out of the doomed set.
                        std::string rpre = REV + ghex16(k.child) + ":";
                        std::string rhi = gpast(rpre);
                        heap::vector<std::string> rev;
                        acc.range(rpre, rhi, (int64_t) walk_page, rev);
                        bool outside = false;
                        for (const auto& rk : rev) {
                            if (rk.size() < rpre.size() + 16)
                                continue;
                            std::string rraw;
                            if (!acc.get(rk, rraw))
                                continue;
                            uint64_t rp = gunhex(rraw);
                            if (rp == UINT64_MAX)
                                continue;
                            if (doomed.find((node_id) rp) == doomed.end() &&
                                (node_id) rp != p.target.id) {
                                outside = true;
                                break;
                            }
                        }
                        if (!outside) {
                            doomed.insert(k.child);
                            stack.push_back(k.child);
                        }
                    }
                }
            } else {
                doomed.insert(p.target.id);
                order.push_back(p.target.id);
            }
            // Edges from inside the doomed set go, wherever they point; edges
            // to the target go wherever they hang. The first list is staged
            // below; the second is `edges` above. A full edge-table scan is
            // what the missing reverse walk would have cost - RM is honest
            // about paying it.
            // remove the target's edges everywhere first
            for (const auto& [parent, eid] : edges) {
                ops.remove(edge_key(parent, eid));
                ops.remove(rev_key(p.target.id, eid));
            }
            if (it.recursive && p.target.dir) {
                std::string epre = EDGES;
                std::string ehi = gpast(epre);
                std::string eat = epre;
                std::string eseen;
                for (;;) {
                    heap::vector<std::string> got;
                    acc.range(eat, ehi, (int64_t) walk_page, got);
                    if (got.empty())
                        break;
                    size_t took = 0;
                    for (const auto& key : got) {
                        if (key == eseen)
                            continue;
                        ++took;
                        eseen = key;
                        eat = key;
                        std::string raw;
                        if (!acc.get(key, raw))
                            continue;
                        edge e;
                        if (!parse_edge(key, raw, e))
                            continue;
                        if (doomed.find(e.parent) == doomed.end())
                            continue;       // from outside: not ours to take
                        ops.remove(key);
                        ops.remove(rev_key(e.child, e.id));
                        // the child's refs lose one; counted below from the
                        // before-images so the commit is a pure function of
                        // the plan pass.
                    }
                    if (took == 0 || got.size() < walk_page)
                        break;
                }
            }
            // Refs after the sweep: the target dies unconditionally; a
            // descendant dies when no edge names it any more. Every doomed
            // node loses exactly the edges the sweep removed: its reverse
            // edges under doomed parents. A node kept out of the doomed set
            // (shared from outside) is not in `order` at all - its node
            // record is untouched and only its doomed-parent edge goes, so
            // its refs drop by the edges removed above. Counted from the
            // before-images so the commit is a pure function of the plan.
            std::unordered_map<node_id, uint64_t> lost;
            for (node_id d : order)
                lost[d] = 0;
            if (it.recursive && p.target.dir) {
                // recount from the store: for each doomed node, how many of
                // its reverse edges hang under a doomed parent or are the
                // target's own. One reverse range per doomed node - RM is
                // already O(subtree) and this keeps it linear in it.
                for (node_id d : order) {
                    if (d == p.target.id) {
                        lost[d] = p.target.refs;   // everything naming it goes
                        continue;
                    }
                    std::string rpre = REV + ghex16(d) + ":";
                    std::string rhi = gpast(rpre);
                    std::string rat = rpre;
                    std::string rseen;
                    uint64_t gone = 0;
                    for (;;) {
                        heap::vector<std::string> got;
                        acc.range(rat, rhi, (int64_t) walk_page, got);
                        if (got.empty())
                            break;
                        size_t took = 0;
                        for (const auto& key : got) {
                            if (key == rseen)
                                continue;
                            ++took;
                            rseen = key;
                            rat = key;
                            std::string raw;
                            if (!acc.get(key, raw))
                                continue;
                            uint64_t parent = gunhex(raw);
                            if (parent == UINT64_MAX)
                                continue;
                            if (doomed.find((node_id) parent) != doomed.end())
                                ++gone;
                        }
                        if (took == 0 || got.size() < walk_page)
                            break;
                    }
                    lost[d] = gone;
                }
                // Nodes the sweep's edge removal orphaned without dooming:
                // a child of a doomed parent that stayed out because it is
                // shared from outside loses its doomed-parent edge here.
                // Its refs drop by one per such edge; the node itself stays.
                std::string epre = EDGES;
                std::string ehi = gpast(epre);
                std::string eat = epre;
                std::string eseen;
                std::unordered_map<node_id, uint64_t> shared_lost;
                for (;;) {
                    heap::vector<std::string> got;
                    acc.range(eat, ehi, (int64_t) walk_page, got);
                    if (got.empty())
                        break;
                    size_t took = 0;
                    for (const auto& key : got) {
                        if (key == eseen)
                            continue;
                        ++took;
                        eseen = key;
                        eat = key;
                        std::string raw;
                        if (!acc.get(key, raw))
                            continue;
                        edge e;
                        if (!parse_edge(key, raw, e))
                            continue;
                        if (doomed.find(e.parent) == doomed.end())
                            continue;
                        if (doomed.find(e.child) != doomed.end())
                            continue;       // counted in lost[] above
                        shared_lost[e.child]++;
                    }
                    if (took == 0 || got.size() < walk_page)
                        break;
                }
                for (const auto& [id, n] : shared_lost) {
                    node sn;
                    if (!stat_node(acc, id, sn))
                        continue;
                    if (sn.refs > n) {
                        sn.refs -= n;
                        ops.set(node_key(id), node_json(sn));
                    }
                }
            } else {
                lost[p.target.id] = p.target.refs;
            }
            for (node_id d : order) {
                node n;
                if (!stat_node(acc, d, n))
                    continue;
                if (d == p.target.id) {
                    // the named node dies even when other paths name it: RM
                    // deletes the node, UNLINK deletes the edge. Its surviving
                    // edges are removed above; a descendant shared from
                    // outside keeps its node with its remaining refs.
                    drop_node(ops, n);
                    continue;
                }
                uint64_t survive = (n.refs > lost[d]) ? n.refs - lost[d] : 0;
                if (survive == 0) {
                    drop_node(ops, n);
                } else {
                    // kept alive from outside the subtree: its refs are what
                    // remains after the doomed edges leave.
                    n.refs = survive;
                    ops.set(node_key(d), node_json(n));
                }
            }
            break;
        }
        case item::kind::rename: {
            // only the edge moves: the node id behind it does not change, so
            // a shared node stays shared under its new name.
            ops.set(edge_key(p.parent.id, next_id),
                    edge_json(p.target.id, p.name));
            ops.set(rev_key(p.target.id, next_id), ghex16(p.parent.id));
            ++next_id;
            ops.remove(edge_key(p.target_edge.parent, p.target_edge.id));
            ops.remove(rev_key(p.target.id, p.target_edge.id));
            break;
        }
        }
    }
    if (!ops.commit(err))
        return false;
    pending.clear();
    return true;
}

/**
 * The walk both traversals share - TODO 382.
 *
 * The visited set and the queue (BFS) or stack (DFS) live in a private
 * one-shard scratch space, not in heap containers: a walk over millions of
 * nodes is keys, not memory. The space is anonymous, unregistered and
 * maintenance-free (key_space::make_scratch), and `opt_drop_on_release`
 * removes its files when it goes out of scope at the end of the walk.
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
    sacc.set(vkey(start_id), "1", e);
    const char* slot = breadth ? "q:" : "s:";
    // The queue is monotonic: slots are only ever written once and popped
    // slots are never reused. Reusing a popped slot id (head++) replays a
    // tombstone the merge still counts: the get correctly answers nil while
    // the range still walks the tomb, so the pop after a reuse reads the
    // tomb back as a phantom entry and every node is visited twice. Probed
    // 20-09-2026: BFS over /t listed 9 hits for 5 nodes until the slot ids
    // went monotonic.
    uint64_t tail = 0;
    sacc.set(slotkey(slot, tail++), encode(start_id, 0, start_path), e);
    // The head slot is consumed, not reused: pop by removing, advance by
    // allocating a fresh slot id. `head` counts pops; the slot ids only grow.
    uint64_t head = 0;
    uint64_t pops = 0;
    while (pops < tail) {
        std::string key = slotkey(slot, head);
        std::string raw;
        if (sacc.get(key, raw) != barch::foreign::store_access::read_state::present)
            break;
        sacc.remove(key);
        ++head;
        ++pops;
        node_id id = 0;
        uint64_t depth = 0;
        std::string path;
        if (!decode(raw, id, depth, path))
            break;
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
        if (!breadth) {
            // pushed in reverse so the first child pops first: pre-order.
            // The node itself is already visited above; the children are only
            // emitted when they are popped later. Visited marks are set at
            // push time, so nothing is ever pushed twice.
            for (size_t k = kids.size(); k > 0; --k) {
                const auto& c = kids[k - 1];
                if (visited(c.child))
                    continue;
                sacc.set(vkey(c.child), "1", e);
                node n;
                if (!stat_node(acc, c.child, n))
                    continue;               // an edge whose node went mid-walk
                std::string cpath = (path == "/") ? "/" + c.name : path + "/" + c.name;
                sacc.set(slotkey(slot, tail++), encode(c.child, depth + 1, cpath), e);
            }
            continue;
        }
        for (const auto& c : kids) {
            if (visited(c.child))
                continue;                   // first visit wins; cycles end here
            sacc.set(vkey(c.child), "1", e);
            node n;
            if (!stat_node(acc, c.child, n))
                continue;
            std::string cpath = (path == "/") ? "/" + c.name : path + "/" + c.name;
            // Visit at pop, not at push: BFS pushes the child now and the
            // pop visits it later, exactly once. Visiting here as well would
            // list every node twice - once pushed, once popped. (DFS visits
            // only at pop for the same reason.)
            sacc.set(slotkey(slot, tail++), encode(c.child, depth + 1, cpath), e);
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
