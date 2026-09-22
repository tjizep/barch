#pragma once
//
// The graph store: nodes joined by edges, as ids rather than as paths.
// See TODO 382 and TODO 407.
//
// FS stores paths literally in key names (`fs:n:<path>`), so a node can only
// ever live at one path. Here paths are not stored at all. The store is the
// classic recursive relational tuple {parent_id, name, id}: an edge table maps
// (parent, name) to a node, and a node table says what each node is. One node
// may be edged from any number of parents, so the same content answers at any
// number of paths.
//
// THE LAYOUT
//
//     graph:layout          "3": the name index below is there. "2" is the
//                           same store without it; the first write rebuilds
//                           the index and stamps "3", and until then reads
//                           scan a parent's edges instead. Anything else is
//                           refused rather than half read (layout "1" had
//                           8-hex edge ids, which would parse as truncated
//                           16-hex ones).
//     graph:n:<id>          node -> {kind, refs, size, chunk, chunks, type, version, inode}
//                           kind is "dir" or "leaf"; refs counts the edges that
//                           name it. size/chunk/chunks/type/version mirror the
//                           leaf's FS inode, so a listing needs no second read.
//     graph:e:<parent>:<edge>  edge -> {node, name}
//                           parent and edge both 16-hex. The name lives in
//                           the value so a duplicate (parent, name) pair is a
//                           second edge key rather than an overwrite.
//     graph:r:<child>:<edge>   parent, 16-hex: every edge naming a node, so
//                           RM and the reachability sweep find a node's
//                           parents without scanning the edge table.
//     graph:x:<parent>:<hash>:<edge>  child, 16-hex: the name index. <hash>
//                           is FNV-1a of the name, so one path step is one
//                           short range instead of a read of every sibling.
//                           The edge record stays the truth - a hit is only
//                           where to look, and the name inside it settles
//                           whether it matches.
//     fs:i:<id> / fs:c:<id>:<n>  the leaf bytes, verbatim FS machinery. A leaf
//                           node carries the FS inode id and FS reads it the
//                           way it reads any file. The inode id comes from the
//                           "fs" sequence, the same one FS files take theirs
//                           from - both write these keys, so they have to
//                           share one counter (TODO 407).
//
// Node 0 is the root: an interior node with no edges pointing at it. Paths
// resolve from it one segment at a time, each step taking the first-created
// edge with that name. Interior nodes may be linked anywhere, including under
// themselves - cycles are allowed and traversals carry a visited set. Leaves
// are terminal: they cannot have children.
//
// WHAT KEEPS A NODE: a route from the root. refs is the number of edges that
// name a node, kept exact under the per-space write lock, but with cycles
// allowed a count can't decide what lives - a directory linked under itself
// never gets to zero. So UNLINK and RM RECURSIVE finish with a sweep: anything
// the removed edges were the last route to goes, cycles included, and anything
// still held from outside keeps its node with its count brought up to date.
//
#include <cstdint>
#include <string>
#include <vector>

#include "foreign/driver.h"
#include "key_space.h"

namespace barch::graph {

using node_id = uint64_t;
using edge_id = uint64_t;

/** no node: what resolution answers when a path does not exist. */
constexpr node_id no_node = 0;

struct node {
    /** the id this was read as. */
    node_id id{0};
    /** true for structure-only (directory-like), false for content (file-like). */
    bool dir{false};
    /** how many edges name this node. The root reads as 1 and never drops. */
    uint64_t refs{0};
    /** leaf payload, mirrored from the FS inode so a listing needs no second read. */
    uint64_t size{0};
    uint64_t chunk{0};
    uint64_t chunks{0};
    uint64_t version{0};
    std::string type;
    /** the FS inode id behind a leaf, 0 for an interior node. */
    uint64_t inode{0};
};

struct edge {
    edge_id id{0};
    node_id parent{0};
    node_id child{0};
    /** the name under the parent. May repeat: duplicates are distinct edges. */
    std::string name;
};

/**
 * The read view the graph resolves through - the space plus the caller's
 * rights. Graph key names are exact and never go through the space's
 * key_split/composite encoding (a name may hold a space), so this converts
 * with convert() rather than encode_key. See graph.cpp.
 */
struct view {
    key_space_ptr space;
    bool may_read{false};
    bool may_write{false};
    bool get(const std::string& key, std::string& value) const;
    void range(const std::string& lo, const std::string& hi, int64_t limit,
               heap::vector<std::string>& out) const;
};

/**
 * Absolute, '/' separated, no `..`, no NUL, no empty segment, no trailing
 * slash - the same rule FS paths follow, minus the storage. False fills `err`.
 */
bool normalise(const std::string& in, std::string& out, std::string& err);

/** the keys, exposed because the tests name them the way fstest.py does. */
std::string node_key(node_id id);
std::string edge_key(node_id parent, edge_id edge);

/** split a clean path into its segments. Empty is the root itself. */
std::vector<std::string> split(const std::string& clean);

/**
 * Read one node. False is "no such node". The root (id 0) always reads as an
 * interior node and never touches the store.
 */
bool stat_node(const view& acc, node_id id, node& out);
/** all edges (parent, name) in creation order - creation order is edge id order. */
bool edges_for(const view& acc, node_id parent, const std::string& name,
               std::vector<edge>& out);
/**
 * Every edge under a parent, in edge id order, all of them. `after` starts past
 * that edge id, which is how LS pages: the id is the cursor, so a name that
 * repeats or an edge that went away between pages can't throw it off.
 */
bool children_of(const view& acc, node_id parent, std::vector<edge>& out,
                 edge_id after = 0);
/** how many edges name this node, counted off the reverse index. */
uint64_t refcount(const view& acc, node_id id);

/**
 * Resolve a clean path to its node, first-created edge winning at every step.
 * False is "no such path". `edge_out` optionally takes the final edge. `pick`,
 * when not 0, is the id of the final edge to take instead - how a duplicate
 * that the first-created one hides can still be named.
 */
bool resolve(const view& acc, const std::string& clean, node& out,
             edge* edge_out = nullptr, edge_id pick = 0);
/** resolve the parent of a clean path: the node plus the final segment name. */
bool resolve_parent(const view& acc, const std::string& clean, node& out,
                    std::string& name);

/**
 * Writes that land together, or not at all. Ids are reserved once, before any
 * latch is taken - see ids.h for why that ordering is not optional - and the
 * whole commit runs under a per-space write lock, so the counts and the
 * reachability it works out can't be changed underneath it by another writer.
 *
 * A batch may hold any number of mkdir and write items, and a later one may
 * sit under a directory an earlier one makes. A link, unlink, remove, rename
 * or copy is a batch of its own.
 */
class batch {
public:
    explicit batch(const key_space_ptr& space);
    /** a fresh interior node at `path`. Ids are handed out at commit. */
    node_id mkdir(const std::string& path);
    /** a leaf at `path` holding `body`, created or overwritten. */
    node_id write(const std::string& path, std::string body, const std::string& type,
                  size_t chunk = 0);
    /** a second edge onto an existing node: what makes one node answer twice. */
    bool link(node_id child, const std::string& path, std::string& err);
    /**
     * Remove one edge (UNLINK). Whatever that edge was the last route to goes
     * with it: a leaf's bytes, or a directory and everything under it that
     * nothing else holds. `pick` names the edge by id rather than by age.
     */
    bool unlink(const std::string& path, std::string& err, edge_id pick = 0);
    /**
     * Remove the node at `path` and every edge naming it anywhere (RM).
     * With `recursive`, an interior node takes its subtree too: every node
     * under it that nothing outside still holds.
     */
    bool remove(const std::string& path, bool recursive, std::string& err, edge_id pick = 0);
    /**
     * Move one edge to another path. Only the edge record moves - the node id
     * behind it does not change, so a shared node stays shared under its new
     * name and the other paths never notice.
     */
    bool rename(const std::string& from, const std::string& to, std::string& err,
                edge_id pick = 0);
    /**
     * Copy the directory at `from` to `to`, which must not exist yet. The copy
     * has the same shape as the original: one fresh node per node reachable
     * from `from`, one fresh edge per edge between them, fresh bytes per leaf.
     * A node shared inside the subtree comes out shared inside the copy, and a
     * cycle comes out as a cycle, so nothing is dropped and nothing repeats.
     */
    bool copy(const std::string& from, const std::string& to, std::string& err);
    bool commit(std::string& err);

    /** the node ids created, once commit has succeeded. */
    const std::vector<node_id>& created() const { return done; }

private:
    key_space_ptr space;
    struct item {
        enum class kind { make_dir, put, link, unlink, remove, rename, copy } what;
        std::string path;
        std::string body;
        std::string type;
        size_t chunk{0};
        node_id child{0};
        bool recursive{false};
        std::string to;
        edge_id pick{0};
    };
    std::vector<item> pending;
    std::vector<node_id> done;
};

/**
 * Breadth first walk from `start`, in visit order. Each hit is the node id and
 * the path taken to reach it; a node already seen is not listed again, which
 * is what keeps a cycle a walk rather than a loop. `depth` caps the distance
 * from the start, 0 meaning no cap. `limit` caps the hits, 0 meaning no cap;
 * `offset` leaves out that many first.
 *
 * The visited set and the queue live in a private one-shard scratch space -
 * see TODO 382 - so the walk's own bookkeeping is keys rather than memory. The
 * hits come back in `out`, though, so a caller walking something big pages it
 * with limit and offset.
 */
bool bfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err);

/**
 * The same, depth first (pre-order): a node, then its first child's whole
 * subtree, then the next child. A node reachable along several paths is
 * listed at the first one pre-order meets.
 */
bool dfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err);

/** the default chunk: the FS one, since the bytes land in FS chunk keys. */
constexpr size_t default_chunk = 65536;

}
