#pragma once
//
// The graph store: nodes joined by edges, as ids rather than as paths.
// See TODO 382.
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
//     graph:layout          "2": 16-hex edge ids (layout "1" was 8-hex,
//                           refused rather than half read - an old 8-hex edge
//                           key would parse as a truncated 16-hex one)
//     graph:n:<id>          node -> {kind, refs, size, chunk, chunks, type, version}
//                           kind is "dir" or "leaf"; refs counts the edges that
//                           name it. size/chunk/chunks/type/version are the leaf
//                           payload and live in the FS inode/chunk keys below.
//     graph:e:<parent>:<edge>  edge -> {node, name}
//                           parent and edge both 16-hex. The name lives in
//                           the value so a duplicate (parent, name) pair is a
//                           second edge key rather than an overwrite.
//     fs:i:<id> / fs:c:<id>:<n>  the leaf bytes, verbatim FS machinery. A leaf
//                           node carries the FS inode id and FS reads it the
//                           way it reads any file - graph.cpp never touches a
//                           chunk key itself.
//
// Node 0 is the root: an interior node with no edges pointing at it. Paths
// resolve from it one segment at a time, each step taking the first-created
// edge with that name. Interior nodes may be linked anywhere, including under
// themselves - cycles are allowed and traversals carry a visited set. Leaves
// are terminal: they cannot have children.
//
// THE ONE INVARIANT: refs is the truth about reachability and the edge table
// is a hint. A node whose refs reaches zero has its payload dropped by the
// same commit. A writer that removes an edge and not the count has leaked a
// node; one that drops the count and not the edge has stranded one.
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
    /** the FS inode id behind a leaf, 0 for an interior node or an empty leaf. */
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
/** every edge under a parent, in edge id order. */
bool children_of(const view& acc, node_id parent, std::vector<edge>& out);
/** how many edges name this node - the refcount without reading the node. */
uint64_t refcount(const view& acc, node_id id);

/**
 * Resolve a clean path to its node, first-created edge winning at every step.
 * False is "no such path". `edge_out` optionally takes the final edge.
 */
bool resolve(const view& acc, const std::string& clean, node& out,
             edge* edge_out = nullptr);
/** resolve the parent of a clean path: the node plus the final segment name. */
bool resolve_parent(const view& acc, const std::string& clean, node& out,
                    std::string& name);

/**
 * Files written together, or not at all. Ids are reserved once, before any
 * latch is taken - see ids.h for why that ordering is not optional.
 */
class batch {
public:
    explicit batch(const key_space_ptr& space);
    /** a fresh interior node under `parent` called `name`. Answers its id. */
    node_id mkdir(const std::string& path);
    /** a fresh leaf under `parent` holding `body`. Answers its id. */
    node_id write(const std::string& path, std::string body, const std::string& type,
                  size_t chunk = 0);
    /** a second edge onto an existing node: what makes one node answer twice. */
    bool link(node_id child, const std::string& path, std::string& err);
    /** remove one edge; the node goes when its last edge does (UNLINK). */
    bool unlink(const std::string& path, std::string& err);
    /**
     * Remove the node at `path` and every edge naming it anywhere (RM).
     * With `recursive`, an interior node takes its whole subtree: every
     * descendant edge, and every descendant node left with no edges.
     */
    bool remove(const std::string& path, bool recursive, std::string& err);
    /**
     * Move one edge to another path. Only the edge record moves - the node id
     * behind it does not change, so a shared node stays shared under its new
     * name and the other paths never notice.
     */
    bool rename(const std::string& from, const std::string& to, std::string& err);
    bool commit(std::string& err);

    /** the node ids created, once commit has succeeded. */
    const std::vector<node_id>& created() const { return done; }

private:
    key_space_ptr space;
    struct item {
        enum class kind { make_dir, put, link, unlink, remove, rename } what;
        std::string path;
        std::string body;
        std::string type;
        size_t chunk{0};
        node_id child{0};
        bool recursive{false};
        std::string to;
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
 * see TODO 382 - so a walk over millions of nodes is keys, not memory.
 */
bool bfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err);

/** the same, depth first (pre-order): a whole subtree before the next sibling. */
bool dfs(const key_space_ptr& space, const std::string& start, uint64_t depth,
         std::vector<std::pair<node_id, std::string>>& out, size_t limit, size_t offset,
         std::string& err);

/** the default chunk: the FS one, since the bytes land in FS chunk keys. */
constexpr size_t default_chunk = 65536;

}
