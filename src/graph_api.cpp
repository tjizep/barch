//
// GRAPH - the graph store over RESP. See TODO 382, TODO 407 and graph.h.
//
// Paths are not stored: the store is {parent_id, name, id} edges plus a node
// table, and one node may be edged from any number of parents. Leaves hold
// their bytes in the FS inode/chunk keys, so GET reads a graph leaf the way
// FS GET reads a file.
//
//   GRAPH LS <path> [AFTER edge] [OFFSET n] [LIMIT n]
//                                           a line per edge, one level. AFTER
//                                           takes the edge id off the last
//                                           line of the page before
//   GRAPH STAT <path> [EDGE id]             k=v, edge ids included
//   GRAPH GET <path> [FROM off] [LEN n] [EDGE id]
//                                           the leaf content, or nil
//   GRAPH PUT <path> <content> [TYPE t] [CHUNK n]
//   GRAPH MKDIR <path>
//   GRAPH LINK <node-id> <path>              a second edge onto a node
//   GRAPH UNLINK <path> [EDGE id]            one edge, and whatever it was the
//                                           last route to
//   GRAPH RM <path> [RECURSIVE] [EDGE id]    the node and every edge naming it
//   GRAPH MV <path> <to> [EDGE id]           one edge to another path
//   GRAPH CP <path> <to> [TYPE t] [CHUNK n]  a byte copy under another path; a
//                                           directory comes out whole, in the
//                                           same shape, and takes no options
//   GRAPH BFS <start> [DEPTH n] [OFFSET n] [LIMIT n]
//   GRAPH DFS <start> [DEPTH n] [OFFSET n] [LIMIT n]
//                                           `id path` per visited node
//
// EDGE takes the last step of a path by edge id instead of by age. With a
// name that repeats, the first-created edge is the one a path finds, and EDGE
// is how the others are reached.
//
// Flat replies on purpose, the way FS LS does: the module caller cannot send
// a nested array, so a line per row is what already works.
//
#include "graph.h"

#include "caller.h"
#include "fs.h"
#include "function_api.h"
#include "graph_api.h"
#include "module.h"
#include "vk_caller.h"

#include <cstdlib>
#include <initializer_list>
#include <unordered_map>

namespace graph_api_anon {

using graph_opts = std::unordered_map<std::string, std::string>;

std::string graph_as_text(art::value_type v) {
    return {v.chars(), v.size};
}

std::string graph_upper(std::string s) {
    for (auto& c : s)
        c = (char) toupper((unsigned char) c);
    return s;
}

/**
 * NAME VALUE pairs from `at` on, each NAME one of `names`. Anything else is an
 * error, a word left over at the end included - GET, PUT, CP and the walks
 * used to skip a stray trailing word without a sound.
 */
bool graph_options(const arg_t& argv, size_t at, std::initializer_list<const char*> names,
                   graph_opts& out) {
    if (at > argv.size() || (argv.size() - at) % 2 != 0)
        return false;
    for (; at + 1 < argv.size(); at += 2) {
        std::string word = graph_upper(graph_as_text(argv[at]));
        bool known = false;
        for (const char* n : names)
            known = known || word == n;
        if (!known)
            return false;
        out[word] = graph_as_text(argv[at + 1]);
    }
    return true;
}

bool graph_whole_number(const std::string& s, uint64_t& out) {
    if (s.empty() || s.size() > 20 || s.find_first_not_of("0123456789") != std::string::npos)
        return false;
    out = strtoull(s.c_str(), nullptr, 10);
    return true;
}

/** a whole-number option, when it's there. False only for one that's malformed. */
bool graph_number_opt(const graph_opts& opt, const char* name, uint64_t& out) {
    auto it = opt.find(name);
    return it == opt.end() || graph_whole_number(it->second, out);
}

/** EDGE's value, when it's there: an edge id, and edge ids start at 1. */
bool graph_edge_opt(const graph_opts& opt, uint64_t& pick) {
    return graph_number_opt(opt, "EDGE", pick) && !(opt.count("EDGE") && pick == 0);
}

std::string graph_ls_line(const barch::graph::edge& e, const barch::graph::node& n) {
    // four fields and then the name, which is the only one that can hold a
    // space. The edge id is what AFTER pages by and what EDGE picks.
    return std::string(n.dir ? "dir" : "leaf") + " " + std::to_string(e.id) + " " +
           std::to_string(n.id) + " " + std::to_string(n.refs) + " " + e.name;
}

std::string graph_stat_line(const std::string& path, const barch::graph::node& n,
                      const barch::graph::edge* e) {
    std::string line = "path=" + path +
                       " kind=" + (n.dir ? "dir" : "leaf") +
                       " id=" + std::to_string(n.id) +
                       " refs=" + std::to_string(n.refs);
    if (e)
        line += " edge=" + std::to_string(e->id);
    if (!n.dir) {
        line += " size=" + std::to_string(n.size) +
                " chunk=" + std::to_string(n.chunk) +
                " chunks=" + std::to_string(n.chunks) +
                " version=" + std::to_string(n.version) +
                " type=" + n.type;
    }
    return line;
}

/** read the leaf's bytes through the inode it carries; an empty leaf has none. */
bool graph_leaf_bytes(const barch::foreign::store_access& owner, const barch::graph::node& n,
                      std::string& body, std::string& err) {
    body.clear();
    if (!n.inode)
        return true;
    barch::fs::file f;
    return barch::fs::file::open_id(owner, n.inode, f, err) &&
           f.read_at(0, f.meta().size, body, err);
}

} // namespace graph_api_anon

using namespace graph_api_anon;

int GRAPH(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    std::string sub = graph_upper(graph_as_text(argv[1]));
    auto space = call.kspace();
    // The RESP command runs as the connection's user - the dispatch checked
    // the GRAPH categories before this ran - so the graph view carries full
    // rights. Per-key ACLs inside the graph are future work; the space
    // boundary is the one enforced here.
    barch::graph::view acc;
    acc.space = space;
    acc.may_read = acc.may_write = true;
    auto owner = barch::functions::store_for_owner(space);

    if (sub == "LS") {
        if (argv.size() < 3)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t after = 0, offset = 0, limit = 0;
        if (!graph_options(argv, 3, {"AFTER", "OFFSET", "LIMIT"}, opt))
            return call.push_error("GRAPH LS path [AFTER edge] [OFFSET n] [LIMIT n]");
        // AFTER is an edge id rather than a name: with a name that repeats, a
        // name can't say which of its edges a page stopped at, and a cursor
        // name that went away between pages ended the listing (TODO 407)
        if (!graph_number_opt(opt, "AFTER", after))
            return call.push_error("AFTER is the edge id off the last line of a page");
        if (!graph_number_opt(opt, "OFFSET", offset))
            return call.push_error("OFFSET is a whole number, not negative");
        if (!graph_number_opt(opt, "LIMIT", limit))
            return call.push_error("LIMIT is a whole number, not negative");
        std::string clean, err;
        if (!barch::graph::normalise(graph_as_text(argv[2]), clean, err))
            return call.push_error(err.c_str());
        barch::graph::node here;
        if (!barch::graph::resolve(acc, clean, here))
            return call.push_error("no such path");
        if (!here.dir)
            return call.push_error("that is a leaf");
        std::vector<barch::graph::edge> kids;
        barch::graph::children_of(acc, here.id, kids, after);
        call.start_array();
        size_t skipped = 0, sent = 0;
        for (const auto& e : kids) {
            if (limit && sent >= limit)
                break;
            barch::graph::node n;
            if (!barch::graph::stat_node(acc, e.child, n))
                continue;
            if (skipped < offset) {
                ++skipped;
                continue;
            }
            call.push_string(graph_ls_line(e, n));
            ++sent;
        }
        return call.end_array();
    }
    if (sub == "STAT") {
        if (argv.size() < 3)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t pick = 0;
        if (!graph_options(argv, 3, {"EDGE"}, opt) || !graph_edge_opt(opt, pick))
            return call.push_error("GRAPH STAT path [EDGE id]");
        std::string clean, err;
        if (!barch::graph::normalise(graph_as_text(argv[2]), clean, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        barch::graph::edge e;
        if (!barch::graph::resolve(acc, clean, n, &e, pick))
            return call.push_null();
        return call.push_string(graph_stat_line(clean, n, clean == "/" ? nullptr : &e));
    }
    if (sub == "GET") {
        if (argv.size() < 3)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t pick = 0, at_off = 0, want = 0;
        if (!graph_options(argv, 3, {"FROM", "LEN", "EDGE"}, opt) || !graph_edge_opt(opt, pick))
            return call.push_error("GRAPH GET path [FROM off] [LEN n] [EDGE id]");
        if (!graph_number_opt(opt, "FROM", at_off))
            return call.push_error("FROM is a whole number, not negative");
        if (!graph_number_opt(opt, "LEN", want))
            return call.push_error("LEN is a whole number, not negative");
        std::string clean, err;
        if (!barch::graph::normalise(graph_as_text(argv[2]), clean, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        if (!barch::graph::resolve(acc, clean, n, nullptr, pick))
            return call.push_null();
        if (n.dir)
            return call.push_error("that is a directory");
        if (!n.inode)
            return call.push_string(std::string());
        barch::fs::file f;
        if (!barch::fs::file::open_id(owner, n.inode, f, err))
            return call.push_null();
        if (!opt.count("LEN"))
            want = f.meta().size;
        std::string body;
        if (!f.read_at(at_off, want, body, err))
            return call.push_error(err.c_str());
        return call.push_string(body);
    }
    if (sub == "PUT") {
        if (argv.size() < 4)
            return call.wrong_arity();
        graph_opts opt;
        if (!graph_options(argv, 4, {"TYPE", "CHUNK"}, opt))
            return call.push_error("GRAPH PUT path content [TYPE t] [CHUNK n]");
        size_t chunk = 0;
        std::string err;
        if (!barch::fs::parse_chunk(opt["CHUNK"], chunk, err))
            return call.push_error(err.c_str());
        barch::graph::batch b(space);
        b.write(graph_as_text(argv[2]), graph_as_text(argv[3]), opt["TYPE"], chunk);
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "MKDIR") {
        if (argv.size() != 3)
            return call.wrong_arity();
        barch::graph::batch b(space);
        b.mkdir(graph_as_text(argv[2]));
        std::string err;
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "LINK") {
        if (argv.size() != 4)
            return call.wrong_arity();
        uint64_t id = 0;
        if (!graph_whole_number(graph_as_text(argv[2]), id) || id == 0)
            return call.push_error("LINK takes a node id and a path");
        barch::graph::batch b(space);
        std::string err;
        if (!b.link(id, graph_as_text(argv[3]), err))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "UNLINK") {
        if (argv.size() < 3)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t pick = 0;
        if (!graph_options(argv, 3, {"EDGE"}, opt) || !graph_edge_opt(opt, pick))
            return call.push_error("GRAPH UNLINK path [EDGE id]");
        barch::graph::batch b(space);
        std::string err;
        if (!b.unlink(graph_as_text(argv[2]), err, pick))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "RM") {
        if (argv.size() < 3)
            return call.wrong_arity();
        bool recursive = false;
        uint64_t pick = 0;
        for (size_t at = 3; at < argv.size(); ++at) {
            std::string word = graph_upper(graph_as_text(argv[at]));
            if (word == "RECURSIVE" && !recursive) {
                recursive = true;
                continue;
            }
            if (word == "EDGE" && !pick && at + 1 < argv.size() &&
                graph_whole_number(graph_as_text(argv[at + 1]), pick) && pick) {
                ++at;
                continue;
            }
            return call.push_error("GRAPH RM path [RECURSIVE] [EDGE id]");
        }
        barch::graph::batch b(space);
        std::string err;
        if (!b.remove(graph_as_text(argv[2]), recursive, err, pick))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "MV") {
        if (argv.size() < 4)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t pick = 0;
        if (!graph_options(argv, 4, {"EDGE"}, opt) || !graph_edge_opt(opt, pick))
            return call.push_error("GRAPH MV path to [EDGE id]");
        barch::graph::batch b(space);
        std::string err;
        if (!b.rename(graph_as_text(argv[2]), graph_as_text(argv[3]), err, pick))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "CP") {
        // a byte copy, never a shared node: LINK is the verb that shares.
        if (argv.size() < 4)
            return call.wrong_arity();
        graph_opts opt;
        if (!graph_options(argv, 4, {"TYPE", "CHUNK"}, opt))
            return call.push_error("GRAPH CP from to [TYPE t] [CHUNK n]");
        std::string from = graph_as_text(argv[2]);
        std::string to = graph_as_text(argv[3]);
        size_t chunk = 0;
        std::string clean_from, err;
        if (!barch::fs::parse_chunk(opt.count("CHUNK") ? opt["CHUNK"] : "", chunk, err))
            return call.push_error(err.c_str());
        if (!barch::graph::normalise(from, clean_from, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        if (!barch::graph::resolve(acc, clean_from, n))
            return call.push_error("no such path");
        barch::graph::batch b(space);
        if (n.dir) {
            // The whole subtree, in one commit and in its own shape: shared
            // nodes stay shared and cycles stay cycles. The old copy replayed
            // a walk path by path, and the batch couldn't see a directory it
            // was making itself, so any directory with something in it failed
            // with "no such path" (TODO 407).
            if (opt.count("TYPE") || opt.count("CHUNK"))
                return call.push_error("TYPE and CHUNK are for a leaf copy");
            if (!b.copy(from, to, err))
                return call.push_error(err.c_str());
        } else {
            std::string body;
            if (!graph_leaf_bytes(owner, n, body, err))
                return call.push_error(err.c_str());
            // the source's chunk unless asked otherwise - one a bad CHUNK left
            // out of range falls back to the default rather than failing the copy
            size_t keep = (n.chunk && n.chunk <= barch::fs::max_chunk) ? (size_t) n.chunk : 0;
            b.write(to, body, opt.count("TYPE") ? opt["TYPE"] : n.type, chunk ? chunk : keep);
        }
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "BFS" || sub == "DFS") {
        if (argv.size() < 3)
            return call.wrong_arity();
        graph_opts opt;
        uint64_t depth = 0, limit = 0, offset = 0;
        if (!graph_options(argv, 3, {"DEPTH", "OFFSET", "LIMIT"}, opt))
            return call.push_error("GRAPH BFS|DFS start [DEPTH n] [OFFSET n] [LIMIT n]");
        if (!graph_number_opt(opt, "DEPTH", depth))
            return call.push_error("DEPTH is a whole number, not negative");
        if (!graph_number_opt(opt, "OFFSET", offset))
            return call.push_error("OFFSET is a whole number, not negative");
        if (!graph_number_opt(opt, "LIMIT", limit))
            return call.push_error("LIMIT is a whole number, not negative");
        std::string start = graph_as_text(argv[2]);
        std::vector<std::pair<barch::graph::node_id, std::string>> hits;
        std::string err;
        bool ok = (sub == "BFS")
            ? barch::graph::bfs(space, start, depth, hits, (size_t) limit, (size_t) offset, err)
            : barch::graph::dfs(space, start, depth, hits, (size_t) limit, (size_t) offset, err);
        if (!ok)
            return call.push_error(err.c_str());
        call.start_array();
        for (const auto& [id, p] : hits)
            call.push_string(std::to_string(id) + " " + p);
        return call.end_array();
    }
    return call.push_error("GRAPH LS|STAT|GET|PUT|MKDIR|LINK|UNLINK|RM|MV|CP|BFS|DFS");
}

int cmd_GRAPH(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, GRAPH);
}

void register_graph_api(function_map& r) {
    // one command for both halves, the way FS does it - a read only caller
    // cannot have GRAPH at all. Splitting a read verb from the writes is the
    // way out if that turns out to matter.
    r["GRAPH"] = {::GRAPH, {"read", "write", "keys", "data"}};
}
