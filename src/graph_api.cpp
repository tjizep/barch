//
// GRAPH - the graph store over RESP. See TODO 382 and graph.h.
//
// Paths are not stored: the store is {parent_id, name, id} edges plus a node
// table, and one node may be edged from any number of parents. Leaves hold
// their bytes in the FS inode/chunk keys, so GET reads a graph leaf the way
// FS GET reads a file.
//
//   GRAPH LS <path> [AFTER name] [OFFSET n] [LIMIT n]
//                                           a line per edge, one level
//   GRAPH STAT <path>                       k=v, edge ids included
//   GRAPH GET <path> [FROM off] [LEN n]     the leaf content, or nil
//   GRAPH PUT <path> <content> [TYPE t] [CHUNK n]
//   GRAPH MKDIR <path>
//   GRAPH LINK <node-id> <path>              a second edge onto a node
//   GRAPH UNLINK <path>                      one edge; the node goes with its last
//   GRAPH RM <path> [RECURSIVE]              the node and every edge naming it
//   GRAPH MV <path> <to>                     one edge to another path
//   GRAPH CP <path> <to>                     a byte copy under another path
//   GRAPH BFS <start> [DEPTH n] [OFFSET n] [LIMIT n]
//   GRAPH DFS <start> [DEPTH n] [OFFSET n] [LIMIT n]
//                                           `id path` per visited node
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

namespace graph_api_anon {

std::string graph_as_text(art::value_type v) {
    return {v.chars(), v.size};
}

std::string graph_upper(std::string s) {
    for (auto& c : s)
        c = (char) toupper((unsigned char) c);
    return s;
}

/** an optional NAME VALUE pair after the positional arguments. */
bool graph_option_at(const arg_t& argv, size_t at, const char* name, std::string& value) {
    if (at + 1 >= argv.size())
        return false;
    if (graph_upper(graph_as_text(argv[at])) != name)
        return false;
    value = graph_as_text(argv[at + 1]);
    return true;
}

bool graph_whole_number(const std::string& s, uint64_t& out) {
    if (s.empty() || s.find_first_not_of("0123456789") != std::string::npos)
        return false;
    out = strtoull(s.c_str(), nullptr, 10);
    return true;
}

std::string graph_ls_line(const barch::graph::edge& e, const barch::graph::node& n) {
    // four fields and then the name, which is the only one that can hold a
    // space. The edge id is what LINK/UNLINK work with when a name repeats.
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

struct graph_page_opts {
    std::string after;
    size_t limit{0};
    size_t offset{0};
};

/** parse [AFTER name] [OFFSET n] [LIMIT n] from `at` onwards. */
std::string graph_read_page(const arg_t& argv, size_t at, graph_page_opts& out) {
    for (; at + 1 < argv.size(); at += 2) {
        std::string v;
        if (graph_option_at(argv, at, "AFTER", v)) {
            out.after = v;
        } else if (graph_option_at(argv, at, "OFFSET", v)) {
            uint64_t n = 0;
            if (!graph_whole_number(v, n))
                return "OFFSET is a whole number, not negative";
            out.offset = (size_t) n;
        } else if (graph_option_at(argv, at, "LIMIT", v)) {
            uint64_t n = 0;
            if (!graph_whole_number(v, n))
                return "LIMIT is a whole number, not negative";
            out.limit = (size_t) n;
        } else {
            return "expected AFTER, OFFSET or LIMIT";
        }
    }
    if (at < argv.size())
        return "an option needs a value";
    return {};
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
        std::string path = graph_as_text(argv[2]);
        graph_page_opts opt;
        auto bad = graph_read_page(argv, 3, opt);
        if (!bad.empty())
            return call.push_error(bad.c_str());
        std::string clean, err;
        if (!barch::graph::normalise(path, clean, err))
            return call.push_error(err.c_str());
        barch::graph::node here;
        if (!barch::graph::resolve(acc, clean, here))
            return call.push_error("no such path");
        if (!here.dir)
            return call.push_error("that is a leaf");
        std::vector<barch::graph::edge> kids;
        if (!barch::graph::children_of(acc, here.id, kids))
            return call.push_error("that is a leaf");
        call.start_array();
        size_t skipped = 0, sent = 0;
        bool past_after = opt.after.empty();
        // AFTER names an edge, not a node: with duplicate names the cursor
        // is ambiguous, so it restarts past the last edge carrying it.
        uint64_t after_edge = 0;
        for (const auto& e : kids) {
            if (!past_after) {
                if (e.name == opt.after)
                    after_edge = e.id;
                continue;
            }
            if (skipped < opt.offset) {
                ++skipped;
                continue;
            }
            if (opt.limit && sent >= opt.limit)
                break;
            barch::graph::node n;
            if (!barch::graph::stat_node(acc, e.child, n))
                continue;
            call.push_string(graph_ls_line(e, n));
            ++sent;
        }
        // second pass when AFTER matched: everything past its last edge.
        if (!opt.after.empty()) {
            bool resume = after_edge == 0;  // no such name: empty, not everything
            if (after_edge != 0) {
                for (const auto& e : kids) {
                    if (!resume) {
                        if (e.id == after_edge)
                            resume = true;
                        continue;
                    }
                    if (skipped < opt.offset) {
                        ++skipped;
                        continue;
                    }
                    if (opt.limit && sent >= opt.limit)
                        break;
                    barch::graph::node n;
                    if (!barch::graph::stat_node(acc, e.child, n))
                        continue;
                    call.push_string(graph_ls_line(e, n));
                    ++sent;
                }
            }
        }
        return call.end_array();
    }
    if (sub == "STAT") {
        if (argv.size() != 3)
            return call.wrong_arity();
        std::string clean, err;
        if (!barch::graph::normalise(graph_as_text(argv[2]), clean, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        barch::graph::edge e;
        if (!barch::graph::resolve(acc, clean, n, &e))
            return call.push_null();
        return call.push_string(graph_stat_line(clean, n, clean == "/" ? nullptr : &e));
    }
    if (sub == "GET") {
        if (argv.size() < 3)
            return call.wrong_arity();
        std::string from, len;
        for (size_t at = 3; at + 1 < argv.size(); at += 2) {
            if (!graph_option_at(argv, at, "FROM", from) && !graph_option_at(argv, at, "LEN", len))
                return call.push_error("GRAPH GET path [FROM off] [LEN n]");
        }
        std::string clean, err;
        if (!barch::graph::normalise(graph_as_text(argv[2]), clean, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        if (!barch::graph::resolve(acc, clean, n))
            return call.push_null();
        if (n.dir)
            return call.push_error("that is a directory");
        if (!n.inode)
            return call.push_string(std::string());
        barch::fs::file f;
        if (!barch::fs::file::open_id(owner, n.inode, f, err))
            return call.push_null();
        uint64_t at_off = 0, want = f.meta().size;
        if (!from.empty() && !graph_whole_number(from, at_off))
            return call.push_error("FROM is a whole number, not negative");
        if (!len.empty() && !graph_whole_number(len, want))
            return call.push_error("LEN is a whole number, not negative");
        std::string body;
        if (!f.read_at(at_off, want, body, err))
            return call.push_error(err.c_str());
        return call.push_string(body);
    }
    if (sub == "PUT") {
        if (argv.size() < 4)
            return call.wrong_arity();
        std::string path = graph_as_text(argv[2]);
        std::string content = graph_as_text(argv[3]);
        std::string type, chunk;
        for (size_t at = 4; at + 1 < argv.size(); at += 2) {
            if (!graph_option_at(argv, at, "TYPE", type) && !graph_option_at(argv, at, "CHUNK", chunk))
                return call.push_error("GRAPH PUT path content [TYPE t] [CHUNK n]");
        }
        barch::graph::batch b(space);
        b.write(path, content, type,
                chunk.empty() ? 0 : (size_t) strtoull(chunk.c_str(), nullptr, 10));
        std::string err;
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
        if (argv.size() != 3)
            return call.wrong_arity();
        barch::graph::batch b(space);
        std::string err;
        if (!b.unlink(graph_as_text(argv[2]), err))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "RM") {
        if (argv.size() < 3 || argv.size() > 4)
            return call.wrong_arity();
        bool recursive = false;
        if (argv.size() == 4) {
            if (graph_upper(graph_as_text(argv[3])) != "RECURSIVE")
                return call.push_error("GRAPH RM path [RECURSIVE]");
            recursive = true;
        }
        barch::graph::batch b(space);
        std::string err;
        if (!b.remove(graph_as_text(argv[2]), recursive, err))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "MV") {
        if (argv.size() != 4)
            return call.wrong_arity();
        barch::graph::batch b(space);
        std::string err;
        if (!b.rename(graph_as_text(argv[2]), graph_as_text(argv[3]), err))
            return call.push_error(err.c_str());
        if (!b.commit(err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "CP") {
        // a byte copy, never a shared node: LINK is the verb that shares.
        if (argv.size() < 4)
            return call.wrong_arity();
        std::string from = graph_as_text(argv[2]);
        std::string to = graph_as_text(argv[3]);
        std::string type, chunk;
        for (size_t at = 4; at + 1 < argv.size(); at += 2) {
            if (!graph_option_at(argv, at, "TYPE", type) && !graph_option_at(argv, at, "CHUNK", chunk))
                return call.push_error("GRAPH CP from to [TYPE t] [CHUNK n]");
        }
        std::string clean_from, err;
        if (!barch::graph::normalise(from, clean_from, err))
            return call.push_error(err.c_str());
        barch::graph::node n;
        if (!barch::graph::resolve(acc, clean_from, n))
            return call.push_error("no such path");
        if (n.dir) {
            // a subtree copy is a walk that replays: parents before children,
            // which is what DFS pre-order gives. Cycles are refused rather
            // than copied forever - a cycle has no tree copy.
            std::vector<std::pair<barch::graph::node_id, std::string>> order;
            if (!barch::graph::dfs(space, clean_from, 0, order, 0, 0, err))
                return call.push_error(err.c_str());
            std::unordered_map<barch::graph::node_id, int> seen;
            for (const auto& [id, p] : order) {
                if (!seen.emplace(id, 0).second)
                    return call.push_error("cannot copy a cycle");
            }
            std::string clean_to;
            if (!barch::graph::normalise(to, clean_to, err))
                return call.push_error(err.c_str());
            barch::graph::batch b(space);
            for (const auto& [id, p] : order) {
                std::string rel = p.substr(clean_from.size());
                std::string dst = clean_to + rel;
                barch::graph::node src;
                if (!barch::graph::resolve(acc, p, src))
                    return call.push_error("the source changed underneath");
                if (src.dir)
                    b.mkdir(dst);
                else {
                    // re-read the bytes through the inode the node carries.
                    std::string body;
                    if (src.inode) {
                        barch::fs::file f;
                        if (!barch::fs::file::open_id(owner, src.inode, f, err))
                            return call.push_error(err.c_str());
                        if (!f.read_at(0, f.meta().size, body, err))
                            return call.push_error(err.c_str());
                    }
                    b.write(dst, body, src.type, src.chunk);
                }
            }
            if (!b.commit(err))
                return call.push_error(err.empty() ? "no such path" : err.c_str());
            return call.push_simple("OK");
        }
        std::string body;
        if (n.inode) {
            barch::fs::file f;
            if (!barch::fs::file::open_id(owner, n.inode, f, err))
                return call.push_error(err.c_str());
            if (!f.read_at(0, f.meta().size, body, err))
                return call.push_error(err.c_str());
        }
        barch::graph::batch b(space);
        b.write(to, body, type.empty() ? n.type : type,
                chunk.empty() ? n.chunk : (size_t) strtoull(chunk.c_str(), nullptr, 10));
        if (!b.commit(err))
            return call.push_error(err.empty() ? "no such path" : err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "BFS" || sub == "DFS") {
        if (argv.size() < 3)
            return call.wrong_arity();
        std::string start = graph_as_text(argv[2]);
        uint64_t depth = 0, limit = 0, offset = 0;
        for (size_t at = 3; at + 1 < argv.size(); at += 2) {
            std::string v;
            uint64_t n = 0;
            if (graph_option_at(argv, at, "DEPTH", v)) {
                if (!graph_whole_number(v, n))
                    return call.push_error("DEPTH is a whole number, not negative");
                depth = n;
            } else if (graph_option_at(argv, at, "OFFSET", v)) {
                if (!graph_whole_number(v, n))
                    return call.push_error("OFFSET is a whole number, not negative");
                offset = (size_t) n;
            } else if (graph_option_at(argv, at, "LIMIT", v)) {
                if (!graph_whole_number(v, n))
                    return call.push_error("LIMIT is a whole number, not negative");
                limit = (size_t) n;
            } else {
                return call.push_error("GRAPH BFS|DFS start [DEPTH n] [OFFSET n] [LIMIT n]");
            }
        }
        std::vector<std::pair<barch::graph::node_id, std::string>> hits;
        std::string err;
        bool ok = (sub == "BFS")
            ? barch::graph::bfs(space, start, depth, hits, limit, offset, err)
            : barch::graph::dfs(space, start, depth, hits, limit, offset, err);
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
