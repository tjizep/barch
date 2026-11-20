// Raft's gate, every combination of what it is told - TODO 937.
//
// src/raft_gate.h decides what a command in a space replicated with Raft does:
// refused, into the log, a write on the leader, or a read under the read rules.
// This runs every combination of its inputs through it and checks:
//   - a write that changes one node's copy only is refused;
//   - a logged command never runs on one node alone;
//   - a write to the space, on a node that doesn't lead its one group, never runs;
//   - a read of the space always goes under the read rules;
//   - a command that changes the server runs with no check;
//   - writes are gathered only in a space split over several groups.
//
// It also runs rules that are known to be wrong and checks that it catches them,
// so a model that checks nothing fails too:
//   - a read subcommand taken as a server one: the rule before TODO 937, when a
//     follower answered INDEX FIND from its own copy;
//   - no leader check on a write;
//   - a logged command run here only;
//   - a local command let run.
//
// The table's side is checked too: which class raft_writes::of gives INDEX's
// words, and that CLUSTER WRITES lists them as of() decides them.
#include "raft_gate.h"
#include "raft_writes.h"

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

using namespace barch::raft_writes;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    using gate_fn = std::function<gate_out(const gate_in&)>;

    // every input, each field over all it can be
    void every_input(const std::function<void(const gate_in&)>& f) {
        const stored fns[] = {stored::none, stored::read_only, stored::log, stored::effects};
        const kind ks[] = {kind::replicated, kind::server, kind::logged, kind::local, kind::reads};
        for (auto fn : fns)
            for (auto k : ks)
                for (unsigned bits = 0; bits < (1u << 11); ++bits) {
                    gate_in in;
                    in.fn = fn;
                    in.k = k;
                    in.builtin = bits & 1;
                    in.is_write = bits & 2;
                    in.is_data = bits & 4;
                    in.kspace = bits & 8;
                    in.split = bits & 16;
                    in.one_point = bits & 32;
                    in.install = bits & 64;
                    in.tx_layer = bits & 128;
                    in.leader = bits & 256;
                    in.committx = bits & 512;
                    in.tx_function = bits & 1024;
                    // a stored function is not a builtin
                    if (in.fn != stored::none && in.builtin)
                        continue;
                    f(in);
                }
    }

    // the table decides a builtin write that isn't KSPACE
    bool tabled(const gate_in& in) {
        return in.fn == stored::none && in.builtin && in.is_write && !in.kspace;
    }

    /** the first rule an input breaks, or empty */
    std::string fault(const gate_in& in, const gate_out& out) {
        const bool stored_fn = in.fn != stored::none;
        if (tabled(in) && in.k == kind::local && out.what != step::refuse_local)
            return "a local write ran";
        if (tabled(in) && in.k == kind::logged) {
            // SETF and REMF in a split space run here, their writes gathered into one batch
            const bool gathered = in.split && in.install && out.gather
                                  && (out.what == step::run || out.what == step::write);
            if (out.what != step::through_log && out.what != step::every_group && !gathered)
                return "a logged command ran on one node";
        }
        if (in.fn == stored::log && !in.split && out.what != step::through_log)
            return "a function that runs from the log ran here";
        if (in.fn == stored::log && in.split && !out.gather)
            return "a split space's function did not gather its writes";
        const bool writes_space = (stored_fn && in.fn != stored::read_only)
                                  || (tabled(in) && in.k == kind::replicated && in.is_data);
        if (writes_space && !in.split && !in.leader && !in.tx_layer && !in.committx && !in.tx_function
            && out.what != step::not_leader && out.what != step::through_log)
            return "a write ran on a node that doesn't lead";
        const bool reads_space = (in.fn == stored::read_only)
                                 || (in.fn == stored::none && in.builtin && !in.is_write && in.is_data)
                                 || (tabled(in) && in.k == kind::reads);
        if (reads_space && !in.tx_layer && out.what != step::read)
            return "a read of the space skipped the read rules";
        if (tabled(in) && in.k == kind::server && (out.what != step::run || out.gather))
            return "a server command was checked";
        if (out.gather && !in.split)
            return "gathered in a space of one group";
        return {};
    }

    /** the first fault over every input, with the input that showed it */
    std::string explore(const gate_fn& g, std::string* where = nullptr) {
        std::string first;
        every_input([&](const gate_in& in) {
            if (!first.empty())
                return;
            first = fault(in, g(in));
            if (!first.empty() && where)
                *where = std::string("fn ") + std::to_string((int) in.fn) + " kind "
                         + std::to_string((int) in.k) + (in.builtin ? " builtin" : "")
                         + (in.is_write ? " write" : "") + (in.is_data ? " data" : "")
                         + (in.split ? " split" : "") + (in.leader ? " leader" : "")
                         + (in.tx_layer ? " tx" : "");
        });
        return first;
    }

    size_t count_inputs() {
        size_t n = 0;
        every_input([&](const gate_in&) { ++n; });
        return n;
    }
}

int main() {
    std::printf("the gate directly\n");
    {
        gate_in in;
        in.builtin = in.is_write = true;
        in.k = kind::reads;
        check(gate(in).what == step::read, "INDEX FIND on a follower is a read - TODO 937");
        in.k = kind::server;
        check(gate(in).what == step::run, "a server command runs");
        in.k = kind::local;
        check(gate(in).what == step::refuse_local, "a local command is refused");
        in.k = kind::logged;
        check(gate(in).what == step::through_log, "a logged command goes in the log");
        in.split = in.one_point = true;
        check(gate(in).what == step::every_group, "BEGIN in a split space goes in every group's log");
        in.one_point = false;
        in.install = true;
        auto out = gate(in);
        check(out.what == step::run && out.gather, "SETF in a split space is gathered");
        in = {};
        in.builtin = in.is_write = in.is_data = true;
        in.k = kind::replicated;
        check(gate(in).what == step::not_leader, "SET on a follower is NOTLEADER");
        in.leader = true;
        check(gate(in).what == step::write, "SET on the leader is a write");
        in = {};
        in.fn = stored::read_only;
        check(gate(in).what == step::read, "a read-only function is a read");
        in.fn = stored::log;
        check(gate(in).what == step::through_log, "a writing function runs from the log");
    }

    std::printf("the table - TODO 937\n");
    {
        check(of("INDEX", "FIND", "") == kind::reads, "INDEX FIND reads");
        check(of("INDEX", "LIST", "") == kind::reads, "INDEX LIST reads");
        check(of("INDEX", "CHAINS", "") == kind::reads, "INDEX CHAINS reads");
        check(of("INDEX", "NOSUCH", "") == kind::reads, "an INDEX word it doesn't know is a read, not server");
        check(of("INDEX", "CREATE", "") == kind::logged, "INDEX CREATE is logged");
        bool listed_right = true;
        for (const auto& [sub, k] : subcommands("INDEX"))
            listed_right = listed_right && of("INDEX", sub, "") == k;
        check(listed_right, "CLUSTER WRITES lists INDEX's words as of() decides them");
        check(subcommands("INDEX").size() == 6, "and lists all six");
        check(std::string(name_of(kind::reads)) == "reads", "the class is named reads");
    }

    std::printf("every combination of inputs\n");
    {
        char line[128];
        std::snprintf(line, sizeof line, "as now (%zu inputs)", count_inputs());
        std::string where;
        const auto got = explore(gate, &where);
        if (!got.empty())
            std::printf("  %s: %s\n", got.c_str(), where.c_str());
        check(got.empty(), line);
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            gate_fn g;
        };
        const std::vector<mutant> mutants{
            {"a read subcommand taken as server (before TODO 937)", [](const gate_in& in) {
                auto m = in;
                if (m.k == kind::reads) m.k = kind::server;
                return gate(m);
            }},
            {"no leader check on a write", [](const gate_in& in) {
                auto out = gate(in);
                if (out.what == step::not_leader) out.what = step::write;
                return out;
            }},
            {"a logged command run here only", [](const gate_in& in) {
                auto out = gate(in);
                if (tabled(in) && in.k == kind::logged) out.what = step::write;
                return out;
            }},
            {"a local command let run", [](const gate_in& in) {
                auto out = gate(in);
                if (out.what == step::refuse_local) out.what = step::run;
                return out;
            }},
        };
        for (const auto& m : mutants) {
            const auto got = explore(m.g);
            char line[128];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
