// Raft groups over group_store, restarted from their files - TODO 610.
#include "cluster/raft_group.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using barch::cluster::raft_group;
using namespace nuraft;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    std::fflush(stdout);
    if (!ok) ++failures;
}

static bool wait_for(int ms, const std::function<bool()>& f) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < until) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return f();
}

// the values in commit order; a commit answers with its index, as raft_group expects
class list_machine : public state_machine {
public:
    ptr<buffer> commit(const ulong idx, buffer& data) override {
        buffer_serializer bs(data);
        const uint64_t v = bs.get_u64();
        {
            std::lock_guard l(m);
            values.push_back(v);
            last = idx;
        }
        auto out = buffer::alloc(sizeof(ulong));
        out->put(idx);
        out->pos(0);
        return out;
    }
    void commit_config(const ulong idx, ptr<cluster_config>&) override {
        std::lock_guard l(m);
        last = idx;
    }
    ulong last_commit_index() override {
        std::lock_guard l(m);
        return last;
    }
    ptr<snapshot> last_snapshot() override { return nullptr; }
    void create_snapshot(snapshot&, async_result<bool>::handler_type& done) override {
        ptr<std::exception> none;
        bool ok = false;
        done(ok, none);
    }
    bool apply_snapshot(snapshot&) override { return false; }
    std::vector<uint64_t> copy() {
        std::lock_guard l(m);
        return values;
    }
private:
    std::mutex m;
    std::vector<uint64_t> values;
    ulong last{0};
};

struct node {
    raft_group::options opt;
    ptr<list_machine> sm;
    std::unique_ptr<raft_group> g;

    bool start() {
        sm = cs_new<list_machine>();
        g = std::make_unique<raft_group>(opt, sm);
        std::string err;
        if (!g->start(err)) {
            std::printf("    %s\n", err.c_str());
            return false;
        }
        return true;
    }
    void stop() {
        if (g) g->stop();
        g.reset();
    }
};

static ptr<buffer> value(uint64_t v) {
    auto b = buffer::alloc(sizeof(uint64_t));
    buffer_serializer bs(b);
    bs.put_u64(v);
    return b;
}

static node* leader_of(std::vector<node>& nodes) {
    for (auto& n : nodes)
        if (n.g && n.g->is_leader()) return &n;
    return nullptr;
}

static bool all_hold(std::vector<node>& nodes, const std::vector<uint64_t>& want) {
    for (auto& n : nodes)
        if (n.g && n.sm->copy() != want) return false;
    return true;
}

static bool append_all(std::vector<node>& nodes, uint64_t from, uint64_t to, std::vector<uint64_t>& want) {
    for (uint64_t v = from; v <= to; ++v) {
        bool done = wait_for(5000, [&] {
            auto* l = leader_of(nodes);
            if (!l) return false;
            uint64_t idx = 0;
            std::string why;
            return l->g->append(value(v), idx, why) == raft_group::outcome::committed && idx > 0;
        });
        if (!done) return false;
        want.push_back(v);
    }
    return true;
}

int main() {
    namespace fs = std::filesystem;
    const char* p = std::getenv("BARCH_TEST_PORT");
    const int base = p && *p ? std::atoi(p) : 26200;
    const fs::path dir = fs::current_path() / "raftgrouptest.d";
    fs::remove_all(dir);

    // three nodes, each with its own raft_port and directory; group 1 on each
    std::vector<node> nodes(3);
    for (int i = 0; i < 3; ++i) {
        nodes[i].opt.group = 1;
        nodes[i].opt.dir = (dir / ("n" + std::to_string(i + 1))).string();
        nodes[i].opt.base_port = base + 2 * i;
        nodes[i].opt.server_id = i + 1;
        nodes[i].opt.join = i != 0;
    }

    std::printf("a group of three, built one member at a time\n");
    check(nodes[0].start(), "node 1 starts the group");
    check(wait_for(5000, [&] { return nodes[0].g->is_leader(); }), "and leads it");
    check(nodes[1].start() && nodes[2].start(), "nodes 2 and 3 start, waiting to be added");
    check(!nodes[1].g->is_leader() && !nodes[2].g->is_leader(), "and don't lead a group of their own");
    std::string err;
    for (int i = 1; i < 3; ++i) {
        const bool added = nodes[0].g->add_member(nodes[i].opt.server_id, nodes[i].g->my_endpoint(), 10000, err);
        check(added, "node " + std::to_string(i + 1) + " is added" + (added ? "" : ": " + err));
    }
    check(nodes[0].g->members().size() == 3, "the leader lists three members");
    check(wait_for(5000, [&] { return nodes[2].g->members().size() == 3; }), "and so does the last one added");

    std::vector<uint64_t> want;
    check(append_all(nodes, 1, 40, want), "40 entries commit");
    check(wait_for(5000, [&] { return all_hold(nodes, want); }), "and every member applied them in order");

    std::printf("the leader stops\n");
    node* old = leader_of(nodes);
    const int32_t old_id = old ? old->g->my_id() : -1;
    if (old) old->stop();
    check(wait_for(10000, [&] {
              auto* l = leader_of(nodes);
              return l && l->g->my_id() != old_id;
          }),
          "the other two elect a new leader");
    check(append_all(nodes, 41, 60, want), "20 more commit on two of three");
    for (auto& n : nodes)
        if (n.opt.server_id == old_id) check(n.start(), "the old leader comes back from its file");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }), "and catches up with all 60");

    std::printf("every member stops and starts again\n");
    for (auto& n : nodes) n.stop();
    bool started = true;
    for (auto& n : nodes) started = started && n.start();
    check(started, "all three start from their files");
    check(wait_for(10000, [&] { return leader_of(nodes) != nullptr; }), "elect a leader");
    check(leader_of(nodes) && leader_of(nodes)->g->members().size() == 3, "with the same three members");
    check(append_all(nodes, 61, 70, want), "take 10 more");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }),
          "and every member holds all 70, the first 60 replayed from the log");

    std::printf("a member is removed\n");
    node* l = leader_of(nodes);
    node* other = nullptr;
    for (auto& n : nodes)
        if (&n != l) { other = &n; break; }
    check(l && other && l->g->remove_member(other->g->my_id(), 10000, err), "the leader removes one");
    check(l && l->g->members().size() == 2, "and lists two");
    other->stop();
    check(append_all(nodes, 71, 75, want), "the two left still commit");

    for (auto& n : nodes) n.stop();
    fs::remove_all(dir);
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
