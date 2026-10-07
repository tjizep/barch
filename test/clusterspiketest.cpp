// Phase 0 of docs/CLUSTERING.md - TODO 609. NuRaft and libgossip, built against
// barch's own asio, doing the few things the design leans on:
//
//   - three Raft servers in this process elect a leader and apply the same
//     entries in the same order
//   - servers that join after the leader has compacted its log catch up through a
//     logical snapshot, which is the call the CoW freeze and RETRIEVE will sit
//     behind
//   - with the leader gone the other two elect a new one and keep committing
//   - three gossip nodes that only know the first one find each other
//
// Ports are BARCH_TEST_PORT and the five after it. Nothing here touches barch's
// own code yet.
#include "nuraft.hxx"
#include "in_memory_state_mgr.hxx"

#include "core/gossip_core.hpp"
#include "net/json_serializer.hpp"
#include "net/transport_factory.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace nuraft;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    std::fflush(stdout);
    if (!ok) ++failures;
}

// wait up to `ms` for `f`, checking every 50ms
static bool wait_for(int ms, const std::function<bool()>& f) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < until) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return f();
}

static int base_port() {
    const char* p = std::getenv("BARCH_TEST_PORT");
    return p && *p ? std::atoi(p) : 26100;
}

// NuRaft's own log lines, only with CLUSTER_SPIKE_LOG set
struct spike_logger : logger {
    int id;
    explicit spike_logger(int i) : id(i) {}
    void put_details(int level, const char* file, const char* func, size_t line,
                     const std::string& msg) override {
        if (std::getenv("CLUSTER_SPIKE_LOG") && level <= 4)
            std::fprintf(stderr, "[raft %d] %s\n", id, msg.c_str());
        (void)file; (void)func; (void)line;
    }
    int get_level() override { return std::getenv("CLUSTER_SPIKE_LOG") ? 4 : 2; }
};

/*
 * The state is a list of numbers, one per entry. A snapshot is that list as it
 * was at its index, sent as a header object and then chunks of 16, the way a
 * shard's pages will go as a series of objects.
 */
class list_machine : public state_machine {
public:
    static constexpr size_t chunk = 16;

    ptr<buffer> commit(const ulong idx, buffer& data) override {
        buffer_serializer bs(data);
        const uint64_t v = bs.get_u64();
        std::lock_guard l(m);
        values.push_back(v);
        last_idx = idx;
        return nullptr;
    }
    void commit_config(const ulong idx, ptr<cluster_config>&) override {
        std::lock_guard l(m);
        last_idx = idx;
    }
    ulong last_commit_index() override {
        std::lock_guard l(m);
        return last_idx;
    }

    void create_snapshot(snapshot& s, async_result<bool>::handler_type& done) override {
        {
            std::lock_guard l(m);
            // NuRaft asks for the snapshot at the index it has just applied,
            // which is what makes the freeze line up with one log index
            ptr<buffer> b = s.serialize();
            snaps[s.get_last_log_idx()] = {snapshot::deserialize(*b), values};
            while (snaps.size() > 3) snaps.erase(snaps.begin());
            ++snapshots_made;
        }
        ptr<std::exception> none;
        bool ok = true;
        done(ok, none);
    }
    ptr<snapshot> last_snapshot() override {
        std::lock_guard l(m);
        return snaps.empty() ? nullptr : snaps.rbegin()->second.meta;
    }
    int read_logical_snp_obj(snapshot& s, void*&, ulong obj_id, ptr<buffer>& out,
                             bool& is_last) override {
        std::lock_guard l(m);
        auto it = snaps.find(s.get_last_log_idx());
        if (it == snaps.end()) {
            out = nullptr;
            is_last = true;
            return -1;
        }
        const auto& vals = it->second.values;
        const size_t chunks = (vals.size() + chunk - 1) / chunk;
        if (obj_id == 0) {
            out = buffer::alloc(sizeof(uint64_t));
            buffer_serializer bs(out);
            bs.put_u64(vals.size());
            is_last = chunks == 0;
            return 0;
        }
        const size_t from = (obj_id - 1) * chunk;
        const size_t to = std::min(vals.size(), from + chunk);
        out = buffer::alloc(sizeof(uint64_t) * (to - from));
        buffer_serializer bs(out);
        for (size_t i = from; i < to; ++i) bs.put_u64(vals[i]);
        is_last = obj_id >= chunks;
        ++objects_sent;
        return 0;
    }
    void save_logical_snp_obj(snapshot&, ulong& obj_id, buffer& data, bool, bool) override {
        std::lock_guard l(m);
        buffer_serializer bs(data);
        if (obj_id == 0) {
            receiving.clear();
            receiving.reserve(bs.get_u64());
        } else {
            while (bs.pos() < data.size()) receiving.push_back(bs.get_u64());
            ++objects_received;
        }
        ++obj_id;
    }
    bool apply_snapshot(snapshot& s) override {
        std::lock_guard l(m);
        values = receiving;
        last_idx = s.get_last_log_idx();
        ptr<buffer> b = s.serialize();
        snaps[s.get_last_log_idx()] = {snapshot::deserialize(*b), values};
        ++snapshots_applied;
        return true;
    }

    std::vector<uint64_t> copy() {
        std::lock_guard l(m);
        return values;
    }

    std::atomic<int> snapshots_made{0};
    std::atomic<int> snapshots_applied{0};
    std::atomic<int> objects_sent{0};
    std::atomic<int> objects_received{0};

private:
    struct held {
        ptr<snapshot> meta;
        std::vector<uint64_t> values;
    };
    std::mutex m;
    std::vector<uint64_t> values;
    std::vector<uint64_t> receiving;
    std::map<ulong, held> snaps;
    ulong last_idx{0};
};

struct raft_node {
    int id{0};
    int port{0};
    std::string endpoint;
    ptr<list_machine> sm;
    ptr<state_mgr> smgr;
    raft_launcher launcher;
    ptr<raft_server> server;
    bool up{false};

    bool start(const raft_params& params) {
        endpoint = "127.0.0.1:" + std::to_string(port);
        sm = cs_new<list_machine>();
        smgr = cs_new<inmem_state_mgr>(id, endpoint);
        asio_service::options asio_opt;
        asio_opt.thread_pool_size_ = 2;
        server = launcher.init(sm, smgr, cs_new<spike_logger>(id), port, asio_opt, params);
        up = server != nullptr && wait_for(5000, [&] { return server->is_initialized(); });
        return up;
    }
    void stop() {
        if (!up) return;
        launcher.shutdown(5);
        server.reset();
        up = false;
    }
};

static raft_params spike_params() {
    raft_params p;
    p.heart_beat_interval_ = 100;
    p.election_timeout_lower_bound_ = 300;
    p.election_timeout_upper_bound_ = 600;
    // compact early, so the servers that join later can only catch up from a snapshot
    p.snapshot_distance_ = 20;
    p.reserved_log_items_ = 5;
    p.log_sync_stop_gap_ = 5;
    p.client_req_timeout_ = 3000;
    p.return_method_ = raft_params::blocking;
    return p;
}

// append one value through whichever node leads; true once it's committed there
static bool append(std::vector<std::unique_ptr<raft_node>>& nodes, uint64_t v) {
    for (auto& n : nodes) {
        if (!n->up || !n->server->is_leader()) continue;
        ptr<buffer> b = buffer::alloc(sizeof(uint64_t));
        buffer_serializer bs(b);
        bs.put_u64(v);
        auto r = n->server->append_entries({b});
        return r->get_accepted() && r->get_result_code() == cmd_result_code::OK;
    }
    return false;
}

static int leader_of(std::vector<std::unique_ptr<raft_node>>& nodes) {
    for (auto& n : nodes)
        if (n->up && n->server->is_leader()) return n->id;
    return -1;
}

static bool all_hold(std::vector<std::unique_ptr<raft_node>>& nodes, const std::vector<uint64_t>& want) {
    for (auto& n : nodes)
        if (n->up && n->sm->copy() != want) return false;
    return true;
}

static void raft_part(int port) {
    std::printf("raft: one server, then two more that join from a snapshot\n");
    std::vector<std::unique_ptr<raft_node>> nodes;
    for (int i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<raft_node>());
        nodes.back()->id = i + 1;
        nodes.back()->port = port + i;
    }
    const auto params = spike_params();
    check(nodes[0]->start(params), "server 1 starts");
    check(wait_for(5000, [&] { return leader_of(nodes) == 1; }), "and leads on its own");

    std::vector<uint64_t> want;
    bool ok = true;
    for (uint64_t v = 1; v <= 50; ++v) {
        ok = ok && append(nodes, v * 7);
        want.push_back(v * 7);
    }
    check(ok, "50 entries commit on one server");
    check(nodes[0]->sm->snapshots_made > 0, "and it took a snapshot");

    for (int i = 1; i < 3; ++i) {
        check(nodes[i]->start(params), "server " + std::to_string(i + 1) + " starts");
        auto r = nodes[0]->server->add_srv(srv_config(nodes[i]->id, nodes[i]->endpoint));
        check(r->get_accepted(), "server " + std::to_string(i + 1) + " is added");
        // one membership change at a time: wait for this one before the next
        check(wait_for(10000, [&] {
                  std::vector<ptr<srv_config>> all;
                  nodes[0]->server->get_srv_config_all(all);
                  return all.size() == static_cast<size_t>(i + 1);
              }),
              "and is in the configuration");
    }
    check(wait_for(10000, [&] { return all_hold(nodes, want); }), "all three hold the first 50");
    check(nodes[1]->sm->snapshots_applied > 0 && nodes[2]->sm->snapshots_applied > 0,
          "the two that joined caught up through a snapshot");
    check(nodes[0]->sm->objects_sent > 1, "sent as more than one object");

    for (uint64_t v = 51; v <= 100; ++v) {
        ok = ok && append(nodes, v * 7);
        want.push_back(v * 7);
    }
    check(ok, "50 more commit with three members");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }), "and all three hold 100, in order");

    std::printf("raft: the leader goes\n");
    const int old = leader_of(nodes);
    nodes[old - 1]->stop();
    check(wait_for(10000, [&] {
              const int l = leader_of(nodes);
              return l > 0 && l != old;
          }),
          "the other two elect a new leader");
    // the new leader can need a moment before it takes writes
    for (uint64_t v = 101; v <= 120; ++v) {
        bool one = wait_for(5000, [&] { return append(nodes, v * 7); });
        ok = ok && one;
        want.push_back(v * 7);
    }
    check(ok, "20 more commit with two of three");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }), "and both hold all 120");

    for (auto& n : nodes) n->stop();
}

static void gossip_part(int port) {
    std::printf("gossip: three nodes that only know the first\n");
    struct gnode {
        std::shared_ptr<libgossip::gossip_core> core;
        std::unique_ptr<gossip::net::transport> transport;
        libgossip::node_view self;
    };
    std::vector<std::unique_ptr<gnode>> nodes;
    bool started = true;
    for (int i = 0; i < 3; ++i) {
        auto g = std::make_unique<gnode>();
        g->self.id = {};
        g->self.id[15] = static_cast<uint8_t>(i + 1);
        g->self.ip = "127.0.0.1";
        g->self.port = port + i;
        g->self.status = libgossip::node_status::online;
        g->transport = gossip::net::transport_factory::create_transport(
                gossip::net::transport_type::udp, "127.0.0.1", static_cast<uint16_t>(port + i));
        if (!g->transport) {
            started = false;
            break;
        }
        g->transport->set_serializer(std::make_unique<gossip::net::json_serializer>());
        auto* t = g->transport.get();
        g->core = std::make_shared<libgossip::gossip_core>(
                g->self,
                [t](const libgossip::gossip_message& msg, const libgossip::node_view& to) {
                    t->send_message(msg, to);
                },
                [](const libgossip::node_view&, libgossip::node_status) {});
        g->transport->set_gossip_core(g->core);
        started = started && g->transport->start() == gossip::net::error_code::success;
        nodes.push_back(std::move(g));
    }
    check(started, "three udp transports start");
    if (!started) return;

    nodes[1]->core->meet(nodes[0]->self);
    nodes[2]->core->meet(nodes[0]->self);

    std::atomic<bool> stop{false};
    std::thread ticker([&] {
        while (!stop) {
            for (auto& g : nodes) g->core->tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
    auto knows_all = [&] {
        for (auto& g : nodes) {
            size_t online = 0;
            for (const auto& n : g->core->get_nodes())
                if (n.status == libgossip::node_status::online) ++online;
            if (online < 2) return false;
        }
        return true;
    };
    check(wait_for(10000, knows_all), "each sees the other two online");
    stop = true;
    ticker.join();
    for (auto& g : nodes) g->transport->stop();
}

int main() {
    const int port = base_port();
    std::printf("ports %d to %d\n", port, port + 5);
    raft_part(port);
    gossip_part(port + 3);
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
