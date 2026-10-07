//
// One Raft group on this node - TODO 610.
//
#include "raft_group.h"

#include "lzr_log.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

namespace barch::cluster {
    namespace {
        // NuRaft's own log lines: its errors and warnings go to barch's log, the
        // rest only with BARCH_RAFT_LOG set
        class raft_logger : public nuraft::logger {
        public:
            explicit raft_logger(uint32_t g) : group(g), chatty(std::getenv("BARCH_RAFT_LOG") != nullptr) {}
            void put_details(int level, const char*, const char*, size_t, const std::string& msg) override {
                if (level <= 2)
                    barch::err({"raft group", (uint64_t) group, msg});
                else if (level == 3)
                    barch::warn({"raft group", (uint64_t) group, msg});
                else if (chatty && level <= 5)
                    barch::log({"raft group", (uint64_t) group, msg});
            }
            int get_level() override { return chatty ? 5 : 3; }
        private:
            uint32_t group;
            bool chatty;
        };

        bool wait_for(int ms, const std::function<bool()>& f) {
            const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
            while (std::chrono::steady_clock::now() < until) {
                if (f()) return true;
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            return f();
        }
    }

    /*
     * NuRaft's state manager, over the group's one file. A node that has never
     * had a configuration offers one with only itself in it: the group of one a
     * bootstrap starts, or the placeholder a joining node waits behind until the
     * leader sends it the real one.
     */
    class raft_group::state_mgr : public nuraft::state_mgr {
    public:
        state_mgr(std::shared_ptr<group_store> s, int32_t id, std::string ep)
            : store(std::move(s)), id(id), endpoint(std::move(ep)) {}

        nuraft::ptr<nuraft::cluster_config> load_config() override {
            if (auto c = store->load_config())
                return c;
            auto c = nuraft::cs_new<nuraft::cluster_config>();
            c->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(id, endpoint));
            return c;
        }
        void save_config(const nuraft::cluster_config& c) override { store->save_config(c); }
        void save_state(const nuraft::srv_state& s) override { store->save_state(s); }
        nuraft::ptr<nuraft::srv_state> read_state() override { return store->read_state(); }
        nuraft::ptr<nuraft::log_store> load_log_store() override { return store; }
        nuraft::int32 server_id() override { return id; }
        void system_exit(const int code) override {
            barch::err({"raft stopped this process with code", (int64_t) code, "- its log", store->path(),
                        "can't be trusted"});
            std::abort();
        }

    private:
        std::shared_ptr<group_store> store;
        int32_t id;
        std::string endpoint;
    };

    std::string raft_group::endpoint_for(const std::string& host, int base_port, uint32_t group) {
        return host + ":" + std::to_string(base_port + (int) group);
    }

    raft_group::raft_group(options o, nuraft::ptr<nuraft::state_machine> sm)
        : opt(std::move(o)), machine(std::move(sm)) {}

    raft_group::~raft_group() {
        stop();
    }

    nuraft::ptr<nuraft::raft_server> raft_group::srv() const {
        std::lock_guard l(ptrs);
        return server;
    }

    std::shared_ptr<group_store> raft_group::st() const {
        std::lock_guard l(ptrs);
        return store;
    }

    uint64_t raft_group::last_index() const {
        auto s = st();
        return s ? s->next_slot() - 1 : 0;
    }

    bool raft_group::start(std::string& err) {
        if (srv())
            return true;
        std::shared_ptr<group_store> store;
        try {
            std::filesystem::create_directories(opt.dir);
            const auto file = (std::filesystem::path(opt.dir) / ("group_" + std::to_string(opt.group) + ".raft")).string();
            store = std::make_shared<group_store>(file);
        } catch (const std::exception& e) {
            err = e.what();
            return false;
        }
        {
            std::lock_guard l(ptrs);
            this->store = store;
        }
        const bool fresh = !store->load_config();
        if (!store->self(self_id, self_endpoint)) {
            self_id = opt.server_id;
            self_endpoint = endpoint_for(opt.host, opt.base_port, opt.group);
            store->save_self(self_id, self_endpoint);
        }
        smgr = nuraft::cs_new<state_mgr>(store, self_id, self_endpoint);

        nuraft::raft_params params;
        params.heart_beat_interval_ = opt.heartbeat_ms;
        params.election_timeout_lower_bound_ = opt.election_min_ms;
        params.election_timeout_upper_bound_ = opt.election_max_ms;
        params.client_req_timeout_ = opt.client_timeout_ms;
        params.return_method_ = nuraft::raft_params::blocking;
        params.auto_forwarding_ = false;
        params.snapshot_distance_ = opt.snapshot_distance;
        params.reserved_log_items_ = opt.reserved_entries;
        params.snapshot_sync_ctx_timeout_ = opt.snapshot_timeout_ms;

        nuraft::asio_service::options asio_opt;
        asio_opt.thread_pool_size_ = 2;

        nuraft::raft_server::init_options init;
        // a node waiting to be added must not elect itself leader of a group of one
        init.skip_initial_election_timeout_ = fresh && opt.join;
        init.raft_callback_ = [this](nuraft::cb_func::Type t, nuraft::cb_func::Param* p) {
            if (events) events(t, p);
            return nuraft::cb_func::ReturnCode::Ok;
        };

        const int port = opt.base_port + (int) opt.group;
        auto server = launcher.init(machine, smgr, nuraft::cs_new<raft_logger>(opt.group), port, asio_opt, params, init);
        {
            std::lock_guard l(ptrs);
            this->server = server;
        }
        if (!server) {
            err = "could not start raft group " + std::to_string(opt.group) + " on port " + std::to_string(port);
            return false;
        }
        // only a group of one can be ready on its own. A node waiting to be added,
        // or one of several coming back, is initialised once a leader reaches it,
        // and that needs a quorum of them up - which may be after this returns
        const auto config = smgr->load_config();
        const bool alone = !(fresh && opt.join) && config && config->get_servers().size() <= 1;
        if (alone && !wait_for(5000, [&] { return server->is_initialized(); })) {
            err = "raft group " + std::to_string(opt.group) + " did not initialise";
            stop();
            return false;
        }
        barch::log({"raft group", (uint64_t) opt.group, "started as server", (int64_t) self_id, "on",
                    self_endpoint, fresh ? (opt.join ? "waiting to be added" : "as a new group") : "from its log"});
        return true;
    }

    void raft_group::stop() {
        nuraft::ptr<nuraft::raft_server> s;
        {
            std::lock_guard l(ptrs);
            s = std::move(server);
            server.reset();
        }
        if (!s)
            return;
        launcher.shutdown(5);
        smgr.reset();
        std::lock_guard l(ptrs);
        store.reset();
    }

    bool raft_group::is_leader() const {
        auto s = srv();
        return s && s->is_leader();
    }

    int32_t raft_group::leader_id() const {
        auto s = srv();
        return s ? s->get_leader() : -1;
    }

    std::string raft_group::leader_endpoint() const {
        auto s = srv();
        if (!s) return {};
        const auto id = s->get_leader();
        if (id < 0) return {};
        auto c = s->get_srv_config(id);
        return c ? c->get_endpoint() : std::string{};
    }

    uint64_t raft_group::term() const {
        auto s = srv();
        return s ? s->get_term() : 0;
    }

    uint64_t raft_group::committed_index() const {
        auto s = srv();
        return s ? s->get_committed_log_idx() : 0;
    }

    std::vector<raft_group::member> raft_group::members() const {
        std::vector<member> out;
        auto s = srv();
        if (!s) return out;
        std::vector<nuraft::ptr<nuraft::srv_config>> all;
        s->get_srv_config_all(all);
        for (const auto& c : all)
            out.push_back({c->get_id(), c->get_endpoint(), c->is_learner()});
        return out;
    }

    raft_group::outcome raft_group::append(nuraft::ptr<nuraft::buffer> data, uint64_t& index, std::string& why) {
        index = 0;
        auto s = srv();
        if (!s) {
            why = "the group isn't running";
            return outcome::refused;
        }
        if (!s->is_leader()) {
            why = "not the leader";
            return outcome::refused;
        }
        auto r = s->append_entries({std::move(data)});
        if (!r->get_accepted()) {
            why = "refused: " + r->get_result_str();
            return outcome::refused;
        }
        if (r->get_result_code() != nuraft::cmd_result_code::OK) {
            why = r->get_result_str();
            return outcome::unknown;
        }
        // the state machine answers a commit with the index it was given
        if (auto b = r->get(); b && b->size() == sizeof(uint64_t)) {
            b->pos(0);
            index = b->get_ulong();
        }
        return outcome::committed;
    }

    bool raft_group::add_member(int32_t id, const std::string& endpoint, int wait_ms, std::string& err,
                                bool learner) {
        std::lock_guard l(change);
        auto s = srv();
        if (!s || !s->is_leader()) {
            err = "not the leader";
            return false;
        }
        auto r = s->add_srv(nuraft::srv_config(id, 0, endpoint, "", learner));
        if (!r->get_accepted()) {
            err = "the leader refused to add it: " + r->get_result_str();
            return false;
        }
        const bool in = wait_for(wait_ms, [&] { return s->get_srv_config(id) != nullptr; });
        if (!in)
            err = "it wasn't in the configuration after " + std::to_string(wait_ms) + "ms";
        return in;
    }

    bool raft_group::remove_member(int32_t id, int wait_ms, std::string& err) {
        std::lock_guard l(change);
        auto s = srv();
        if (!s || !s->is_leader()) {
            err = "not the leader";
            return false;
        }
        auto r = s->remove_srv(id);
        if (!r->get_accepted()) {
            err = "the leader refused to remove it: " + r->get_result_str();
            return false;
        }
        const bool out = wait_for(wait_ms, [&] { return s->get_srv_config(id) == nullptr; });
        if (!out)
            err = "it was still in the configuration after " + std::to_string(wait_ms) + "ms";
        return out;
    }

    void raft_group::hand_over(int32_t id) {
        if (auto s = srv(); s && s->is_leader())
            s->yield_leadership(false, id);
    }

    void raft_group::yield_leadership() {
        if (auto s = srv(); s && s->is_leader())
            s->yield_leadership();
    }

    bool raft_group::promote(int32_t id, int wait_ms, std::string& err) {
        std::lock_guard l(change);
        auto s = srv();
        if (!s || !s->is_leader()) {
            err = "not the leader";
            return false;
        }
        // NuRaft answers this one with a result code and never marks it accepted
        auto r = s->flip_learner_flag(id, false);
        if (!r || r->get_result_code() != nuraft::cmd_result_code::OK) {
            err = "the leader refused to promote it" + (r ? ": " + r->get_result_str() : std::string{});
            return false;
        }
        const bool voter = wait_for(wait_ms, [&] {
            auto c = s->get_srv_config(id);
            return c && !c->is_learner();
        });
        if (!voter)
            err = "it was still a learner after " + std::to_string(wait_ms) + "ms";
        return voter;
    }

    uint64_t raft_group::member_index(int32_t id) const {
        auto s = srv();
        if (!s || !s->is_leader()) return 0;
        return s->get_peer_info(id).last_log_idx_;
    }

    void raft_group::save_snapshot(nuraft::snapshot& snap) {
        if (auto s = st()) s->save_snapshot(snap);
    }

    nuraft::ptr<nuraft::snapshot> raft_group::last_snapshot() const {
        auto s = st();
        return s ? s->last_snapshot() : nullptr;
    }

    uint64_t raft_group::start_index() const {
        auto s = st();
        return s ? s->start_index() : 0;
    }
}
