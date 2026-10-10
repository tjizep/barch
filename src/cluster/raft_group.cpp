//
// One Raft group on this node - TODO 610.
//
#include "raft_group.h"
#include "raft_auth.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "lzr_log.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <set>
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
        state_mgr(std::shared_ptr<group_store> s, int32_t id, std::string ep,
                  std::vector<std::pair<int32_t, std::string>> initial = {})
            : store(std::move(s)), id(id), endpoint(std::move(ep)), initial(std::move(initial)) {}

        nuraft::ptr<nuraft::cluster_config> load_config() override {
            if (auto c = store->load_config())
                return c;
            auto c = nuraft::cs_new<nuraft::cluster_config>();
            if (initial.empty()) {
                c->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(id, endpoint));
            } else {
                // a split's new group: every member starts with the same voters
                for (const auto& [mid, mep] : initial)
                    c->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(mid, mep));
            }
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
        std::vector<std::pair<int32_t, std::string>> initial;
    };

    /*
     * The TLS contexts NuRaft would make from raft_tls's files, made here - TODO 629 -
     * so their certificates can be warmed before any handshake uses them. OpenSSL 3
     * fills in a certificate's cached fields the first time it's checked, under a
     * lock of its own, and another thread checking it at the same moment reads them
     * after a flag TSan can't see. Two handshakes verifying against the same CA did
     * just that: harmless by OpenSSL's design, but a report in every few TSan runs.
     * Filling the cache here, before the context is shared, leaves nothing to fill.
     */
    static void warm(X509* x) {
        if (x) X509_check_purpose(x, -1, 0);
    }

    static std::string ssl_error(const std::string& what) {
        char buf[256] = {0};
        ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
        return what + ": " + buf;
    }

    SSL_CTX* tls_server_context(const std::string& cert, const std::string& key) {
        SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
        if (!ctx) throw std::runtime_error(ssl_error("could not make a TLS context"));
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1) {
            SSL_CTX_free(ctx);
            throw std::runtime_error(ssl_error("could not load " + cert));
        }
        if (SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1) {
            SSL_CTX_free(ctx);
            throw std::runtime_error(ssl_error("could not load " + key));
        }
        warm(SSL_CTX_get0_certificate(ctx));
        STACK_OF(X509)* chain = nullptr;
        if (SSL_CTX_get0_chain_certs(ctx, &chain) == 1 && chain)
            for (int i = 0; i < sk_X509_num(chain); ++i) warm(sk_X509_value(chain, i));
        return ctx;
    }

    SSL_CTX* tls_client_context(const std::string& ca) {
        SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
        if (!ctx) throw std::runtime_error(ssl_error("could not make a TLS context"));
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        if (SSL_CTX_load_verify_locations(ctx, ca.c_str(), nullptr) != 1) {
            SSL_CTX_free(ctx);
            throw std::runtime_error(ssl_error("could not load " + ca));
        }
        // every certificate a peer's chain is checked against
        if (auto* objs = X509_STORE_get0_objects(SSL_CTX_get_cert_store(ctx)))
            for (int i = 0; i < sk_X509_OBJECT_num(objs); ++i)
                warm(X509_OBJECT_get0_X509(sk_X509_OBJECT_value(objs, i)));
        return ctx;
    }

    /*
     * Every request and response carries its signature in NuRaft's metadata, and
     * one without the right signature closes the connection - TODO 620. See
     * raft_auth.h for what's signed.
     */
    static void sign_messages(nuraft::asio_service::options& o, const std::string& secret, uint32_t group,
                       const std::shared_ptr<std::atomic<uint64_t>>& refused) {
        auto refuse = [group, refused](const char* what) {
            // the first, then every thousandth: someone hammering the port
            // shouldn't fill the log
            const auto n = ++*refused;
            if (n == 1 || n % 1000 == 0)
                barch::err({"raft group", (uint64_t) group, "refused", what,
                            "without the cluster's signature, so far", n});
            return false;
        };
        o.write_req_meta_ = [secret](const nuraft::asio_service_meta_cb_params& p) {
            try {
                return p.req_ ? sign_request(secret, *p.req_) : std::string{};
            } catch (const std::exception&) {
                return std::string{};
            }
        };
        o.read_req_meta_ = [secret, refuse](const nuraft::asio_service_meta_cb_params& p, const std::string& got) {
            try {
                if (p.req_ && same_signature(got, sign_request(secret, *p.req_)))
                    return true;
            } catch (const std::exception&) {}
            return refuse("a request");
        };
        o.write_resp_meta_ = [secret](const nuraft::asio_service_meta_cb_params& p) {
            try {
                return p.req_ && p.resp_ ? sign_response(secret, *p.req_, *p.resp_) : std::string{};
            } catch (const std::exception&) {
                return std::string{};
            }
        };
        o.read_resp_meta_ = [secret, refuse](const nuraft::asio_service_meta_cb_params& p, const std::string& got) {
            try {
                if (p.req_ && p.resp_ && same_signature(got, sign_response(secret, *p.req_, *p.resp_)))
                    return true;
            } catch (const std::exception&) {}
            return refuse("a response");
        };
        // an empty signature is a wrong one
        o.invoke_req_cb_on_empty_meta_ = true;
        o.invoke_resp_cb_on_empty_meta_ = true;
    }

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
            // test knob, TODO 641: a slow disk under this group's log (every group's,
            // without BARCH_TEST_LOG_SYNC_DELAY_GROUP)
            if (const char* v = std::getenv("BARCH_TEST_LOG_SYNC_DELAY_MS")) {
                const char* g = std::getenv("BARCH_TEST_LOG_SYNC_DELAY_GROUP");
                if (!g || std::atol(g) == (long) opt.group)
                    store->slow_syncs(std::atol(v));
            }
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
        smgr = nuraft::cs_new<state_mgr>(store, self_id, self_endpoint,
                                         fresh ? opt.initial_members : std::vector<std::pair<int32_t, std::string>>{});

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
        params.leadership_expiry_ = opt.step_down_ms > 0 ? opt.step_down_ms : 2 * opt.election_max_ms;
        /*
         * The leader's log is synced in the background, alongside replication -
         * TODO 625. Without it every client write was its own append and its own
         * sync, one after another, so writes topped out at one per fdatasync
         * however many clients there were. A leader still only commits on a
         * quorum of durable copies: its own once its sync is done, its followers'
         * before that. Followers still answer only once their copy is synced.
         */
        params.parallel_log_appending_ = true;
        store->sync_in_background([n = notice] {
            std::shared_ptr<nuraft::raft_server> s;
            {
                std::lock_guard l(n->m);
                s = n->server.lock();
            }
            if (s) s->notify_log_append_completion(true);
        });

        nuraft::asio_service::options asio_opt;
        asio_opt.thread_pool_size_ = 2;
        if (opt.secret.empty()) {
            err = "raft group " + std::to_string(opt.group) + " needs cluster_secret set";
            return false;
        }
        sign_messages(asio_opt, opt.secret, opt.group, refused);
        if (opt.tls) {
            asio_opt.enable_ssl_ = true;
            const std::string ca = opt.tls_ca.empty() ? opt.tls_cert : opt.tls_ca;
            asio_opt.ssl_context_provider_server_ = [cert = opt.tls_cert, key = opt.tls_key] {
                return tls_server_context(cert, key);
            };
            asio_opt.ssl_context_provider_client_ = [ca] { return tls_client_context(ca); };
        }

        nuraft::raft_server::init_options init;
        // a node waiting to be added must not elect itself leader of a group of one
        init.skip_initial_election_timeout_ = fresh && opt.join;
        init.raft_callback_ = [this](nuraft::cb_func::Type t, nuraft::cb_func::Param* p) {
            if (events) events(t, p);
            return nuraft::cb_func::ReturnCode::Ok;
        };

        const int port = opt.base_port + (int) opt.group;
        nuraft::ptr<nuraft::raft_server> server;
        try {
            server = launcher.init(machine, smgr, nuraft::cs_new<raft_logger>(opt.group), port, asio_opt, params, init);
        } catch (const std::exception& e) {
            // a certificate or key that won't load, mostly
            err = "could not start raft group " + std::to_string(opt.group) + ": " + e.what();
            return false;
        }
        {
            std::lock_guard l(ptrs);
            this->server = server;
        }
        if (server) {
            {
                std::lock_guard l(notice->m);
                notice->server = server;
            }
            // anything the store synced before the server could be told
            server->notify_log_append_completion(true);
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
        {
            std::lock_guard l(notice->m);
            notice->server.reset();
        }
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

    bool raft_group::hand_over(int32_t id) {
        auto s = srv();
        if (!s || !s->is_leader() || !answering(id))
            return false;
        s->yield_leadership(false, id);
        return true;
    }

    bool raft_group::answering(int32_t id) const {
        auto s = srv();
        if (!s || !s->is_leader())
            return false;
        for (const auto& p : s->get_peer_info_all())
            if (p.id_ == id)
                return p.last_succ_resp_us_ < (uint64_t) opt.lease_ms * 1000;
        return false;
    }

    void raft_group::hold_log() {
        auto st_ = st();
        if (!st_) return;
        auto s = srv();
        uint64_t from = 0;
        if (s && s->is_leader() && opt.snapshot_distance > 0) {
            for (const auto& p : s->get_peer_info_all()) {
                if (p.id_ == self_id || p.last_succ_resp_us_ >= 1000000) continue;
                // its last entry too: NuRaft sends the next with that entry's term
                const uint64_t need = std::max<uint64_t>(1, p.last_log_idx_);
                if (from == 0 || need < from) from = need;
            }
        }
        st_->hold_log(from, 10 * (uint64_t) opt.snapshot_distance);
    }

    bool raft_group::another_voter_answering() const {
        auto s = srv();
        if (!s || !s->is_leader())
            return false;
        std::vector<nuraft::ptr<nuraft::srv_config>> all;
        s->get_srv_config_all(all);
        std::set<int32_t> voters;
        for (const auto& c : all)
            if (!c->is_learner() && c->get_id() != s->get_id()) voters.insert(c->get_id());
        for (const auto& p : s->get_peer_info_all())
            if (voters.count(p.id_) && p.last_succ_resp_us_ < (uint64_t) opt.lease_ms * 1000)
                return true;
        return false;
    }

    bool raft_group::lease_valid() const {
        auto s = srv();
        if (!s || !s->is_leader())
            return false;
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard l(lease_lock);
        if (now - lease_checked < std::chrono::milliseconds(50))
            return lease_ok;
        std::vector<nuraft::ptr<nuraft::srv_config>> all;
        s->get_srv_config_all(all);
        std::set<int32_t> voters;
        for (const auto& c : all)
            if (!c->is_learner()) voters.insert(c->get_id());
        size_t fresh = voters.count(s->get_id()) ? 1 : 0;
        for (const auto& p : s->get_peer_info_all())
            if (voters.count(p.id_) && p.last_succ_resp_us_ < (uint64_t) opt.lease_ms * 1000)
                ++fresh;
        lease_ok = s->is_leader() && fresh * 2 > voters.size();
        // when it was worked out, after the work: a check that took long is no fresher
        lease_checked = std::chrono::steady_clock::now();
        return lease_ok;
    }

    bool raft_group::leader_alive() const {
        auto s = srv();
        return s && s->is_leader_alive();
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

    void raft_group::save_range(uint64_t from, uint64_t to) {
        if (auto s = st()) s->save_range(from, to);
    }

    bool raft_group::range(uint64_t& from, uint64_t& to) const {
        auto s = st();
        return s && s->range(from, to);
    }

    std::vector<int32_t> raft_group::voters() const {
        std::vector<int32_t> out;
        for (const auto& m : members())
            if (!m.learner) out.push_back(m.id);
        return out;
    }

    uint64_t raft_group::start_index() const {
        auto s = st();
        return s ? s->start_index() : 0;
    }
}
