//
// The cluster - TODO 610, docs/CLUSTERING.md.
//
// Raft groups, the key spaces bound to them, the `cluster` space that records the
// members, and CLUSTER. Only built with BARCH_CLUSTER; installs itself into
// cluster_hooks when it's linked in.
//
#include "cluster_hooks.h"
#include "raft_group.h"

#include "aof_record.h"
#include "configuration.h"
#include "constants.h"
#include "dictionary_compressor.h"
#include "meta_keys.h"
#include "sharded_store.h"
#include "data_dir.h"
#include "key_space.h"
#include "lzr_log.h"
#include "repl_api.h"
#include "resp_client.h"
#include "rpc/server.h"
#include "rpc_caller.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <thread>

namespace barch::cluster {
    namespace {
        constexpr const char* cluster_space = "cluster";
        constexpr const char* config_space = "configuration";
        constexpr uint8_t entry_version = 1;

        uint64_t now_ms() {
            return (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }

        uint64_t random_u64() {
            std::random_device rd;
            return ((uint64_t) rd() << 32) ^ rd()
                   ^ (uint64_t) std::chrono::steady_clock::now().time_since_epoch().count();
        }

        /*
         * Whether another node's shard pages can be loaded here - TODO 612. Byte
         * order, the sizes of the types pages hold, and the storage version. Two
         * nodes with different tags copy spaces key by key instead of page by page.
         * BARCH_TEST_LAYOUT replaces it, so a test can force the key by key path
         * between two identical builds.
         */
        std::string layout_tag() {
            if (const char* forced = std::getenv("BARCH_TEST_LAYOUT"); forced && *forced)
                return forced;
            const uint16_t one = 1;
            const bool little = *(const uint8_t*) &one == 1;
            return std::string(little ? "le" : "be") + "-l" + std::to_string(sizeof(long))
                   + "-p" + std::to_string(sizeof(void*)) + "-s" + std::to_string((uint64_t) storage_version);
        }

        // records go hex encoded: a reply string that starts with '$' loses that
        // byte in Variable::to_string, which takes it for a bulk marker
        std::string to_hex(const std::string& b) {
            static const char* d = "0123456789abcdef";
            std::string out;
            out.reserve(b.size() * 2);
            for (unsigned char c : b) {
                out.push_back(d[c >> 4]);
                out.push_back(d[c & 15]);
            }
            return out;
        }

        bool from_hex(const std::string& h, std::string& out) {
            if (h.size() % 2) return false;
            out.clear();
            out.reserve(h.size() / 2);
            auto nib = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                return -1;
            };
            for (size_t i = 0; i < h.size(); i += 2) {
                const int a = nib(h[i]), b = nib(h[i + 1]);
                if (a < 0 || b < 0) return false;
                out.push_back((char) ((a << 4) | b));
            }
            return true;
        }

        bool split_address(const std::string& a, std::string& host, int& port) {
            const auto c = a.rfind(':');
            if (c == std::string::npos || c == 0) return false;
            host = a.substr(0, c);
            try {
                port = std::stoi(a.substr(c + 1));
            } catch (...) {
                return false;
            }
            return port > 0 && port < 65536;
        }

        // ---- calls into this process, as the server -------------------------------

        /*
         * A command run here through the same dispatch a client's goes through,
         * on `space`. Reads of a Raft space this node doesn't lead are allowed:
         * the cluster's bookkeeping reads its own copy.
         */
        bool local(const std::string& space, const std::vector<std::string>& params,
                   std::vector<std::string>* out, std::string& err) {
            try {
                rpc_caller sc;
                sc.raft_local_reads = true;
                sc.set_kspace(barch::get_keyspace(space));
                auto fi = barch::barch_functions->find(params.at(0));
                if (fi == barch::barch_functions->end()) {
                    err = "no command " + params[0];
                    return false;
                }
                if (sc.call(params, fi->second.call) != 0) {
                    err = sc.errors.empty() ? std::string("call failed") : sc.errors[0];
                    return false;
                }
                if (out) {
                    std::vector<Variable> res;
                    sc.append_flat(res);
                    for (const auto& v : res)
                        out->push_back(v.to_string());
                }
                return true;
            } catch (const std::exception& e) {
                err = e.what();
                return false;
            }
        }

        // ---- calls to another node, over RESP ------------------------------------

        void flatten(const Variable& v, std::vector<std::string>& out) {
            switch (v.index()) {
                case var_null:
                    out.emplace_back();
                    return;
                case var_string:
                case var_int64:
                case var_uint64:
                case var_bool:
                case var_double:
                    out.push_back(v.to_string());
                    return;
                default:
                    for (const auto& e : v.elements())
                        flatten(Variable{e.var}, out);
            }
        }

        bool remote(const std::string& address, const std::vector<std::string>& cmd,
                    std::vector<std::string>* out, std::string& err, uint32_t timeout_ms = 10000) {
            resp_client::endpoint ep;
            int port = 0;
            if (!split_address(address, ep.host, port)) {
                err = "not a host:port - " + address;
                return false;
            }
            ep.port = (uint16_t) port;
            resp_client::settings s;
            s.connect_timeout_ms = 3000;
            s.timeout_ms = timeout_ms;
            auto done = std::make_shared<std::promise<std::pair<std::vector<Variable>, std::string>>>();
            auto f = done->get_future();
            resp_client::run(ep, s, {cmd}, [done](std::vector<Variable> replies, std::string e) {
                done->set_value({std::move(replies), std::move(e)});
            });
            auto [replies, e] = f.get();
            if (!e.empty()) {
                err = address + ": " + e;
                return false;
            }
            if (replies.empty()) {
                err = address + ": no reply";
                return false;
            }
            if (replies[0].isError()) {
                err = replies[0].to_string();
                return false;
            }
            if (out) flatten(replies[0], *out);
            return true;
        }

        // ---- what the cluster space says -----------------------------------------

        struct node_info {
            std::string id;
            int32_t num{0};
            std::string rpc;        // host:port, RESP and the replication protocol
            std::string raft;       // host:raft_port
            std::string role;
        };

        struct space_info {
            std::string name;
            uint32_t group{0};
            int32_t creator{0};
            std::string mode;
        };

        std::map<std::string, std::string> pairs(const std::vector<std::string>& flat) {
            std::map<std::string, std::string> m;
            for (size_t i = 0; i + 1 < flat.size(); i += 2)
                m[flat[i]] = flat[i + 1];
            return m;
        }

        std::vector<std::string> keys_like(const std::string& pattern) {
            std::vector<std::string> out;
            std::string err;
            local(cluster_space, {"KEYS", pattern}, &out, err);
            return out;
        }

        std::vector<node_info> read_nodes() {
            std::vector<node_info> nodes;
            for (const auto& k : keys_like("node:*")) {
                // node:<id>, not node:<id>:seen or node:<id>:space:<name>
                if (k.find(':', 5) != std::string::npos) continue;
                std::vector<std::string> flat;
                std::string err;
                if (!local(cluster_space, {"HGETALL", k}, &flat, err)) continue;
                auto m = pairs(flat);
                node_info n;
                n.id = k.substr(5);
                try {
                    n.num = std::stoi(m["num"]);
                } catch (...) {
                    continue;
                }
                n.rpc = m["rpc"];
                n.raft = m["raft"];
                n.role = m["role"];
                nodes.push_back(n);
            }
            return nodes;
        }

        std::vector<space_info> read_spaces() {
            std::vector<space_info> spaces;
            for (const auto& k : keys_like("space:*")) {
                std::vector<std::string> flat;
                std::string err;
                if (!local(cluster_space, {"HGETALL", k}, &flat, err)) continue;
                auto m = pairs(flat);
                space_info s;
                s.name = k.substr(6);
                try {
                    s.group = (uint32_t) std::stoul(m["group"]);
                    s.creator = std::stoi(m["creator"]);
                } catch (...) {
                    continue;
                }
                s.mode = m["mode"];
                spaces.push_back(s);
            }
            return spaces;
        }

        // ---- a group and the spaces bound to it ----------------------------------

        class group_rt;

        /*
         * What a group's members apply. An entry is
         *
         *     u8 version, u64 incarnation, then one change log record
         *
         * The incarnation is the writing process's, new each time a group starts
         * here. The leader made the write itself before logging it (see
         * cluster_hooks.h), so an entry with this group's own incarnation is
         * already in place and isn't applied again. Everything else is applied
         * the way REPLAPPLY applies a record. A restart is a new incarnation, so
         * the whole log is applied again over what the files held: records are
         * absolute sets, erases and clears, so that ends where the log does.
         */
        class machine : public nuraft::state_machine {
        public:
            explicit machine(group_rt& g) : g(g) {}
            nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong idx, nuraft::buffer& data) override;
            void commit_config(const nuraft::ulong idx, nuraft::ptr<nuraft::cluster_config>&) override;
            nuraft::ulong last_commit_index() override;
            /*
             * Snapshots - TODO 611. A snapshot at index i is the group's spaces as
             * this node's data files hold them: SAVE on each, then i in the group
             * file. Writes hold their shard latch until they commit, so the files
             * only hold committed writes, and they hold at least everything up to
             * i - perhaps more. Applying the log from i + 1 over them still ends
             * where the log does, because records are absolute.
             *
             * Sending one is a single object naming the sender. The member it's
             * for copies the spaces from there with RETRIEVE, which takes them
             * under the sender's CoW freeze. That copy is at least at i too.
             */
            nuraft::ptr<nuraft::snapshot> last_snapshot() override;
            void create_snapshot(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done) override;
            int read_logical_snp_obj(nuraft::snapshot& s, void*& ctx, nuraft::ulong obj_id,
                                     nuraft::ptr<nuraft::buffer>& out, bool& is_last) override;
            void save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id, nuraft::buffer& data,
                                      bool is_first, bool is_last) override;
            bool apply_snapshot(nuraft::snapshot& s) override;
        private:
            group_rt& g;
        };

        class runtime_impl;

        class group_rt {
        public:
            group_rt(runtime_impl& rt, uint32_t number, std::vector<std::string> spaces)
                : rt(rt), number(number), spaces(std::move(spaces)) {}

            runtime_impl& rt;
            const uint32_t number;
            const std::vector<std::string> spaces;
            nuraft::ptr<machine> sm;
            std::atomic<uint64_t> incarnation{0};
            std::atomic<uint64_t> applied{0};
            std::atomic<uint64_t> leader_from{UINT64_MAX};
            std::atomic<uint64_t> apply_failures{0};
            std::atomic<bool> needs_rebuild{false};
            // entries applied here since the group started, its own skipped - TODO 611
            std::atomic<uint64_t> applied_here{0};
            std::atomic<uint64_t> snapshots_made{0};
            std::atomic<uint64_t> snapshots_installed{0};
            uint64_t last_handover{0};                      // the cluster's thread only
            // the group's raft_group from before start() publishes it, for the
            // state machine NuRaft calls while it starts
            std::atomic<raft_group*> live{nullptr};
            // what a snapshot being received copied, and whether that worked
            std::atomic<bool> received_ok{false};

            bool start(int32_t server_id, bool join, std::string& err);
            /** SAVE every space, then record the snapshot; `done` when finished */
            void snapshot_async(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done);
            void stop();
            /**
             * The group's Raft server, as a copy. A rebuild stops and replaces it on
             * the cluster's thread while clients' threads ask whether it leads.
             */
            [[nodiscard]] std::shared_ptr<raft_group> rg() const {
                std::lock_guard l(raft_lock);
                return raft;
            }
            [[nodiscard]] bool running() const {
                auto r = rg();
                return r && r->running();
            }
            /** leads, has applied everything from before it led, and isn't waiting on a rebuild */
            [[nodiscard]] bool ready() const {
                auto r = rg();
                return r && r->is_leader() && applied.load() >= leader_from.load() && !needs_rebuild;
            }
        private:
            mutable std::mutex raft_lock;
            std::shared_ptr<raft_group> raft;               // under raft_lock
            std::mutex snap_lock;
            std::thread snap_thread;                        // under snap_lock
        public:
            binding::result commit(const std::string& record, std::string& why);
            [[nodiscard]] std::string not_leader() const;
        };

        nuraft::ptr<nuraft::buffer> machine::commit(const nuraft::ulong idx, nuraft::buffer& data) {
            const auto* p = data.data_begin();
            const size_t n = data.size();
            if (n >= 9 && p[0] == entry_version) {
                uint64_t inc = 0;
                for (int i = 7; i >= 0; --i) inc = (inc << 8) | p[1 + i];
                if (inc != g.incarnation.load()) {
                    aof::record r;
                    const auto d = aof::decode(p + 9, (uint32_t) (n - 9), r);
                    std::string why;
                    bool ok = false;
                    if (d != aof::decoded::ok) {
                        why = std::string(aof::describe(d));
                    } else {
                        try {
                            repl::applying applying;
                            ok = repl::apply_one(r, why);
                        } catch (const std::exception& e) {
                            why = e.what();
                        }
                        ++g.applied_here;
                    }
                    if (!ok) {
                        // a member that can't apply an entry no longer holds what the
                        // others do, so it has to be rebuilt from them
                        ++g.apply_failures;
                        g.needs_rebuild = true;
                        barch::err({"raft group", (uint64_t) g.number, "could not apply entry",
                                    (uint64_t) idx, "-", why});
                    }
                }
            } else {
                barch::err({"raft group", (uint64_t) g.number, "entry", (uint64_t) idx,
                            "isn't one this build reads"});
                ++g.apply_failures;
                g.needs_rebuild = true;
            }
            g.applied.store(idx);
            auto out = nuraft::buffer::alloc(sizeof(nuraft::ulong));
            out->put(idx);
            out->pos(0);
            return out;
        }

        void machine::commit_config(const nuraft::ulong idx, nuraft::ptr<nuraft::cluster_config>&) {
            g.applied.store(idx);
        }

        nuraft::ulong machine::last_commit_index() {
            /*
             * NuRaft asks as it starts, and applies the log from the next index on.
             * The files hold at least the last snapshot, so a restart starts there
             * rather than at 1, whose entries may be compacted away - TODO 611.
             */
            if (g.applied.load() == 0) {
                if (auto* r = g.live.load()) {
                    if (auto snap = r->last_snapshot())
                        g.applied = snap->get_last_log_idx();
                }
            }
            return g.applied.load();
        }

        /*
         * A key space's view of its group. Its reads and writes are refused
         * while this node doesn't lead the group, and while a new leader is still
         * applying what came before it.
         */
        class space_binding : public binding {
        public:
            space_binding(std::shared_ptr<group_rt> g) : g(std::move(g)) {}
            result commit(const std::string& record, std::string& why) override {
                return g->commit(record, why);
            }
            [[nodiscard]] bool leader() const override { return g->ready(); }
            [[nodiscard]] std::string not_leader() const override { return g->not_leader(); }
        private:
            std::shared_ptr<group_rt> g;
        };

        // ---- the runtime ----------------------------------------------------------

        class runtime_impl : public runtime {
        public:
            bool start(std::string& err) override;
            void stop() override;
            bool command(const std::vector<std::string>& args, std::vector<std::string>& out,
                         std::string& err) override;

            /**
             * Each space from that node: with RETRIEVE, under its CoW freeze, when the
             * two lay pages out the same way, and key by key when they don't
             */
            bool copy_from(const std::string& rpc_address, const std::vector<std::string>& spaces, std::string& err);
            bool logical_copy(const std::string& from, const std::string& space, std::string& err);
            bool export_page(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err);
            std::atomic<uint64_t> logical_copies{0};
            [[nodiscard]] std::string my_rpc() const { return host + ":" + std::to_string(server_port); }
            /** the RESP address of a group member, from the cluster space */
            std::string address_of(int32_t num) {
                std::lock_guard l(nodes_lock);
                auto i = addresses.find(num);
                return i == addresses.end() ? std::string{} : i->second;
            }

        private:
            bool init(std::string& err);
            bool join(const std::string& address, std::string& err);
            bool reserve(std::vector<std::string>& out, std::string& err);
            bool admit(const std::vector<std::string>& args, std::string& err);
            bool remove(const std::string& id, std::string& err);
            bool heartbeat_in(const std::vector<std::string>& args, std::string& err);
            void info(std::vector<std::string>& out);

            std::shared_ptr<group_rt> open_group(uint32_t number, const std::vector<std::string>& spaces);
            bool bind_group(const std::shared_ptr<group_rt>& g, std::string& err);
            bool start_group(const std::shared_ptr<group_rt>& g, int32_t server_id, bool join, std::string& err);
            void detach(const std::vector<std::string>& spaces);
            void tick();
            void refresh_nodes();
            void claim_spaces();
            void open_spaces();
            void reconcile(const std::shared_ptr<group_rt>& g);
            void send_heartbeat();
            void rebuild(const std::shared_ptr<group_rt>& g);
            std::string group_file(uint32_t n) const {
                return (std::filesystem::path(dir) / ("group_" + std::to_string(n) + ".raft")).string();
            }
            [[nodiscard]] std::string my_raft() const { return host + ":" + std::to_string(raft_port); }
            int32_t my_num();

            std::string dir;
            std::string host;
            int raft_port{0};
            int server_port{0};
            uint64_t heartbeat_ms{5000};
            std::string node_id;
            std::string layout;

            std::mutex groups_lock;
            std::map<uint32_t, std::shared_ptr<group_rt>> groups;

            std::mutex nodes_lock;
            std::map<int32_t, std::string> addresses;       // num -> rpc
            std::vector<node_info> nodes;
            int32_t reserved{0};

            std::mutex admin;                               // one INIT, JOIN, ADMIT or REMOVE at a time
            std::thread ticker;
            std::mutex tick_lock;
            std::condition_variable tick_cv;
            bool stopping{false};
            bool running{false};
            uint64_t last_heartbeat{0};
        };

        runtime_impl& the_runtime();

        // ---- snapshots - TODO 611 ---------------------------------------------------

        constexpr const char* snapshot_tag = "barch-snapshot-1";

        nuraft::ptr<nuraft::snapshot> machine::last_snapshot() {
            auto* r = g.live.load();
            return r ? r->last_snapshot() : nullptr;
        }

        void machine::create_snapshot(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done) {
            g.snapshot_async(s, done);
        }

        int machine::read_logical_snp_obj(nuraft::snapshot& s, void*&, nuraft::ulong obj_id,
                                          nuraft::ptr<nuraft::buffer>& out, bool& is_last) {
            // one object: where to copy from. Asked for again, it's the same one
            (void) obj_id;
            const std::string what = std::string(snapshot_tag) + " " + g.rt.my_rpc() + " "
                                     + std::to_string(s.get_last_log_idx());
            out = nuraft::buffer::alloc(what.size());
            std::memcpy(out->data_begin(), what.data(), what.size());
            is_last = true;
            return 0;
        }

        void machine::save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id, nuraft::buffer& data,
                                           bool, bool) {
            g.received_ok = false;
            std::istringstream in(std::string((const char*) data.data_begin(), data.size()));
            std::string tag, from;
            uint64_t at = 0;
            if (!(in >> tag >> from >> at) || tag != snapshot_tag) {
                barch::err({"raft group", (uint64_t) g.number, "got a snapshot this build can't read"});
                ++obj_id;
                return;
            }
            barch::log({"raft group", (uint64_t) g.number, "copying a snapshot at", (uint64_t) at,
                        "from", from});
            std::string err;
            if (g.rt.copy_from(from, g.spaces, err)) {
                g.received_ok = true;
            } else {
                barch::err({"raft group", (uint64_t) g.number, "couldn't copy the snapshot -", err});
            }
            (void) s;
            ++obj_id;
        }

        bool machine::apply_snapshot(nuraft::snapshot& s) {
            // false makes NuRaft send it again
            if (!g.received_ok.exchange(false))
                return false;
            if (auto* r = g.live.load())
                r->save_snapshot(s);
            g.applied = s.get_last_log_idx();
            ++g.snapshots_installed;
            return true;
        }

        void group_rt::snapshot_async(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done) {
            auto copy = nuraft::snapshot::deserialize(*s.serialize());
            std::lock_guard l(snap_lock);
            // NuRaft asks for one at a time, so the last one has finished by now
            if (snap_thread.joinable())
                snap_thread.join();
            snap_thread = std::thread([this, copy, done]() mutable {
                bool ok = true;
                std::string err;
                for (const auto& space : spaces) {
                    if (!local(space, {"SAVE"}, nullptr, err)) {
                        ok = false;
                        barch::err({"raft group", (uint64_t) number, "couldn't save", space,
                                    "for a snapshot -", err});
                        break;
                    }
                }
                if (ok) {
                    if (auto* r = live.load()) {
                        r->save_snapshot(*copy);
                        ++snapshots_made;
                    }
                }
                nuraft::ptr<std::exception> none;
                done(ok, none);
            });
        }

        // ---- group_rt ---------------------------------------------------------------

        bool group_rt::start(int32_t server_id, bool join, std::string& err) {
            incarnation = random_u64();
            applied = 0;
            leader_from = UINT64_MAX;
            needs_rebuild = false;
            sm = nuraft::cs_new<machine>(*this);
            raft_group::options o;
            o.group = number;
            o.dir = std::filesystem::path(barch::data_path("cluster")).string();
            o.host = barch::get_external_host();
            o.base_port = (int) barch::get_raft_port();
            o.server_id = server_id;
            o.join = join;
            o.snapshot_distance = (int) barch::get_raft_snapshot_entries();
            // keep a little log behind each snapshot, so a member just behind it
            // catches up from the log rather than from a copy
            o.reserved_entries = std::max(10, o.snapshot_distance / 4);
            applied_here = 0;
            auto r = std::make_shared<raft_group>(o, sm);
            live = r.get();
            // the group outlives its callbacks: stop() shuts NuRaft down before it goes
            auto* events_from = r.get();
            r->on_event([this, events_from](nuraft::cb_func::Type t, nuraft::cb_func::Param*) {
                if (t == nuraft::cb_func::BecomeLeader) {
                    // it appended a configuration entry as it took over. Once that's
                    // applied, so is everything an earlier leader committed
                    leader_from = events_from->last_index();
                } else if (t == nuraft::cb_func::BecomeFollower) {
                    leader_from = UINT64_MAX;
                }
            });
            if (!r->start(err)) {
                live = nullptr;
                return false;
            }
            std::lock_guard l(raft_lock);
            raft = r;
            return true;
        }

        void group_rt::stop() {
            std::shared_ptr<raft_group> r;
            {
                std::lock_guard l(raft_lock);
                r = std::move(raft);
                raft.reset();
            }
            {
                // a snapshot being saved finishes before NuRaft goes
                std::lock_guard sl(snap_lock);
                if (snap_thread.joinable())
                    snap_thread.join();
            }
            if (r) r->stop();
            live = nullptr;
            leader_from = UINT64_MAX;
        }

        std::string group_rt::not_leader() const {
            auto raft = rg();
            if (!raft)
                return "TRYAGAIN this node hasn't joined the space's raft group yet";
            if (needs_rebuild)
                return "TRYAGAIN this node's copy of the space is being rebuilt";
            if (raft->is_leader())
                return "TRYAGAIN the new leader is still applying what came before it";
            const int32_t l = raft->leader_id();
            const std::string where = l > 0 ? rt.address_of(l) : std::string{};
            return where.empty() ? std::string("NOTLEADER no leader is known yet")
                                 : "NOTLEADER " + where;
        }

        binding::result group_rt::commit(const std::string& record, std::string& why) {
            if (!ready()) {
                why = not_leader();
                return binding::result::refused;
            }
            auto buf = nuraft::buffer::alloc(9 + record.size());
            auto* p = buf->data_begin();
            p[0] = entry_version;
            const uint64_t inc = incarnation.load();
            for (int i = 0; i < 8; ++i) p[1 + i] = (uint8_t) ((inc >> (8 * i)) & 0xff);
            std::memcpy(p + 9, record.data(), record.size());
            uint64_t idx = 0;
            std::string w;
            auto raft = rg();
            if (!raft) {
                why = not_leader();
                return binding::result::refused;
            }
            switch (raft->append(buf, idx, w)) {
                case raft_group::outcome::committed:
                    return binding::result::committed;
                case raft_group::outcome::refused:
                    why = raft->is_leader() ? "ERR the write was refused: " + w : not_leader();
                    return binding::result::refused;
                case raft_group::outcome::unknown:
                    // it may commit yet, and this copy already has it: rebuild from
                    // the group rather than guess
                    needs_rebuild = true;
                    why = "UNKNOWN leadership changed before the write was known to commit (" + w
                          + ") - it may or may not have happened";
                    return binding::result::unknown;
            }
            return binding::result::refused;
        }

        // ---- runtime_impl -----------------------------------------------------------

        int32_t runtime_impl::my_num() {
            std::lock_guard l(nodes_lock);
            for (const auto& n : nodes)
                if (n.id == node_id) return n.num;
            return 0;
        }

        bool runtime_impl::start(std::string& err) {
            raft_port = (int) barch::get_raft_port();
            if (raft_port == 0)
                return true;
            dir = barch::data_path("cluster");
            host = barch::get_external_host();
            server_port = (int) barch::get_server_port();
            heartbeat_ms = barch::get_cluster_heartbeat_ms();
            node_id = barch::repl::this_node();
            layout = layout_tag();
            try {
                std::filesystem::create_directories(dir);
            } catch (const std::exception& e) {
                err = "can't make " + dir + ": " + e.what();
                return false;
            }
            if (std::filesystem::exists(group_file(0))) {
                // a member already: the cluster group comes back from its log, and
                // the data spaces' groups follow from what it says
                auto g = open_group(0, {cluster_space, config_space});
                if (!start_group(g, 0, false, err))
                    return false;
                // and the data spaces' groups before the listener opens: until its
                // group is bound, a space would take writes the log never sees
                refresh_nodes();
                open_spaces();
            } else {
                barch::log({"cluster: raft_port is", (int64_t) raft_port,
                            "- CLUSTER INIT starts a cluster here, CLUSTER JOIN joins one"});
            }
            stopping = false;
            running = true;
            ticker = std::thread([this] {
                std::unique_lock l(tick_lock);
                while (!stopping) {
                    tick_cv.wait_for(l, std::chrono::milliseconds(250));
                    if (stopping) break;
                    l.unlock();
                    try {
                        tick();
                    } catch (const std::exception& e) {
                        barch::err({"cluster:", e.what()});
                    }
                    l.lock();
                }
            });
            return true;
        }

        void runtime_impl::stop() {
            if (!running)
                return;
            {
                std::lock_guard l(tick_lock);
                stopping = true;
            }
            tick_cv.notify_all();
            if (ticker.joinable())
                ticker.join();
            std::vector<std::shared_ptr<group_rt>> all;
            {
                std::lock_guard l(groups_lock);
                for (auto& [n, g] : groups) all.push_back(g);
            }
            for (auto& g : all) {
                detach(g->spaces);
                g->stop();
            }
            running = false;
        }

        std::shared_ptr<group_rt> runtime_impl::open_group(uint32_t number, const std::vector<std::string>& spaces) {
            std::lock_guard l(groups_lock);
            auto& g = groups[number];
            if (!g)
                g = std::make_shared<group_rt>(*this, number, spaces);
            return g;
        }

        /*
         * Start a group and bind its spaces to it. They're bound before the group
         * applies anything, so a space the first entry opens takes its binding as
         * it's built; and attached to the ones already open.
         */
        bool runtime_impl::bind_group(const std::shared_ptr<group_rt>& g, std::string& err) {
            auto b = std::make_shared<space_binding>(g);
            for (const auto& s : g->spaces)
                bind(s, b);
            for (const auto& s : g->spaces) {
                if (auto ks = barch::get_keyspace(s); ks && !ks->attach_raft(b, err)) {
                    detach(g->spaces);
                    return false;
                }
            }
            return true;
        }

        bool runtime_impl::start_group(const std::shared_ptr<group_rt>& g, int32_t server_id, bool join,
                                       std::string& err) {
            if (!bind_group(g, err))
                return false;
            if (!g->start(server_id, join, err)) {
                detach(g->spaces);
                return false;
            }
            return true;
        }

        void runtime_impl::detach(const std::vector<std::string>& spaces) {
            for (const auto& s : spaces) {
                unbind(s);
                std::string ignored;
                if (auto ks = barch::get_keyspace(s))
                    ks->attach_raft(nullptr, ignored);
            }
        }

        bool runtime_impl::copy_from(const std::string& rpc_address, const std::vector<std::string>& spaces,
                                     std::string& err) {
            std::string h;
            int p = 0;
            if (!split_address(rpc_address, h, p)) {
                err = "no address to copy from: " + rpc_address;
                return false;
            }
            std::vector<std::string> theirs;
            if (!remote(rpc_address, {"CLUSTER", "LAYOUT"}, &theirs, err, 5000) || theirs.empty()) {
                err = "asking " + rpc_address + " for its page layout: " + err;
                return false;
            }
            const bool same = theirs[0] == layout;
            for (const auto& s : spaces) {
                // RETRIEVE: every shard of the space from the other node, under its
                // CoW freeze, and in here only once all of them arrived. Pages only
                // load where they were written the same way; otherwise key by key
                const bool ok = same ? local(s, {"RETRIEVE", h, std::to_string(p)}, nullptr, err)
                                     : logical_copy(rpc_address, s, err);
                if (!ok) {
                    err = "copying " + s + " from " + rpc_address + ": " + err;
                    return false;
                }
            }
            return true;
        }

        /*
         * A space key by key from another node - TODO 612. This copy is cleared,
         * then the other node hands over each shard a page of leaves at a time, as
         * change log records, walked the way SCAN walks: every key present for the
         * whole walk comes over at least once. A key written or erased during the
         * walk may or may not; the log from the snapshot this copy stands for has
         * every such write after it, and applying those over the copy ends where
         * the log does, since records are absolute.
         */
        bool runtime_impl::logical_copy(const std::string& from, const std::string& space, std::string& err) {
            auto ks = barch::get_keyspace(space);
            barch::sharded_store store(ks);
            {
                repl::applying applying;
                store.clear_space();
            }
            uint64_t records = 0;
            const size_t shards = ks->get_shard_count();
            for (size_t shard = 0; shard < shards; ++shard) {
                uint64_t page = 0;
                for (;;) {
                    std::vector<std::string> got;
                    if (!remote(from, {"CLUSTER", "EXPORT", space, std::to_string(shard), std::to_string(page)},
                                &got, err, 30000))
                        return false;
                    if (got.empty()) {
                        err = from + " answered EXPORT with nothing";
                        return false;
                    }
                    try {
                        page = std::stoull(got[0]);
                    } catch (...) {
                        err = from + " answered EXPORT with " + got[0];
                        return false;
                    }
                    for (size_t i = 1; i < got.size(); ++i) {
                        std::string bytes;
                        aof::record r;
                        if (!from_hex(got[i], bytes)
                            || aof::decode((const uint8_t*) bytes.data(), (uint32_t) bytes.size(), r)
                                   != aof::decoded::ok) {
                            err = "a record from " + from + " that doesn't decode";
                            return false;
                        }
                        repl::applying applying;
                        if (!repl::apply_one(r, err))
                            return false;
                        ++records;
                    }
                    if (page == 0) break;
                }
            }
            ++logical_copies;
            barch::log({"cluster: copied", space, "from", from, "key by key -", records, "records"});
            return true;
        }

        // EXPORT <space> <shard> <page> - one page of a shard's leaves, as records
        bool runtime_impl::export_page(const std::vector<std::string>& args, std::vector<std::string>& out,
                                       std::string& err) {
            if (args.size() != 4) {
                err = "ERR CLUSTER EXPORT space shard page";
                return false;
            }
            auto ks = barch::get_keyspace(args[1]);
            size_t shard = 0, page = 0;
            try {
                shard = std::stoull(args[2]);
                page = std::stoull(args[3]);
            } catch (...) {
                err = "ERR shard and page are numbers";
                return false;
            }
            if (!ks || shard >= ks->get_shard_count()) {
                err = "ERR no such shard";
                return false;
            }
            auto t = ks->get(shard);
            barch::sharded_store store(ks);
            out.emplace_back();
            const bool more = store.leaf_page(shard, page, [&](const art::leaf& l) {
                const auto key = l.get_key();
                if (barch::meta::is_meta(key)) {
                    // the dictionary is each node's own, as it is for PUBLISH
                    std::string name;
                    if (barch::meta::name_of(key, name) && name == dictionary::meta_name)
                        return;
                }
                aof::record r;
                r.type = aof::record_type::set;
                r.expiry_ms = (int64_t) l.expiry_ms();
                r.shard = (uint32_t) shard;
                r.shard_count = (uint32_t) t->space_shards.load(std::memory_order_relaxed);
                r.routing = t->space_routing.load(std::memory_order_relaxed);
                r.space = t->space_name();
                r.key.assign(key.chars(), key.size);
                auto flags = art::key_options((int64_t) l.expiry_ms(), false, l.is_volatile(), l.is_hashed(),
                                              l.is_compressed()).flags;
                const auto v = l.get_value();
                if (l.is_compressed()) {
                    const auto plain = dictionary::decompress(t->space_name(), v);
                    r.value.assign(plain.chars(), plain.size);
                    flags &= ~art::key_options::flag_is_compressed;
                } else {
                    r.value.assign(v.chars(), v.size);
                }
                r.options = flags;
                std::vector<uint8_t> encoded;
                aof::encode(r, encoded);
                out.push_back(to_hex(std::string((const char*) encoded.data(), encoded.size())));
            });
            out[0] = more ? std::to_string(page) : std::string("0");
            return true;
        }

        bool runtime_impl::init(std::string& err) {
            std::lock_guard a(admin);
            if (raft_port == 0) {
                err = "ERR set raft_port first";
                return false;
            }
            if (std::filesystem::exists(group_file(0))) {
                err = "ERR this node is already in a cluster";
                return false;
            }
            auto g = open_group(0, {cluster_space, config_space});
            if (!start_group(g, 1, false, err))
                return false;
            const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (!g->ready() && std::chrono::steady_clock::now() < until)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            if (!g->ready()) {
                err = "ERR the new cluster group didn't elect this node";
                return false;
            }
            if (!local(cluster_space, {"HSET", "node:" + node_id, "num", "1", "rpc", my_rpc(), "raft", my_raft(),
                                       "role", "voter", "joined", std::to_string(now_ms()), "layout", layout},
                       nullptr, err))
                return false;
            refresh_nodes();
            barch::log({"cluster: started a new cluster as node 1,", node_id});
            return true;
        }

        bool runtime_impl::reserve(std::vector<std::string>& out, std::string& err) {
            auto g0 = open_group(0, {cluster_space, config_space});
            if (!g0->ready()) {
                err = g0->not_leader();
                return false;
            }
            refresh_nodes();
            std::lock_guard l(nodes_lock);
            int32_t top = reserved;
            for (const auto& n : nodes) top = std::max(top, n.num);
            reserved = top + 1;
            out.push_back(std::to_string(reserved));
            return true;
        }

        // ADMIT <num> <id> <rpc> <raft> - on the leader, for a node that's waiting to be added
        bool runtime_impl::admit(const std::vector<std::string>& args, std::string& err) {
            std::lock_guard a(admin);
            if (args.size() != 5 && args.size() != 6) {
                err = "ERR CLUSTER ADMIT num id rpc raft [layout]";
                return false;
            }
            auto g0 = open_group(0, {cluster_space, config_space});
            if (!g0->ready()) {
                err = g0->not_leader();
                return false;
            }
            int32_t num = 0;
            try {
                num = std::stoi(args[1]);
            } catch (...) {
                err = "ERR not a number: " + args[1];
                return false;
            }
            std::string h;
            int p = 0;
            if (!split_address(args[4], h, p)) {
                err = "ERR not a host:port - " + args[4];
                return false;
            }
            auto r0 = g0->rg();
            if (!r0 || !r0->add_member(num, raft_group::endpoint_for(h, p, 0), 20000, err, true)) {
                err = "ERR " + err;
                return false;
            }
            if (!local(cluster_space, {"HSET", "node:" + args[2], "num", args[1], "rpc", args[3], "raft", args[4],
                                       "role", "voter", "joined", std::to_string(now_ms()),
                                       "layout", args.size() == 6 ? args[5] : std::string("unknown")},
                       nullptr, err))
                return false;
            refresh_nodes();
            barch::log({"cluster: added node", (int64_t) num, args[2], "at", args[3]});
            return true;
        }

        /*
         * JOIN, on the node joining. It asks the leader for a number, copies the
         * cluster and configuration spaces from it, starts the cluster group
         * waiting to be added, and asks the leader to add it. The log then
         * replays over the copy, which ends where the leader is.
         */
        bool runtime_impl::join(const std::string& address, std::string& err) {
            std::lock_guard a(admin);
            if (raft_port == 0) {
                err = "ERR set raft_port first";
                return false;
            }
            if (std::filesystem::exists(group_file(0))) {
                err = "ERR this node is already in a cluster";
                return false;
            }
            std::string leader = address;
            std::vector<std::string> said;
            for (int hop = 0; hop < 3; ++hop) {
                said.clear();
                if (remote(leader, {"CLUSTER", "RESERVE"}, &said, err))
                    break;
                // follow a redirect to the leader
                if (err.rfind("NOTLEADER ", 0) == 0 && err.find("no leader") == std::string::npos) {
                    leader = err.substr(10);
                    continue;
                }
                return false;
            }
            if (said.empty()) {
                err = "ERR " + leader + " didn't give this node a number";
                return false;
            }
            const std::string num = said[0];
            int32_t n = 0;
            try {
                n = std::stoi(num);
            } catch (...) {
                err = "ERR " + leader + " answered RESERVE with " + num;
                return false;
            }
            if (!copy_from(leader, {cluster_space, config_space}, err))
                return false;
            auto g = open_group(0, {cluster_space, config_space});
            if (!start_group(g, n, true, err))
                return false;
            if (!remote(leader, {"CLUSTER", "ADMIT", num, node_id, my_rpc(), my_raft(), layout}, nullptr, err, 30000)) {
                // it never became a member, so it's as it was: no cluster
                detach(g->spaces);
                g->stop();
                {
                    std::lock_guard l(groups_lock);
                    groups.erase(0);
                }
                std::error_code ec;
                std::filesystem::remove(group_file(0), ec);
                return false;
            }
            refresh_nodes();
            barch::log({"cluster: joined as node", (int64_t) n, "through", leader});
            return true;
        }

        bool runtime_impl::remove(const std::string& id, std::string& err) {
            std::lock_guard a(admin);
            auto g0 = open_group(0, {cluster_space, config_space});
            if (!g0->ready()) {
                err = g0->not_leader();
                return false;
            }
            refresh_nodes();
            int32_t num = 0;
            {
                std::lock_guard l(nodes_lock);
                for (const auto& n : nodes)
                    if (n.id == id) num = n.num;
            }
            if (num == 0) {
                err = "ERR no node " + id;
                return false;
            }
            if (id == node_id) {
                err = "ERR a leader can't remove itself - remove it from another node once it no longer leads";
                return false;
            }
            auto r0 = g0->rg();
            if (!r0 || !r0->remove_member(num, 20000, err)) {
                err = "ERR " + err;
                return false;
            }
            for (const auto& k : keys_like("node:" + id + "*"))
                local(cluster_space, {"DEL", k}, nullptr, err);
            refresh_nodes();
            barch::log({"cluster: removed node", (int64_t) num, id});
            return true;
        }

        // HEARTBEAT <id> <space> <applied> <term> <leads> ... - on the cluster leader
        bool runtime_impl::heartbeat_in(const std::vector<std::string>& args, std::string& err) {
            auto g0 = open_group(0, {cluster_space, config_space});
            if (!g0->ready()) {
                err = g0->not_leader();
                return false;
            }
            if (args.size() < 2 || (args.size() - 2) % 4 != 0) {
                err = "ERR CLUSTER HEARTBEAT id [space applied term leads]...";
                return false;
            }
            const std::string& id = args[1];
            // the leader's clock, so every member stores the same time
            if (!local(cluster_space, {"SET", "node:" + id + ":seen", std::to_string(now_ms())}, nullptr, err))
                return false;
            for (size_t i = 2; i + 3 < args.size(); i += 4) {
                if (!local(cluster_space, {"HSET", "node:" + id + ":space:" + args[i], "applied", args[i + 1],
                                           "term", args[i + 2], "leader", args[i + 3]}, nullptr, err))
                    return false;
            }
            return true;
        }

        void runtime_impl::refresh_nodes() {
            auto fresh = read_nodes();
            std::lock_guard l(nodes_lock);
            nodes = fresh;
            addresses.clear();
            for (const auto& n : nodes)
                addresses[n.num] = n.rpc;
        }

        void runtime_impl::send_heartbeat() {
            std::vector<std::string> hb{"CLUSTER", "HEARTBEAT", node_id};
            {
                std::lock_guard l(groups_lock);
                for (const auto& [n, g] : groups) {
                    auto r = g->rg();
                    if (!r) continue;
                    for (const auto& s : g->spaces) {
                        hb.push_back(s);
                        hb.push_back(std::to_string(g->applied.load()));
                        hb.push_back(std::to_string(r->term()));
                        hb.push_back(r->is_leader() ? "1" : "0");
                    }
                }
            }
            auto g0 = open_group(0, {cluster_space, config_space});
            std::string err;
            std::vector<std::string> args(hb.begin() + 1, hb.end());
            if (g0->ready()) {
                heartbeat_in(args, err);
                return;
            }
            auto r0 = g0->rg();
            const int32_t l = r0 ? r0->leader_id() : -1;
            const std::string where = l > 0 ? address_of(l) : std::string{};
            if (!where.empty())
                remote(where, hb, nullptr, err, 3000);
        }

        /*
         * The cluster leader turns `<space>.raft on` in the configuration space into
         * a `space:<name>` record: the group it goes in, and which node creates
         * that group. Phase 1 takes one data space - TODO 610.
         */
        void runtime_impl::claim_spaces() {
            std::vector<std::string> wanted;
            for (const auto& k : [] {
                std::vector<std::string> out;
                std::string err;
                local(config_space, {"KEYS", "*.raft"}, &out, err);
                return out;
            }()) {
                std::vector<std::string> v;
                std::string err;
                if (!local(config_space, {"GET", k}, &v, err) || v.empty()) continue;
                const auto on = v[0];
                if (on == "on" || on == "1" || on == "true" || on == "yes")
                    wanted.push_back(k.substr(0, k.size() - 5));
            }
            auto have = read_spaces();
            std::set<std::string> claimed;
            uint32_t top = 0;
            for (const auto& s : have) {
                claimed.insert(s.name);
                top = std::max(top, s.group);
            }
            for (const auto& name : wanted) {
                if (claimed.count(name) || name == cluster_space || name == config_space || name.empty())
                    continue;
                std::string err;
                const auto me = my_num();
                if (local(cluster_space, {"HSET", "space:" + name, "group", std::to_string(top + 1), "mode", "raft",
                                          "creator", std::to_string(me)}, nullptr, err)) {
                    claimed.insert(name);
                    ++top;
                    barch::log({"cluster: space", name, "is replicated in raft group", (uint64_t) top});
                } else {
                    barch::err({"cluster: could not record space", name, "-", err});
                }
            }
        }

        /*
         * Every member opens the groups the cluster space lists. The creator
         * starts its group; anyone else copies the space from the creator and
         * waits to be added. After the first time each comes back from its log.
         */
        void runtime_impl::open_spaces() {
            const auto me = my_num();
            if (me == 0) return;
            for (const auto& s : read_spaces()) {
                std::shared_ptr<group_rt> g;
                {
                    std::lock_guard l(groups_lock);
                    auto i = groups.find(s.group);
                    if (i != groups.end() && i->second->running()) continue;
                }
                g = open_group(s.group, {s.name});
                std::string err;
                // bound first: from here the space refuses clients until its group
                // runs here, instead of answering from a copy nothing replicates to
                if (!bind_group(g, err)) {
                    barch::err({"cluster: space", s.name, "can't be bound to group", (uint64_t) s.group, "-", err});
                    continue;
                }
                const bool have_log = std::filesystem::exists(group_file(s.group));
                const bool creator = s.creator == me;
                if (!have_log && !creator) {
                    // from whoever leads it now, as the heartbeats say; the creator
                    // may not, or may be gone
                    std::string from = address_of(s.creator);
                    for (const auto& k : keys_like("node:*:space:" + s.name)) {
                        std::vector<std::string> flat;
                        std::string e;
                        if (!local(cluster_space, {"HGETALL", k}, &flat, e)) continue;
                        auto m = pairs(flat);
                        if (m["leader"] != "1") continue;
                        const auto id = k.substr(5, k.find(':', 5) - 5);
                        std::lock_guard l(nodes_lock);
                        for (const auto& n : nodes)
                            if (n.id == id && !n.rpc.empty()) from = n.rpc;
                    }
                    if (!copy_from(from, {s.name}, err)) {
                        barch::warn({"cluster: can't join group", (uint64_t) s.group, "for", s.name, "yet -", err});
                        continue;
                    }
                }
                if (!start_group(g, me, !have_log && !creator, err))
                    barch::err({"cluster: group", (uint64_t) s.group, "for", s.name, "didn't start -", err});
            }
        }

        /*
         * A data group's leader keeps its members the same as the cluster's. A
         * node in the cluster that isn't in the group is added; one in the group
         * that has left the cluster is removed.
         */
        void runtime_impl::reconcile(const std::shared_ptr<group_rt>& g) {
            auto r = g->rg();
            if (!r || !g->ready()) return;
            /*
             * A member joins as a learner and is made a voter once it's within a
             * few entries of the leader - TODO 611. Until then it doesn't count
             * towards a quorum, so a node copying a large space can't slow the
             * group's commits down or, by failing, cost it its majority.
             */
            const uint64_t top = r->last_index();
            for (const auto& m : r->members()) {
                if (!m.learner) continue;
                const uint64_t at = r->member_index(m.id);
                if (at + 8 < top) continue;
                std::string err;
                if (r->promote(m.id, 5000, err))
                    barch::log({"cluster: node", (int64_t) m.id, "is a voter in group", (uint64_t) g->number});
                else
                    barch::warn({"cluster: node", (int64_t) m.id, "not promoted in group", (uint64_t) g->number,
                                 "-", err});
                return;                             // one change at a time
            }
            // the cluster group's members come and go through JOIN and REMOVE
            if (g->number == 0) return;
            std::vector<node_info> all;
            {
                std::lock_guard l(nodes_lock);
                all = nodes;
            }
            if (all.empty()) return;
            std::set<int32_t> want;
            for (const auto& n : all) want.insert(n.num);
            std::set<int32_t> have;
            for (const auto& m : r->members()) have.insert(m.id);
            for (const auto& n : all) {
                if (have.count(n.num)) continue;
                std::string h, err;
                int p = 0;
                if (!split_address(n.raft, h, p)) continue;
                if (r->add_member(n.num, raft_group::endpoint_for(h, p, g->number), 5000, err, true))
                    barch::log({"cluster: node", (int64_t) n.num, "added to group", (uint64_t) g->number,
                                "as a learner"});
                return;                             // one change at a time
            }
            for (const auto id : have) {
                if (want.count(id) || id == r->my_id()) continue;
                std::string err;
                if (r->remove_member(id, 5000, err))
                    barch::log({"cluster: node", (int64_t) id, "removed from group", (uint64_t) g->number});
                return;
            }
            /*
             * Spread the leaders - TODO 612. Every group starts on the node that led
             * the cluster group when its space was replicated, so that one node's
             * copy is the one everyone gets. Then each group prefers a member of its
             * own: the voters in num order, group n the (n - 1)th, wrapping round.
             * The leader hands over to it once it has caught up, at most once every
             * ten seconds, so a preferred node that keeps failing doesn't keep the
             * group changing leaders.
             */
            std::vector<int32_t> voters;
            for (const auto& m : r->members())
                if (!m.learner) voters.push_back(m.id);
            std::sort(voters.begin(), voters.end());
            if (voters.size() < 2) return;
            const int32_t preferred = voters[(g->number - 1) % voters.size()];
            if (preferred == r->my_id()) return;
            if (r->member_index(preferred) + 8 < r->last_index()) return;
            const uint64_t now = now_ms();
            if (now - g->last_handover < 10000) return;
            g->last_handover = now;
            barch::log({"cluster: group", (uint64_t) g->number, "hands its leadership to node", (int64_t) preferred});
            r->hand_over(preferred);
        }

        /*
         * A write whose outcome was unknown, or an entry that couldn't be applied,
         * leaves this copy unlike the others. The group stops here, the spaces
         * are copied again from its leader, and it starts again as a new
         * incarnation, so the whole log is applied over the copy.
         */
        void runtime_impl::rebuild(const std::shared_ptr<group_rt>& g) {
            if (g->number == 0) {
                // the cluster space is small and the log holds all of it
                barch::warn({"cluster: the cluster group needs a rebuild - restarting it"});
            }
            auto r = g->rg();
            const int32_t leader = r ? r->leader_id() : -1;
            const auto me = my_num();
            if (leader == me && r) {
                /*
                 * Leading again: this node's log is the group's now, and every entry
                 * in it commits, the one whose outcome was unknown too. So this copy
                 * agrees with the log - unless that entry was cut out of the log
                 * before this node led again. Not knowing which, a group with others
                 * in it is handed to one of them and this copy rebuilt from it. A
                 * group of one has nobody to disagree with.
                 */
                if (r->members().size() <= 1)
                    g->needs_rebuild = false;
                else
                    r->yield_leadership();
                return;
            }
            if (leader <= 0) return;                    // not until there's a leader to copy from
            std::string err;
            const auto from = address_of(leader);
            barch::warn({"cluster: rebuilding group", (uint64_t) g->number, "from node", (int64_t) leader});
            detach(g->spaces);
            g->stop();
            if (!copy_from(from, g->spaces, err)) {
                barch::err({"cluster: rebuild of group", (uint64_t) g->number, "failed -", err});
            }
            if (!start_group(g, me, false, err))
                barch::err({"cluster: group", (uint64_t) g->number, "didn't start again -", err});
        }

        void runtime_impl::tick() {
            std::shared_ptr<group_rt> g0;
            {
                std::lock_guard l(groups_lock);
                auto i = groups.find(0);
                if (i == groups.end() || !i->second->running()) return;
                g0 = i->second;
            }
            refresh_nodes();
            if (g0->ready())
                claim_spaces();
            open_spaces();
            std::vector<std::shared_ptr<group_rt>> all;
            {
                std::lock_guard l(groups_lock);
                for (auto& [n, g] : groups) all.push_back(g);
            }
            for (auto& g : all) {
                if (g->needs_rebuild)
                    rebuild(g);
                else if (g->running())
                    reconcile(g);
            }
            if (now_ms() - last_heartbeat >= heartbeat_ms) {
                last_heartbeat = now_ms();
                send_heartbeat();
            }
        }

        void runtime_impl::info(std::vector<std::string>& out) {
            out.push_back("node " + node_id + " num " + std::to_string(my_num()));
            out.push_back("rpc " + my_rpc() + " raft " + (raft_port ? my_raft() : std::string("off"))
                          + " layout " + layout + " logical_copies " + std::to_string(logical_copies.load()));
            std::lock_guard l(groups_lock);
            for (const auto& [n, g] : groups) {
                std::ostringstream line;
                line << "group " << n << " spaces";
                for (const auto& s : g->spaces) line << " " << s;
                auto r = g->rg();
                if (!r || !r->running()) {
                    line << " stopped";
                } else {
                    size_t learners = 0;
                    const auto ms = r->members();
                    for (const auto& m : ms) learners += m.learner ? 1 : 0;
                    auto snap = r->last_snapshot();
                    line << " leader " << r->leader_id() << (g->ready() ? " (this node)" : "")
                         << " term " << r->term() << " committed " << r->committed_index()
                         << " applied " << g->applied.load() << " members " << ms.size()
                         << " learners " << learners
                         << " snapshot " << (snap ? snap->get_last_log_idx() : 0)
                         << " log_start " << r->start_index()
                         << " applied_here " << g->applied_here.load()
                         << " snapshots_made " << g->snapshots_made.load()
                         << " snapshots_installed " << g->snapshots_installed.load();
                    if (g->apply_failures) line << " apply_failures " << g->apply_failures.load();
                }
                out.push_back(line.str());
            }
        }

        bool runtime_impl::command(const std::vector<std::string>& args, std::vector<std::string>& out,
                                   std::string& err) {
            std::string sub = args.empty() ? std::string{} : args[0];
            for (auto& c : sub) c = (char) std::toupper((unsigned char) c);
            if (sub == "INFO") {
                info(out);
                return true;
            }
            if (sub == "DIGEST" && args.size() == 2) {
                /*
                 * This node's own copy of a space, as a key count and a checksum
                 * over its keys and values in key order - what tests compare across
                 * members. Plain string keys only; anything else counts as its key.
                 */
                std::vector<std::string> keys;
                if (!local(args[1], {"KEYS", "*"}, &keys, err)) return false;
                std::sort(keys.begin(), keys.end());
                uint32_t crc = 0;
                for (const auto& k : keys) {
                    std::vector<std::string> v;
                    std::string ignored;
                    local(args[1], {"GET", k}, &v, ignored);
                    std::string both = k;
                    both.push_back('\0');
                    if (!v.empty()) both += v[0];
                    both.push_back('\0');
                    crc = aof::crc32c((const uint8_t*) both.data(), both.size(), crc);
                }
                char hex[9];
                std::snprintf(hex, sizeof(hex), "%08x", crc);
                out.push_back("keys " + std::to_string(keys.size()) + " crc " + hex);
                out.push_back("applied " + [&] {
                    std::lock_guard l(groups_lock);
                    for (const auto& [n, g] : groups)
                        for (const auto& sp : g->spaces)
                            if (sp == args[1]) return std::to_string(g->applied.load());
                    return std::string("-");
                }());
                return true;
            }
            if (raft_port == 0) {
                err = "ERR raft_port is 0 - set it and restart to cluster this node";
                return false;
            }
            if (sub == "INIT" && args.size() == 1) {
                if (!init(err)) return false;
                out.push_back("OK");
                return true;
            }
            if (sub == "JOIN" && args.size() == 3) {
                if (!join(args[1] + ":" + args[2], err)) return false;
                out.push_back("OK");
                return true;
            }
            if (sub == "REMOVE" && args.size() == 2) {
                if (!remove(args[1], err)) return false;
                out.push_back("OK");
                return true;
            }
            if (sub == "RESERVE" && args.size() == 1)
                return reserve(out, err);
            if (sub == "ADMIT") {
                if (!admit(args, err)) return false;
                out.push_back("OK");
                return true;
            }
            if (sub == "LAYOUT" && args.size() == 1) {
                out.push_back(layout);
                return true;
            }
            if (sub == "EXPORT")
                return export_page(args, out, err);
            if (sub == "HEARTBEAT") {
                if (!heartbeat_in(args, err)) return false;
                out.push_back("OK");
                return true;
            }
            err = "ERR CLUSTER INIT | JOIN host port | REMOVE node-id | INFO | DIGEST space | LAYOUT";
            return false;
        }

        runtime_impl& the_runtime() {
            static auto* r = new runtime_impl;      // never destroyed - TODO 57
            return *r;
        }

        // installs itself when this file is linked in
        const bool installed = [] {
            install(&the_runtime());
            return true;
        }();
    }
}
