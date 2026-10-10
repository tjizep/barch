//
// The cluster - TODO 610, docs/CLUSTERING.md.
//
// Raft groups, the key spaces bound to them, the `cluster` space that records the
// members, and CLUSTER. Only built with BARCH_CLUSTER; installs itself into
// cluster_hooks when it's linked in.
//
#include "cluster_hooks.h"
#include "raft_group.h"
#include "raft_auth.h"

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
#include <shared_mutex>
#include <sstream>
#include <thread>

namespace barch::cluster {
    namespace {
        constexpr const char* cluster_space = "cluster";
        constexpr const char* config_space = "configuration";
        constexpr uint8_t entry_version = 1;
        // a control entry: what follows is a line of words, not a record - TODO 616
        constexpr uint8_t control_version = 2;
        /*
         * A record the writer didn't apply - TODO 632. An entry_version record was
         * applied by the leader that wrote it before it went in the log, and the
         * leader skips it as it commits (by incarnation). This one was taken back
         * out of the leader's tree while it committed, so every member applies it as
         * it commits, the leader too, in log order: nothing uncommitted is ever in
         * the tree for a reader or a save to see.
         */
        constexpr uint8_t entry_version_unapplied = 3;

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

        /*
         * One group of a replicated space, from its `space:<name>` record. A space
         * split over N groups (TODO 615) has groups g .. g+N-1, the k-th owning the
         * k-th of N even runs of its shards.
         */
        struct space_info {
            std::string name;
            uint32_t group{0};
            int32_t creator{0};
            std::string mode;
            uint32_t k{0};
            uint32_t of{1};
            // the record's own group: the one the space was first replicated in, and
            // the only one its `creator` started
            uint32_t first{0};
            // after a split the record lists every group's run itself - TODO 616
            std::string given_label{};
            bool given_run{false};
            size_t run_from{0};
            size_t run_to{SIZE_MAX};
            // the shard count the leader decided for it, 0 in a record from before
            // that - TODO 631
            size_t shards{0};
            [[nodiscard]] std::string label() const {
                if (!given_label.empty()) return given_label;
                return of > 1 ? name + "/" + std::to_string(k) : name;
            }
        };

        /** the shards [from, to) the k-th of `of` groups owns, of `shards` */
        std::pair<size_t, size_t> shard_run(size_t shards, uint32_t k, uint32_t of) {
            if (of <= 1) return {0, SIZE_MAX};
            return {shards * k / of, shards * (k + 1) / of};
        }

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
                    s.of = m["groups"].empty() ? 1 : (uint32_t) std::stoul(m["groups"]);
                } catch (...) {
                    continue;
                }
                s.mode = m["mode"];
                if (const auto n = m["shards"]; !n.empty()) {
                    try {
                        s.shards = std::stoull(n);
                    } catch (...) {}
                }
                if (s.of == 0) s.of = 1;
                s.first = s.group;
                if (const auto runs = m["runs"]; !runs.empty()) {
                    // `<group>:<label>:<from>-<to>;...` - TODO 616
                    std::istringstream rs(runs);
                    for (std::string run; std::getline(rs, run, ';');) {
                        const auto c1 = run.find(':'), c2 = run.rfind(':'), dash = run.rfind('-');
                        if (c1 == std::string::npos || c2 == c1 || dash == std::string::npos || dash < c2) continue;
                        space_info e = s;
                        try {
                            e.group = (uint32_t) std::stoul(run.substr(0, c1));
                            e.run_from = std::stoull(run.substr(c2 + 1, dash - c2 - 1));
                            e.run_to = std::stoull(run.substr(dash + 1));
                        } catch (...) {
                            continue;
                        }
                        e.given_label = run.substr(c1 + 1, c2 - c1 - 1);
                        e.given_run = true;
                        spaces.push_back(e);
                    }
                    continue;
                }
                const uint32_t first = s.group;
                for (uint32_t i = 0; i < s.of; ++i) {
                    s.k = i;
                    s.group = first + i;
                    spaces.push_back(s);
                }
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
            bool copy_step(const std::string& from, uint64_t at);
            group_rt& g;
        };

        class runtime_impl;

        class group_rt {
        public:
            group_rt(runtime_impl& rt, uint32_t number, std::vector<std::string> spaces,
                     std::string label = {}, size_t from = 0, size_t to = SIZE_MAX)
                : rt(rt), number(number), spaces(std::move(spaces)),
                  label(label.empty() ? this->spaces.front() : std::move(label)), from(from), to(to) {}

            // a split's new group starts with these voters - TODO 616
            std::vector<int32_t> initial_members{};

            runtime_impl& rt;
            const uint32_t number;
            const std::vector<std::string> spaces;
            // its name in sessions, heartbeats and routes, and the shards it owns
            // of its space - all of them, unless the space is split - TODO 615
            const std::string label;
            // a split moves the tail of the run to a new group - TODO 616
            std::atomic<size_t> from;
            std::atomic<size_t> to;
            // shards from here up are on their way to another group: refused here
            std::atomic<size_t> fence_from{SIZE_MAX};
            [[nodiscard]] bool split() const { return from.load() != 0 || to.load() != SIZE_MAX; }
            binding_ptr binding{};                          // the cluster's thread only
            nuraft::ptr<machine> sm;
            std::atomic<uint64_t> incarnation{0};
            std::atomic<uint64_t> applied{0};
            std::atomic<uint64_t> leader_from{UINT64_MAX};
            std::atomic<uint64_t> apply_failures{0};
            // entries of this group for shards it no longer owns: written after a
            // hand-off of their shard, which the fence is there to stop - TODO 617
            std::atomic<uint64_t> strays{0};
            std::atomic<bool> needs_rebuild{false};
            // whether the rebuild under way has been logged - TODO 628
            std::atomic<bool> rebuild_noted{false};
            // entries applied here since the group started, its own skipped - TODO 611
            std::atomic<uint64_t> applied_here{0};
            std::atomic<uint64_t> snapshots_made{0};
            std::atomic<uint64_t> snapshots_installed{0};
            uint64_t last_handover{0};                      // the cluster's thread only
            // each learner's mark: the leader's last index, and when it was read
            std::map<int32_t, std::pair<uint64_t, uint64_t>> learner_marks; // the cluster's thread only
            // the group's raft_group from before start() publishes it, for the
            // state machine NuRaft calls while it starts
            std::atomic<raft_group*> live{nullptr};
            // what a snapshot being received copied, and whether that worked
            std::atomic<bool> received_ok{false};
            /*
             * A snapshot being received is copied on a thread of its own - TODO 628.
             * NuRaft holds its lock while it hands this node a snapshot object, and
             * a copy made there kept it for as long as the copy took: the node
             * answered nothing else, and a leader with only this node to make a
             * quorum with lost its lease.
             */
            // set by stop() before it takes the snapshot and copy threads, so NuRaft,
            // still running until stop() ends, can't start another behind it - TODO 638
            std::atomic<bool> stopping{false};
            enum class copy_state { none, running, done, failed };
            std::mutex copy_lock;
            std::thread copy_thread;                        // under copy_lock
            std::string copy_of;                            // "<from> <index>", under copy_lock
            copy_state copying{copy_state::none};           // under copy_lock

            // a copy still going when the group goes is waited for; stop() does it
            // first, as a rule, and this is for the one that didn't
            ~group_rt() {
                if (copy_thread.joinable())
                    copy_thread.join();
            }
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
            // writes hold it shared from their fence check until they're in the log;
            // a hand-off holds it alone to set the fence - TODO 616
            std::shared_mutex append_lock;
            std::mutex snap_lock;
            std::thread snap_thread;                        // under snap_lock
        public:
            binding::result commit(const std::string& record, size_t shard, std::string& why,
                                   bool writer_applied = true);
            /** a control entry, on the commit thread: changes nothing that needs a shard latch */
            void apply_control(const std::string& line, uint64_t idx);
            /** append a hand-off of the shards [m, to) to group `h`, as the leader; waits for the commit */
            bool hand_off(size_t m, uint32_t h, const std::string& new_label, std::string& err);
            [[nodiscard]] std::string not_leader() const;
        };

        nuraft::ptr<nuraft::buffer> machine::commit(const nuraft::ulong idx, nuraft::buffer& data) {
            const auto* p = data.data_begin();
            const size_t n = data.size();
            if (n >= 1 && p[0] == control_version) {
                // a hand-off, applied by every member in log order - TODO 616
                g.apply_control(std::string((const char*) p + 1, n - 1), idx);
            } else if (n >= 9 && (p[0] == entry_version || p[0] == entry_version_unapplied)) {
                uint64_t inc = 0;
                for (int i = 7; i >= 0; --i) inc = (inc << 8) | p[1 + i];
                // this leader's own entries were applied as they were written, unless
                // the writer took them back out to commit first - TODO 632
                if (p[0] == entry_version_unapplied || inc != g.incarnation.load()) {
                    aof::record r;
                    const auto d = aof::decode(p + 9, (uint32_t) (n - 9), r);
                    std::string why;
                    bool ok = false;
                    if (d != aof::decoded::ok) {
                        why = std::string(aof::describe(d));
                    } else {
                        if (g.split() && (r.type == aof::record_type::set || r.type == aof::record_type::erase)
                            && (r.shard < g.from.load() || r.shard >= g.to.load())) {
                            ++g.strays;
                            barch::err({"raft group", (uint64_t) g.number, "entry", (uint64_t) idx,
                                        "is for shard", (uint64_t) r.shard, "which it handed off"});
                        }
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
            result commit(const std::string& record, size_t shard, std::string& why,
                          bool writer_applied) override {
                return g->commit(record, shard, why, writer_applied);
            }
            [[nodiscard]] bool fenced(size_t shard, std::string& why) const override {
                if (shard < g->fence_from.load())
                    return false;
                why = "TRYAGAIN this shard is moving to another raft group";
                return true;
            }
            [[nodiscard]] bool leader() const override { return g->ready(); }
            [[nodiscard]] bool leader_read() const override {
                auto r = g->rg();
                return r && g->ready() && r->lease_valid();
            }
            [[nodiscard]] bool follower_read(uint64_t after, int wait_ms) const override {
                const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_ms);
                for (;;) {
                    auto r = g->rg();
                    // a follower cut off from every leader may be any distance behind
                    if (!r || g->needs_rebuild || r->is_leader() || !r->leader_alive())
                        return false;
                    if (g->applied.load() >= after)
                        return true;
                    if (std::chrono::steady_clock::now() >= until)
                        return false;
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
            }
            [[nodiscard]] uint64_t applied() const override { return g->applied.load(); }
            [[nodiscard]] const std::string& label() const override { return g->label; }
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
            bool copy_from(const std::string& rpc_address, const std::vector<std::string>& spaces, std::string& err,
                           size_t shard_from = 0, size_t shard_to = SIZE_MAX);
            bool logical_copy(const std::string& from, const std::string& space, std::string& err,
                              size_t shard_from = 0, size_t shard_to = SIZE_MAX);
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
            void routes(std::vector<std::string>& out);
        public:
            /** a hand-off a group applied, for the cluster's thread to finish - TODO 616 */
            void queue_handoff(uint32_t from_group, const std::string& space, size_t m, size_t b, uint32_t h,
                               const std::string& label, const std::vector<int32_t>& voters);
            /** where a member's server for a group listens, from the cluster space */
            std::string raft_endpoint(int32_t num, uint32_t group);
        private:
            struct handoff {
                uint32_t from_group{0};
                std::string space;
                size_t m{0}, b{0};
                uint32_t h{0};
                std::string label;
                std::vector<int32_t> voters;
            };
            std::mutex handoffs_lock;
            std::vector<handoff> handoffs;
            void process_handoffs();
            bool stored_range(uint32_t group, size_t& from, size_t& to);
            bool split(const std::string& label, std::vector<std::string>& out, std::string& err);

            std::shared_ptr<group_rt> open_group(uint32_t number, const std::vector<std::string>& spaces,
                                                 const std::string& label = {}, size_t from = 0,
                                                 size_t to = SIZE_MAX);
            bool bind_group(const std::shared_ptr<group_rt>& g, std::string& err);
            bool start_group(const std::shared_ptr<group_rt>& g, int32_t server_id, bool join, std::string& err);
            void detach(const std::shared_ptr<group_rt>& g);
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

        // ---- splitting a group - TODO 616 -------------------------------------------

        /*
         * `handoff <space> <m> <b> <h> <label> <voter,...>`: the shards [m, b) of the
         * space leave this group for group h, labelled <label>, whose first voters
         * are this group's then. Every member applies it at the same index, so every
         * member's copy of those shards is the same there, and h can start over them
         * as they are. Applying it fences the shards here and keeps the group's new
         * range in its file; starting h and moving the shards to it takes their
         * latches, so the cluster's thread does that, not this one.
         */
        void group_rt::apply_control(const std::string& line, uint64_t idx) {
            std::istringstream in(line);
            std::string word, space, label, voters;
            size_t m = 0, b = 0;
            uint32_t h = 0;
            if (!(in >> word >> space >> m >> b >> h >> label >> voters) || word != "handoff") {
                barch::err({"raft group", (uint64_t) number, "control entry", idx, "isn't one this build reads:", line});
                return;
            }
            if (to.load() <= m)
                return;                                 // applied before: a replay
            fence_from = m;
            to = m;
            if (auto* r = live.load())
                r->save_range(from.load(), m);
            std::vector<int32_t> ids;
            std::istringstream vs(voters);
            for (std::string v; std::getline(vs, v, ',');) {
                try {
                    ids.push_back(std::stoi(v));
                } catch (...) {}
            }
            barch::log({"raft group", (uint64_t) number, "hands shards", (uint64_t) m, "to", (uint64_t) (b - 1),
                        "of", space, "to group", (uint64_t) h, "at", idx});
            rt.queue_handoff(number, space, m, b, h, label, ids);
        }

        bool group_rt::hand_off(size_t m, uint32_t h, const std::string& new_label, std::string& err) {
            auto r = rg();
            if (!r || !ready()) {
                err = not_leader();
                return false;
            }
            // a space's only group owns "all the shards"; the new group gets a real end
            size_t b = to.load();
            if (auto ks = barch::get_keyspace(spaces.front()))
                b = std::min(b, ks->get_shard_count());
            if (m <= from.load() || m >= b) {
                err = "ERR shard " + std::to_string(m) + " isn't inside this group's run";
                return false;
            }
            {
                // every write already past the fence check is in the log once this
                // has it, so the hand-off comes after them all
                std::unique_lock fencing(append_lock);
                if (fence_from.load() > m)
                    fence_from = m;
            }
            std::string voters;
            for (const auto id : r->voters())
                voters += (voters.empty() ? "" : ",") + std::to_string(id);
            const std::string line = "handoff " + spaces.front() + " " + std::to_string(m) + " "
                                     + std::to_string(b) + " " + std::to_string(h) + " " + new_label + " " + voters;
            auto buf = nuraft::buffer::alloc(1 + line.size());
            buf->data_begin()[0] = control_version;
            std::memcpy(buf->data_begin() + 1, line.data(), line.size());
            uint64_t idx = 0;
            std::string why;
            auto outcome = r->append(buf, idx, why);
            // test knob, TODO 617: the first hand-off commits but its answer is lost
            static std::atomic<bool> lose_answer{std::getenv("BARCH_TEST_LOSE_HANDOFF") != nullptr};
            if (outcome == raft_group::outcome::committed && lose_answer.exchange(false)) {
                outcome = raft_group::outcome::unknown;
                why = "answer dropped by BARCH_TEST_LOSE_HANDOFF";
            }
            switch (outcome) {
                case raft_group::outcome::committed:
                    return true;
                case raft_group::outcome::refused:
                    fence_from = SIZE_MAX;              // nothing was handed off
                    err = "ERR the hand-off was refused: " + why;
                    return false;
                case raft_group::outcome::unknown:
                    // it may commit yet: the shards stay fenced, and SPLIT again finishes it
                    err = "UNKNOWN the hand-off may or may not have committed (" + why + ") - split again";
                    return false;
            }
            return false;
        }

        // ---- snapshots - TODO 611 ---------------------------------------------------

        // three objects - TODO 636; "barch-snapshot-1" is an older leader's two
        constexpr const char* snapshot_tag = "barch-snapshot-2";
        constexpr const char* snapshot_tag_v1 = "barch-snapshot-1";

        nuraft::ptr<nuraft::snapshot> machine::last_snapshot() {
            auto* r = g.live.load();
            return r ? r->last_snapshot() : nullptr;
        }

        void machine::create_snapshot(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done) {
            g.snapshot_async(s, done);
        }

        /*
         * A snapshot is three objects - TODO 627, 636. The first says where to copy
         * from, and the member starts the copy when it gets it. The second says the
         * same again, and the member asks for it again until the copy of that
         * snapshot has worked; one that failed is started again. The third, the last,
         * carries nothing.
         *
         * NuRaft compacts the member's log before it applies a snapshot, and an apply
         * that fails after that stops the process, so the copy can't be what the last
         * object does: a source that went away mid-copy (a leader restarting, say)
         * used to take the member down with it (627). And it can't be the first
         * object the member waits on either: while a transfer is at its first object
         * the leader sends its newest snapshot each time, so a copy that took longer
         * than the leader took to make another was never of the snapshot being sent,
         * and the member copied one after another for as long as writes kept coming
         * (636). Past the first object the leader keeps to the same snapshot.
         */
        constexpr const char* snapshot_wait = "barch-snapshot-wait";
        constexpr const char* snapshot_done = "barch-snapshot-done";

        int machine::read_logical_snp_obj(nuraft::snapshot& s, void*&, nuraft::ulong obj_id,
                                          nuraft::ptr<nuraft::buffer>& out, bool& is_last) {
            // its member will need the log after it once the copy's done - TODO 636
            if (auto* r = g.live.load())
                r->hold_for_snapshot(s.get_last_log_idx());
            std::string what;
            const std::string where = " " + g.rt.my_rpc() + " " + std::to_string(s.get_last_log_idx());
            if (obj_id == 0) {
                what = snapshot_tag + where;
                is_last = false;
            } else if (obj_id == 1) {
                what = snapshot_wait + where;
                is_last = false;
            } else {
                what = snapshot_done;
                is_last = true;
            }
            out = nuraft::buffer::alloc(what.size());
            std::memcpy(out->data_begin(), what.data(), what.size());
            return 0;
        }

        void machine::save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id, nuraft::buffer& data,
                                           bool, bool) {
            (void) s;
            const std::string body((const char*) data.data_begin(), data.size());
            std::istringstream in(body);
            std::string tag, from;
            uint64_t at = 0;
            const bool where = (bool) (in >> tag >> from >> at);
            if (obj_id == 0) {
                if (!where || (tag != snapshot_tag && tag != snapshot_tag_v1)) {
                    // leave it unread: apply_snapshot then refuses, and nothing is copied
                    barch::err({"raft group", (uint64_t) g.number, "got a snapshot this build can't read"});
                    ++obj_id;
                    return;
                }
                g.received_ok = false;
                const bool done = copy_step(from, at);
                if (tag == snapshot_tag_v1) {
                    // two objects: the next is the last, so the copy has to be done first
                    if (done) ++obj_id;
                } else if (!g.stopping) {
                    obj_id = done ? 2 : 1;      // past the first: the leader keeps to it
                }
                return;
            }
            if (obj_id == 1 && where && tag == snapshot_wait) {
                if (copy_step(from, at)) obj_id = 2;
                return;                         // otherwise asked for again
            }
            // the end: the copy was made before this came
            if (body != snapshot_done)
                barch::err({"raft group", (uint64_t) g.number, "got a snapshot object this build can't read"});
            ++obj_id;
        }

        /*
         * The copy runs on the group's copy thread (TODO 628), and NuRaft sends an
         * object again until it's done: each time, this starts the copy, finds it
         * still going, or finds it finished. true once a copy of this snapshot has
         * worked, and then the member may move on to the last object.
         */
        bool machine::copy_step(const std::string& from, uint64_t at) {
            const std::string key = from + " " + std::to_string(at);
            std::unique_lock l(g.copy_lock);
            if (g.stopping) {
                l.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return false;                   // the group is going: no new copy
            }
            if (g.copying == group_rt::copy_state::running && g.copy_of == key) {
                l.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return false;
            }
            if (g.copying == group_rt::copy_state::done && g.copy_of == key) {
                if (g.copy_thread.joinable())
                    g.copy_thread.join();       // it has finished; this only reaps it
                g.copying = group_rt::copy_state::none;
                g.received_ok = true;
                return true;
            }
            if (g.copying == group_rt::copy_state::running) {
                // a copy of another snapshot: let it finish before starting this one
                l.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return false;
            }
            const bool again = g.copying == group_rt::copy_state::failed && g.copy_of == key;
            if (g.copy_thread.joinable())
                g.copy_thread.join();
            g.copy_of = key;
            g.copying = group_rt::copy_state::running;
            barch::log({"raft group", (uint64_t) g.number, again ? "copying a snapshot again at" : "copying a snapshot at",
                        (uint64_t) at, "from", from});
            group_rt* gp = &g;
            g.copy_thread = std::thread([gp, from] {
                // test knob, TODO 627: the first copy waits long enough for a test
                // to stop its source; the ones after don't. With ..._DELAY_GROUP, the
                // first copy of that group, whichever comes first - TODO 636
                static std::atomic<long> copy_delay_ms{[] {
                    const char* v = std::getenv("BARCH_TEST_SNAPSHOT_COPY_DELAY_MS");
                    return v ? std::atol(v) : 0L;
                }()};
                static const long delay_group = [] {
                    const char* v = std::getenv("BARCH_TEST_SNAPSHOT_COPY_DELAY_GROUP");
                    return v ? std::atol(v) : -1L;
                }();
                if (delay_group < 0 || delay_group == (long) gp->number)
                    if (const long d = copy_delay_ms.exchange(0); d > 0)
                        std::this_thread::sleep_for(std::chrono::milliseconds(d));
                std::string err;
                const bool ok = gp->rt.copy_from(from, gp->spaces, err, gp->from.load(), gp->to.load());
                if (!ok)
                    barch::err({"raft group", (uint64_t) gp->number, "couldn't copy the snapshot -", err,
                                "- asking for it again"});
                std::lock_guard cl(gp->copy_lock);
                if (gp->copying == group_rt::copy_state::running)
                    gp->copying = ok ? group_rt::copy_state::done : group_rt::copy_state::failed;
            });
            l.unlock();
            // a failed copy isn't started again straight away, so a source that's up
            // but can't be copied from isn't asked in a tight loop
            std::this_thread::sleep_for(std::chrono::milliseconds(again ? 200 : 50));
            return false;
        }

        bool machine::apply_snapshot(nuraft::snapshot& s) {
            // the copy was made, and worked, before the last object came (see
            // read_logical_snp_obj); false here stops the process, so it's only for a
            // snapshot this build couldn't read at all
            if (!g.received_ok.exchange(false)) {
                barch::err({"raft group", (uint64_t) g.number, "was asked to apply a snapshot it never copied"});
                return false;
            }
            if (auto* r = g.live.load())
                r->save_snapshot(s);
            g.applied = s.get_last_log_idx();
            ++g.snapshots_installed;
            return true;
        }

        void group_rt::snapshot_async(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& done) {
            auto copy = nuraft::snapshot::deserialize(*s.serialize());
            std::lock_guard l(snap_lock);
            if (stopping) {
                bool ok = false;
                nuraft::ptr<std::exception> none;
                done(ok, none);                 // the group is going: no new save
                return;
            }
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
            stopping = false;
            incarnation = random_u64();
            applied = 0;
            leader_from = UINT64_MAX;
            needs_rebuild = false;
            rebuild_noted = false;
            sm = nuraft::cs_new<machine>(*this);
            raft_group::options o;
            o.group = number;
            o.dir = std::filesystem::path(barch::data_path("cluster")).string();
            o.host = barch::get_external_host();
            o.base_port = (int) barch::get_raft_port();
            // read at each start: a change takes effect when the group next starts - TODO 620
            o.secret = barch::get_cluster_secret();
            o.tls = barch::get_raft_tls();
            o.tls_cert = barch::get_tls_pem_certificate_chain_file();
            o.tls_key = barch::get_tls_private_key_file();
            o.tls_ca = barch::get_raft_tls_ca_file();
            o.server_id = server_id;
            o.join = join;
            for (const auto id : initial_members) {
                const auto ep = rt.raft_endpoint(id, number);
                if (!ep.empty()) o.initial_members.emplace_back(id, ep);
            }
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
            stopping = true;
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
            {
                // and so does one being copied in; joined outside the lock, which
                // the copy takes to say how it went
                std::thread copier;
                {
                    std::lock_guard cl(copy_lock);
                    copier = std::move(copy_thread);
                    copying = copy_state::none;
                    copy_of.clear();
                }
                if (copier.joinable())
                    copier.join();
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
            if (raft->is_leader()) {
                if (applied.load() < leader_from.load())
                    return "TRYAGAIN the new leader is still applying what came before it";
                // TODO 613: it may have been deposed without knowing it yet
                return "TRYAGAIN no quorum has confirmed this leader within its lease";
            }
            const int32_t l = raft->leader_id();
            const std::string where = l > 0 ? rt.address_of(l) : std::string{};
            return where.empty() ? std::string("NOTLEADER no leader is known yet")
                                 : "NOTLEADER " + where + " " + std::to_string(raft->term());
        }

        binding::result group_rt::commit(const std::string& record, size_t shard, std::string& why,
                                         bool writer_applied) {
            if (!ready()) {
                why = not_leader();
                return binding::result::refused;
            }
            // from the fence check until the entry is committed, so no write for a
            // shard being handed off lands in the log after the hand-off - TODO 616
            std::shared_lock appending(append_lock);
            if (shard >= fence_from.load()) {
                why = "TRYAGAIN this shard is moving to another raft group";
                return binding::result::refused;
            }
            // test knob, TODO 617: hold writes between the fence check and the log
            // long enough for a hand-off to land among them
            static const long delay_ms = [] {
                const char* v = std::getenv("BARCH_TEST_COMMIT_DELAY_MS");
                return v ? std::atol(v) : 0L;
            }();
            if (delay_ms > 0 && number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            auto buf = nuraft::buffer::alloc(9 + record.size());
            auto* p = buf->data_begin();
            p[0] = writer_applied ? entry_version : entry_version_unapplied;
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
                    // for the session whose command this is - TODO 613, by group - TODO 615
                    note_committed(label, idx);
                    if (!split())
                        for (const auto& sp : spaces)
                            if (sp != label) note_committed(sp, idx);
                    return binding::result::committed;
                case raft_group::outcome::refused:
                    why = raft->is_leader() ? "ERR the write was refused: " + w : not_leader();
                    return binding::result::refused;
                case raft_group::outcome::unknown:
                    // it may commit yet. A copy that already has it rebuilds from the
                    // group rather than guess; one whose writer took it back out
                    // (TODO 632) gets it from the log, if it commits, like everyone
                    if (writer_applied)
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
                detach(g);
                g->stop();
            }
            running = false;
        }

        std::shared_ptr<group_rt> runtime_impl::open_group(uint32_t number, const std::vector<std::string>& spaces,
                                                           const std::string& label, size_t from, size_t to) {
            std::lock_guard l(groups_lock);
            auto& g = groups[number];
            if (!g)
                g = std::make_shared<group_rt>(*this, number, spaces, label, from, to);
            return g;
        }

        /*
         * Start a group and bind its spaces to it. They're bound before the group
         * applies anything, so a space the first entry opens takes its binding as
         * it's built; and attached to the ones already open.
         */
        bool runtime_impl::bind_group(const std::shared_ptr<group_rt>& g, std::string& err) {
            auto b = std::make_shared<space_binding>(g);
            g->binding = b;
            // a space opened here before the cluster decided its shards can disagree
            // with every other member, and only recreating it here fixes that - TODO 631
            for (const auto& s : g->spaces) {
                if (s == cluster_space || s == config_space) continue;
                std::vector<std::string> v;
                std::string e;
                if (!local(config_space, {"GET", s + ".shards"}, &v, e) || v.empty() || v[0].empty()) continue;
                auto ks = barch::get_keyspace(s);
                if (!ks || std::to_string(ks->get_shard_count()) == v[0]) continue;
                // binding is tried every tick until it works, so it's said once a space
                static std::mutex said_lock;
                static std::set<std::string> said;
                std::lock_guard sl(said_lock);
                if (said.insert(s).second)
                    barch::err({"cluster: space", s, "has", (uint64_t) ks->get_shard_count(),
                                "shards here and", v[0], "in the cluster - its copy here won't match;"
                                " remove it here and let it be copied again"});
            }
            for (const auto& s : g->spaces)
                barch::cluster::bind(s, b, g->from.load(), g->to.load());
            for (const auto& s : g->spaces) {
                if (auto ks = barch::get_keyspace(s); ks && !ks->attach_raft(b, g->from.load(), g->to.load(), err)) {
                    detach(g);
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
                detach(g);
                return false;
            }
            return true;
        }

        void runtime_impl::detach(const std::shared_ptr<group_rt>& g) {
            for (const auto& s : g->spaces) {
                unbind(s, g->binding);
                std::string ignored;
                if (auto ks = barch::get_keyspace(s))
                    ks->attach_raft(nullptr, g->from.load(), g->to.load(), ignored);
            }
        }

        bool runtime_impl::copy_from(const std::string& rpc_address, const std::vector<std::string>& spaces,
                                     std::string& err, size_t shard_from, size_t shard_to) {
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
            // a group of a split space copies its own shards and no others: the
            // other groups' shards here follow their own logs - TODO 615
            const bool whole = shard_from == 0 && shard_to == SIZE_MAX;
            for (const auto& s : spaces) {
                // RETRIEVE: every shard of the space from the other node, under its
                // CoW freeze, and in here only once all of them arrived. Pages only
                // load where they were written the same way; otherwise key by key
                const bool ok = same && whole ? local(s, {"RETRIEVE", h, std::to_string(p)}, nullptr, err)
                                              : logical_copy(rpc_address, s, err, shard_from, shard_to);
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
        bool runtime_impl::logical_copy(const std::string& from, const std::string& space, std::string& err,
                                        size_t shard_from, size_t shard_to) {
            auto ks = barch::get_keyspace(space);
            barch::sharded_store store(ks);
            const size_t shards = ks->get_shard_count();
            const size_t end = std::min(shards, shard_to);
            if (shard_from == 0 && end == shards) {
                repl::applying applying;
                store.clear_space();
            } else {
                for (size_t shard = shard_from; shard < end; ++shard)
                    ks->get(shard)->clear();
            }
            uint64_t records = 0;
            for (size_t shard = shard_from; shard < end; ++shard) {
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
            if (barch::get_cluster_secret().empty()) {
                // every Raft message is signed with it - TODO 620
                err = "ERR set cluster_secret first, the same on every node";
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
            if (args.size() != 7) {
                err = "ERR CLUSTER ADMIT num id rpc raft layout proof";
                return false;
            }
            // the joiner has to hold the secret: once admitted, every entry of every
            // group goes to it - TODO 620
            const auto secret = barch::get_cluster_secret();
            if (secret.empty() || !same_signature(args[6], sign_words(secret, "admit",
                                                                      {args[1], args[2], args[3], args[4], args[5]}))) {
                barch::err({"cluster: refused to admit node", args[1], "at", args[3],
                            "- it didn't sign with this cluster's secret"});
                err = "ERR the joining node's cluster_secret doesn't match this cluster's";
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
                                       "layout", args[5]},
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
            if (barch::get_cluster_secret().empty()) {
                // every Raft message is signed with it - TODO 620
                err = "ERR set cluster_secret first, the same on every node";
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
            const auto proof = sign_words(barch::get_cluster_secret(), "admit",
                                          {num, node_id, my_rpc(), my_raft(), layout});
            if (!remote(leader, {"CLUSTER", "ADMIT", num, node_id, my_rpc(), my_raft(), layout, proof}, nullptr, err,
                        30000)) {
                // it never became a member, so it's as it was: no cluster
                detach(g);
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
                    const std::vector<std::string> names = g->split() ? std::vector<std::string>{g->label} : g->spaces;
                    for (const auto& s : names) {
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
                top = std::max(top, s.group);       // read_spaces lists every group of a split space
            }
            for (const auto& name : wanted) {
                if (claimed.count(name) || name == cluster_space || name == config_space || name.empty())
                    continue;
                std::string err;
                const auto me = my_num();
                // split over this many groups, each owning a run of its shards - TODO 615
                uint32_t of = 1;
                {
                    std::vector<std::string> v;
                    std::string e;
                    if (local(config_space, {"GET", name + ".raft_groups"}, &v, e) && !v.empty() && !v[0].empty()) {
                        try {
                            of = (uint32_t) std::stoul(v[0]);
                        } catch (...) {
                            of = 1;
                        }
                    }
                    if (of < 1) of = 1;
                    if (of > 16) of = 16;
                }
                /*
                 * Its shard count, decided here once for every member - TODO 631. A
                 * replicated write holds its shard until it commits, so the count is
                 * how many writes can be committing at once. `<space>.shards` if it's
                 * set; else what the space already has, if it exists here, since a
                 * saved space can't change it; else raft_shards. Written into the
                 * configuration space ahead of the record below, so a member opens
                 * the space with it.
                 */
                std::string shards;
                {
                    std::vector<std::string> v;
                    std::string e;
                    const bool given = local(config_space, {"GET", name + ".shards"}, &v, e) && !v.empty()
                                       && !v[0].empty();
                    if (given) {
                        shards = v[0];
                    } else {
                        size_t n = barch::get_raft_shards();
                        if (barch::keyspace_exists(name))
                            if (auto ks = barch::get_keyspace(name)) n = ks->get_shard_count();
                        shards = std::to_string(n);
                        if (!local(config_space, {"SET", name + ".shards", shards}, nullptr, e)) {
                            barch::err({"cluster: could not record the shards of space", name, "-", e});
                            continue;
                        }
                        barch::log({"cluster: space", name, "has", (uint64_t) n, "shards"});
                    }
                }
                // the count goes in the record too, so a member can tell when its
                // configuration has caught up with it (see open_spaces)
                if (local(cluster_space, {"HSET", "space:" + name, "group", std::to_string(top + 1), "mode", "raft",
                                          "creator", std::to_string(me), "groups", std::to_string(of),
                                          "shards", shards},
                          nullptr, err)) {
                    claimed.insert(name);
                    barch::log({"cluster: space", name, "is replicated in raft group", (uint64_t) (top + 1),
                                of > 1 ? "and the next" : "", of > 1 ? (uint64_t) (of - 1) : (uint64_t) 0});
                    top += of;
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
                /*
                 * The record's shard count goes to whoever opens the space here, before
                 * anything does - TODO 631. A node coming back can have the record but
                 * its configuration (`<space>.shards`) only once the cluster group's
                 * snapshot or log reaches it. A space opened in between got the default,
                 * 17 shards to the cluster's 128, and every copy from the leader was
                 * refused.
                 */
                if (s.shards > 0)
                    barch::cluster::set_space_shards(s.name, s.shards);
                std::shared_ptr<group_rt> g;
                {
                    std::lock_guard l(groups_lock);
                    auto i = groups.find(s.group);
                    if (i != groups.end() && i->second->running()) continue;
                }
                size_t from = 0, to = SIZE_MAX;
                if (s.given_run) {
                    from = s.run_from;
                    to = s.run_to;
                } else if (s.of > 1) {
                    auto ks = barch::get_keyspace(s.name);
                    std::tie(from, to) = shard_run(ks ? ks->get_shard_count() : 0, s.k, s.of);
                }
                // a group's own file knows its run better than the record, which the
                // cluster leader writes only after a split has gone through - TODO 616
                stored_range(s.group, from, to);
                const bool have_log = std::filesystem::exists(group_file(s.group));
                /*
                 * A group a split made has no creator: its members start it as they
                 * apply the hand-off (process_handoffs), and only a node that wasn't
                 * in the old group joins it from here - TODO 616. Starting it alone
                 * here made a group of one that elected itself beside the real one.
                 * And a node where another group still owns its shards hasn't applied
                 * the hand-off yet, so it waits: joining now, its old group could
                 * apply older writes to those shards over the copy.
                 */
                const bool split_born = s.given_run && s.group != s.first;
                if (split_born && !have_log) {
                    bool waiting = false;
                    {
                        // applied here and queued: process_handoffs starts it
                        std::lock_guard q(handoffs_lock);
                        for (const auto& ho : handoffs)
                            if (ho.h == s.group) waiting = true;
                    }
                    std::lock_guard l(groups_lock);
                    for (const auto& [n, x] : groups) {
                        if (n != s.group && x->spaces.front() == s.name && x->running()
                            && x->from.load() <= s.run_from && s.run_from < x->to.load())
                            waiting = true;
                    }
                    if (waiting)
                        continue;
                }
                g = open_group(s.group, {s.name}, s.label(), from, to);
                std::string err;
                // bound first: from here the space refuses clients until its group
                // runs here, instead of answering from a copy nothing replicates to
                if (!bind_group(g, err)) {
                    barch::err({"cluster: space", s.name, "can't be bound to group", (uint64_t) s.group, "-", err});
                    continue;
                }
                const bool creator = s.creator == me && !split_born;
                if (!have_log && !creator) {
                    // from whoever leads it now, as the heartbeats say; the creator
                    // may not, or may be gone
                    std::string from = address_of(s.creator);
                    for (const auto& k : keys_like("node:*:space:" + s.label())) {
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
                    if (!copy_from(from, {s.name}, err, g->from.load(), g->to.load())) {
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
             * A member joins as a learner and is made a voter once it's caught up -
             * TODO 611. Until then it doesn't count towards a quorum, so a node
             * copying a large space can't slow the group's commits down or, by
             * failing, cost it its majority.
             *
             * Caught up means within a few entries of the leader, or at the index
             * the leader had on the previous tick. Under steady writes a member
             * that keeps up is still a batch behind at any moment, ~30 entries
             * since TODO 632, so the first test alone never passed - TODO 635.
             */
            const uint64_t top = r->last_index();
            const uint64_t now = now_ms();
            for (const auto& m : r->members()) {
                if (!m.learner) continue;
                const uint64_t at = r->member_index(m.id);
                auto mark = g->learner_marks.find(m.id);
                const bool kept_up = mark != g->learner_marks.end() && now - mark->second.second <= 1000
                                     && at >= mark->second.first;
                g->learner_marks[m.id] = {top, now};
                if (at + 8 < top && !kept_up) continue;
                g->learner_marks.erase(m.id);
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
             * group changing leaders. Only while it's answering, too (TODO 628): its
             * log index is what this leader last heard, so one that has since died
             * looks caught up, and handing it the group leaves the group leaderless.
             */
            std::vector<int32_t> voters;
            for (const auto& m : r->members())
                if (!m.learner) voters.push_back(m.id);
            std::sort(voters.begin(), voters.end());
            if (voters.size() < 2) return;
            const int32_t preferred = voters[(g->number - 1) % voters.size()];
            if (preferred == r->my_id()) return;
            if (r->member_index(preferred) + 8 < r->last_index()) return;
            if (now - g->last_handover < 10000) return;
            if (!r->answering(preferred)) return;
            g->last_handover = now;
            if (r->hand_over(preferred))
                barch::log({"cluster: group", (uint64_t) g->number, "hands its leadership to node", (int64_t) preferred});
        }

        /*
         * A write whose outcome was unknown, or an entry that couldn't be applied,
         * leaves this copy unlike the others. The group stops here, the spaces
         * are copied again from its leader, and it starts again as a new
         * incarnation, so the whole log is applied over the copy.
         */
        void runtime_impl::rebuild(const std::shared_ptr<group_rt>& g) {
            // this runs every tick until the rebuild is done, so it says so once
            const bool first = !g->rebuild_noted.exchange(true);
            if (g->number == 0 && first)
                barch::warn({"cluster: the cluster group needs a rebuild"});
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
                if (r->members().size() <= 1) {
                    g->needs_rebuild = false;
                    g->rebuild_noted = false;
                } else if (r->another_voter_answering()) {
                    r->yield_leadership();
                } else if (first) {
                    // yielding to nobody only brings the group back here, every
                    // tick - TODO 628. It waits for another member to answer
                    barch::warn({"cluster: group", (uint64_t) g->number,
                                 "waits for another member to come back before it rebuilds"});
                }
                return;
            }
            if (leader <= 0) return;                    // not until there's a leader to copy from
            std::string err;
            const auto from = address_of(leader);
            barch::warn({"cluster: rebuilding group", (uint64_t) g->number, "from node", (int64_t) leader});
            /*
             * The spaces stay bound to the group through the copy - TODO 621. A
             * stopped group refuses their reads and writes, as a joining node's does.
             * They used to be detached first and bound again after the restart, and
             * in between a client still writing to this node got plain local writes:
             * acknowledged, never in the log, and mostly overwritten by the copy.
             */
            g->stop();
            if (!copy_from(from, g->spaces, err, g->from.load(), g->to.load())) {
                barch::err({"cluster: rebuild of group", (uint64_t) g->number, "failed -", err});
            }
            if (!g->start(me, false, err))
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
            process_handoffs();
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
                else if (g->running()) {
                    if (auto r = g->rg()) r->hold_log();    // TODO 636
                    reconcile(g);
                }
            }
            if (now_ms() - last_heartbeat >= heartbeat_ms) {
                last_heartbeat = now_ms();
                send_heartbeat();
            }
        }

        /*
         * ROUTES - TODO 613. For every replicated space: its group, the RESP address
         * of its leader as this node knows it, and the epoch, the group's term. A
         * client that gets two answers for a space keeps the one with the higher
         * epoch. Every node is in every group, so each knows them all.
         */
        void runtime_impl::queue_handoff(uint32_t from_group, const std::string& space, size_t m, size_t b,
                                         uint32_t h, const std::string& label, const std::vector<int32_t>& voters) {
            std::lock_guard l(handoffs_lock);
            handoffs.push_back({from_group, space, m, b, h, label, voters});
        }

        std::string runtime_impl::raft_endpoint(int32_t num, uint32_t group) {
            std::lock_guard l(nodes_lock);
            for (const auto& n : nodes) {
                std::string h;
                int p = 0;
                if (n.num == num && split_address(n.raft, h, p))
                    return raft_group::endpoint_for(h, p, group);
            }
            return {};
        }

        bool runtime_impl::stored_range(uint32_t group, size_t& from, size_t& to) {
            const auto file = group_file(group);
            if (!std::filesystem::exists(file))
                return false;
            uint64_t f = 0, t = 0;
            {
                group_store peek(file);         // the group isn't open here yet
                if (!peek.range(f, t))
                    return false;
            }
            from = f;
            to = t;
            return true;
        }

        /*
         * The other half of a hand-off, on the cluster's thread - TODO 616. Every
         * member applied it at the same index, so every member's copy of the shards
         * is the same, and the new group starts over them with the old group's
         * voters, no copy needed. The shards go to the new group as it's bound -
         * each shard's binding swapped under its own latch, so a write sees one
         * group or the other - and only then does the old group's entry shrink.
         */
        void runtime_impl::process_handoffs() {
            std::vector<handoff> todo;
            {
                std::lock_guard l(handoffs_lock);
                todo.swap(handoffs);
            }
            std::vector<handoff> again;
            const auto me = my_num();
            for (auto& ho : todo) {
                std::shared_ptr<group_rt> old;
                {
                    std::lock_guard l(groups_lock);
                    auto i = groups.find(ho.from_group);
                    if (i != groups.end()) old = i->second;
                }
                auto ng = open_group(ho.h, {ho.space}, ho.label, ho.m, ho.b);
                if (ng->running())
                    continue;
                ng->initial_members = ho.voters;
                bool known = me != 0;
                for (const auto id : ho.voters)
                    known = known && !raft_endpoint(id, ho.h).empty();
                std::string err;
                if (!known || !start_group(ng, me, false, err)) {
                    barch::warn({"cluster: group", (uint64_t) ho.h, "for", ho.label, "didn't start yet -",
                                 known ? err : std::string("a member's address isn't known")});
                    again.push_back(ho);
                    continue;
                }
                if (auto r = ng->rg())
                    r->save_range(ho.m, ho.b);
                if (old && old->binding) {
                    unbind(ho.space, old->binding);
                    barch::cluster::bind(ho.space, old->binding, old->from.load(), old->to.load());
                }
                barch::log({"cluster: group", (uint64_t) ho.h, ho.label, "took shards", (uint64_t) ho.m, "to",
                            (uint64_t) (ho.b - 1), "of", ho.space});
            }
            if (!again.empty()) {
                std::lock_guard l(handoffs_lock);
                handoffs.insert(handoffs.end(), again.begin(), again.end());
            }
        }

        /*
         * SPLIT <label>, on the cluster leader - TODO 616. The group's run is halved:
         * its leader hands the upper half to a new group, and once that has
         * committed the space's record lists every group's run explicitly.
         */
        bool runtime_impl::split(const std::string& label, std::vector<std::string>& out, std::string& err) {
            std::lock_guard one_at_a_time(admin);
            auto g0 = open_group(0, {cluster_space, config_space});
            if (!g0->ready()) {
                err = g0->not_leader();
                return false;
            }
            const auto all = read_spaces();
            const space_info* target = nullptr;
            uint32_t top = 0;
            for (const auto& s : all) {
                top = std::max(top, s.group);
                if (s.label() == label) target = &s;
            }
            if (!target) {
                err = "ERR no replicated group called " + label;
                return false;
            }
            const std::string space = target->name;
            auto ks = barch::get_keyspace(space);
            const size_t shards = ks ? ks->get_shard_count() : 0;
            auto run_of = [&](const space_info& s) {
                std::pair<size_t, size_t> r{0, shards};
                if (s.given_run) r = {s.run_from, std::min(s.run_to, shards)};
                else if (s.of > 1) r = shard_run(shards, s.k, s.of);
                return r;
            };
            std::shared_ptr<group_rt> g;
            {
                std::lock_guard l(groups_lock);
                auto i = groups.find(target->group);
                if (i != groups.end()) g = i->second;
                for (const auto& [n, x] : groups) top = std::max(top, n);
            }
            if (!g || !g->rg()) {
                err = "ERR group " + label + " isn't running here";
                return false;
            }
            auto [a, b] = run_of(*target);
            b = std::min(b, shards);                // the run as the record has it
            // shorter here than in the record: a hand-off already went through
            const bool applied_here = g->to.load() < b;
            if (!applied_here && b < a + 2) {
                err = "ERR " + label + " owns " + std::to_string(b - a) + " shard(s), too few to split";
                return false;
            }
            size_t m = (a + b) / 2;
            // the next free <space>/<k>
            uint32_t next_k = 0;
            for (const auto& s : all) {
                if (s.name != space) continue;
                const auto l = s.label();
                const auto slash = l.rfind('/');
                uint32_t k = 0;
                if (slash != std::string::npos) {
                    try {
                        k = (uint32_t) std::stoul(l.substr(slash + 1));
                    } catch (...) {}
                }
                next_k = std::max(next_k, k + 1);
            }
            std::string new_label = space + "/" + std::to_string(next_k);
            uint32_t h = top + 1;
            /*
             * A SPLIT run again after an unknown outcome: the hand-off may have
             * committed, and then this node has applied it - the group's run is
             * already short, and the new group is here. Record that, rather than
             * handing off again to another number.
             */
            bool done = false;
            if (applied_here) {
                std::lock_guard l(groups_lock);
                for (const auto& [n, x] : groups) {
                    if (x->spaces.front() == space && x->from.load() == g->to.load()) {
                        m = g->to.load();
                        h = n;
                        new_label = x->label;
                        done = true;
                    }
                }
                if (!done) {
                    // the hand-off is applied here, but its group hasn't started yet
                    err = "TRYAGAIN " + label + " has handed off; its new group isn't running here yet";
                    return false;
                }
            }
            // the group's leader hands off; every node is in every group, so this one
            // knows who that is
            const int32_t leader = g->rg()->leader_id();
            const auto me = my_num();
            if (done) {
                // recorded below
            } else if (leader == me) {
                if (!g->hand_off(m, h, new_label, err))
                    return false;
            } else {
                const auto where = leader > 0 ? address_of(leader) : std::string{};
                if (where.empty()) {
                    err = "TRYAGAIN " + label + " has no leader yet";
                    return false;
                }
                if (!remote(where, {"CLUSTER", "HANDOFF", std::to_string(target->group), std::to_string(m),
                                    std::to_string(h), new_label}, nullptr, err, 30000))
                    return false;
            }
            // every group of the space, its run as it is now
            std::string runs;
            for (const auto& s : all) {
                if (s.name != space) continue;
                auto [f, t] = run_of(s);
                if (s.group == target->group) t = m;
                runs += (runs.empty() ? "" : ";") + std::to_string(s.group) + ":" + s.label() + ":"
                        + std::to_string(f) + "-" + std::to_string(t);
            }
            runs += ";" + std::to_string(h) + ":" + new_label + ":" + std::to_string(m) + "-" + std::to_string(b);
            if (!local(cluster_space, {"HSET", "space:" + space, "runs", runs}, nullptr, err))
                return false;
            barch::log({"cluster: split", label, "- shards", (uint64_t) m, "to", (uint64_t) (b - 1), "go to",
                        new_label, "in group", (uint64_t) h});
            out.push_back("OK " + new_label + " group " + std::to_string(h));
            return true;
        }

        void runtime_impl::routes(std::vector<std::string>& out) {
            std::lock_guard l(groups_lock);
            for (const auto& [n, g] : groups) {
                auto r = g->rg();
                if (!r) continue;
                const int32_t leader = r->leader_id();
                std::string where = leader > 0 ? address_of(leader) : std::string{};
                if (where.empty()) where = "-";
                const std::string run = g->split()
                    ? " shards " + std::to_string(g->from.load()) + "-" + std::to_string(g->to.load() - 1) : std::string{};
                const std::vector<std::string> names = g->split() ? std::vector<std::string>{g->label} : g->spaces;
                for (const auto& sp : names)
                    out.push_back(sp + " group " + std::to_string(n) + " leader " + where + " epoch "
                                  + std::to_string(r->term()) + run);
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
                if (g->split()) line << " shards " << g->from.load() << "-" << (g->to.load() - 1) << " label " << g->label;
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
                    // a data group's space: how many shards it has here - TODO 631
                    if (g->number != 0)
                        if (auto ks = barch::get_keyspace(g->spaces.front()))
                            line << " space_shards " << ks->get_shard_count();
                    // what the log wrote and synced since the group started - TODO 630
                    const auto ls = r->log_syncs();
                    line << " log_appended " << ls.appended << " log_syncs " << ls.entry_syncs
                         << " other_syncs " << ls.other_syncs << " log_holds " << r->log_holds();
                    if (r->refused_messages()) line << " refused " << r->refused_messages();
                    if (g->apply_failures) line << " apply_failures " << g->apply_failures.load();
                    if (g->strays) line << " strays " << g->strays.load();
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
            if (sub == "SPLIT" && args.size() == 2)
                return split(args[1], out, err);
            if (sub == "HANDOFF" && args.size() == 5) {
                std::shared_ptr<group_rt> g;
                uint32_t n = 0, h = 0;
                size_t m = 0;
                try {
                    n = (uint32_t) std::stoul(args[1]);
                    m = std::stoull(args[2]);
                    h = (uint32_t) std::stoul(args[3]);
                } catch (...) {
                    err = "ERR CLUSTER HANDOFF group shard new-group label";
                    return false;
                }
                {
                    std::lock_guard l(groups_lock);
                    auto i = groups.find(n);
                    if (i != groups.end()) g = i->second;
                }
                if (!g) {
                    err = "ERR no group " + args[1] + " here";
                    return false;
                }
                if (!g->hand_off(m, h, args[4], err)) return false;
                out.push_back("OK");
                return true;
            }
            if (sub == "ROUTES" && args.size() == 1) {
                routes(out);
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
            err = "ERR CLUSTER INIT | JOIN host port | REMOVE node-id | INFO | ROUTES | SPLIT label | DIGEST space"
                  " | LAYOUT"
                  " | READS FOLLOWER|LEADER | INDEX space | AFTER space index";
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
