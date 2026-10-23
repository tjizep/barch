//
// The cluster - TODO 610, docs/CLUSTERING.md.
//
// Raft groups, the key spaces bound to them, the `cluster` space that records the
// members, and CLUSTER. Only built with BARCH_CLUSTER; installs itself into
// cluster_hooks when it's linked in.
//
#include "../tx_session.h"
#include "auth_api.h"
#include "cluster_hooks.h"
#include "perm_index.h"
#include "raft_writes.h"
#include "function_api.h"
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
#include <optional>
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
        /*
         * Several records the writer didn't apply, as one entry - TODO 665: a u32
         * count after the incarnation, then each record as a u32 length and the
         * record. Every member applies them all as it commits, the leader too, as
         * for entry_version_unapplied, so the batch is in on every member or on
         * none. A member that can't read one of them applies none and is rebuilt.
         */
        constexpr uint8_t entry_version_batch = 4;
        /*
         * A command for every member to run - TODO 669: laid out as a batch, but the
         * strings are the space's name and then the command's words. Every member
         * runs it as it commits, the leader too, in that space. The leader keeps what
         * its own run answered, by index, for the client waiting on it.
         */
        constexpr uint8_t entry_version_command = 5;
        /*
         * A batch across groups - TODO 666: laid out as a batch, the strings are its id,
         * the groups it's in ("1,2,3"), then every record of the batch, whichever group
         * owns its shard. Each group's log gets the same entry and each member applies
         * the records of its group's shards. The first group to commit it decides it:
         * the rest get it from the writer or, if the writer is gone, from a leader that
         * finds a group without it (a roll forward). A group applies an id once: the
         * same id at another index is a roll forward of one it has, and is skipped.
         */
        constexpr uint8_t entry_version_xbatch = 6;
        /** one string, a batch's id: every group has it, and it can be forgotten */
        constexpr uint8_t entry_version_xclose = 7;

        /*
         * A transaction's steps in a group - TODO 693, laid out as a batch. Prepare: its id,
         * then its records; the group keeps them as intents, one per key, unapplied, and
         * refuses one whose key already has another transaction's intent. Commit: the id;
         * the records go into the space under its tx_gate, and the intents go. Abort: the
         * id; the intents go. Each is answered by what happened when it was applied.
         */
        constexpr uint8_t entry_version_txprepare = 8;
        constexpr uint8_t entry_version_txcommit = 9;
        constexpr uint8_t entry_version_txabort = 10;
        /*
         * Several records of one shard the writer applied before they went in the log -
         * TODO 738: what one command wrote under one write lock. Laid out as a batch.
         * The writer skips it as it commits, by incarnation, as it does entry_version;
         * every other member applies it under one hold of the shard, so a read there
         * sees all of it or none.
         */
        constexpr uint8_t entry_version_batch_applied = 11;
        /** a transaction prepared and not committed this long is aborted by its group's leader */
        constexpr auto tx_stale = std::chrono::seconds(5);
        /** pending this long at its home, a transaction in another's way can be pushed aside - TODO 696 */
        constexpr uint64_t tx_quiet_ms = 1000;

        /** what a transaction's step said when it was applied on the writer */
        struct tx_reply : command_reply {
            explicit tx_reply(std::string e) : e(std::move(e)) {}
            [[nodiscard]] std::string error() const override { return e; }
            std::string e;
        };

        /** "1,2,5" as numbers; the first is the transaction's home group - TODO 694 */
        std::vector<uint32_t> group_list(const std::string& csv) {
            std::vector<uint32_t> out;
            std::istringstream in(csv);
            for (std::string n; std::getline(in, n, ',');) {
                try {
                    out.push_back((uint32_t) std::stoul(n));
                } catch (const std::exception&) {
                }
            }
            return out;
        }

        /*
         * Transactions over several groups, as this node has applied them - TODO 694.
         * Each group applies its own commit, at its own point in its own log, so for a
         * while a node can have some of a transaction's groups committed and the rest
         * not. While one is like that, its space's tx_partial is above zero, and reads
         * of the space wait for it to settle. A group's state is P (prepared), C or A.
         */
        namespace {
            struct tx_spread {
                std::string space;
                std::vector<uint32_t> groups;
                std::map<uint32_t, char> state;
                bool partial{false};
                std::chrono::steady_clock::time_point since{};
            };
            std::mutex spread_lock;
            std::map<std::string, tx_spread> spreads;

            bool spread_partial(const tx_spread& t) {
                bool committed = false, open = false;
                for (const auto n : t.groups) {
                    const auto i = t.state.find(n);
                    const char c = i == t.state.end() ? '?' : i->second;
                    if (c == 'C')
                        committed = true;
                    else if (c != 'A')
                        open = true;
                }
                return committed && open;
            }

            bool spread_done(const tx_spread& t) {
                for (const auto n : t.groups) {
                    const auto i = t.state.find(n);
                    if (i == t.state.end() || (i->second != 'C' && i->second != 'A'))
                        return false;
                }
                return true;
            }

            void count_partial(const std::string& space, int delta) {
                if (delta == 0)
                    return;
                auto ks = barch::get_keyspace(space.empty() ? std::string("0") : space);
                if (!ks)
                    return;
                {
                    std::lock_guard l(ks->tx_settle_lock);
                    ks->tx_partial.fetch_add(delta);
                }
                if (delta < 0)
                    ks->tx_settled.notify_all();
            }

            /** a C goes in holding the space's tx_gate alone, so a read sees the count steady */
            void note_spread(const std::string& id, const std::string& space, const std::vector<uint32_t>& groups,
                             uint32_t group, char state) {
                if (groups.size() < 2)
                    return;
                int delta = 0;
                std::string name;
                {
                    std::lock_guard l(spread_lock);
                    auto& t = spreads[id];
                    if (t.groups.empty()) {
                        t.groups = groups;
                        t.space = space;
                    }
                    t.state[group] = state;
                    const bool now = spread_partial(t);
                    if (now != t.partial) {
                        delta = now ? 1 : -1;
                        t.partial = now;
                        t.since = std::chrono::steady_clock::now();
                    }
                    name = t.space;
                    if (spread_done(t))
                        spreads.erase(id);
                }
                count_partial(name, delta);
            }

            /** a group's state loaded again (a restart or a copy): what it said before goes */
            void forget_spreads_of(uint32_t group) {
                std::vector<std::pair<std::string, int>> deltas;
                {
                    std::lock_guard l(spread_lock);
                    for (auto i = spreads.begin(); i != spreads.end();) {
                        i->second.state.erase(group);
                        const bool now = spread_partial(i->second);
                        if (now != i->second.partial) {
                            deltas.emplace_back(i->second.space, now ? 1 : -1);
                            i->second.partial = now;
                        }
                        if (i->second.state.empty()) {
                            if (i->second.partial)
                                deltas.emplace_back(i->second.space, -1);
                            i = spreads.erase(i);
                        } else {
                            ++i;
                        }
                    }
                }
                for (const auto& [space, d] : deltas)
                    count_partial(space, d);
            }

            /*
             * One that's stayed part committed here for a minute: a group whose state was
             * dropped (closed and pruned, then the other loaded from a snapshot) never
             * answers for it, and the space's reads would wait for ever. Let them go,
             * and say so.
             */
            void expire_spreads() {
                std::vector<std::pair<std::string, std::string>> gone;
                {
                    std::lock_guard l(spread_lock);
                    const auto now = std::chrono::steady_clock::now();
                    for (auto i = spreads.begin(); i != spreads.end();) {
                        if (i->second.partial && now - i->second.since > std::chrono::seconds(60)) {
                            gone.emplace_back(i->first, i->second.space);
                            i = spreads.erase(i);
                        } else {
                            ++i;
                        }
                    }
                }
                for (const auto& [id, space] : gone) {
                    barch::warn({"transaction", id, "in", space, "has been committed in only some of its groups here"
                                 " for a minute; reads of the space go on without waiting for it"});
                    count_partial(space, -1);
                }
            }
        }

        /** how long a batch across groups stays open before its group's leader rolls it forward */
        constexpr auto xbatch_stale = std::chrono::milliseconds(2000);
        /** how long a closed one is remembered: a roll forward only comes while one is open */
        constexpr uint64_t xbatch_keep_ms = 3600 * 1000;

        void put_u32(uint8_t* p, uint32_t v) {
            for (int i = 0; i < 4; ++i) p[i] = (uint8_t) ((v >> (8 * i)) & 0xff);
        }
        uint32_t get_u32(const uint8_t* p) {
            uint32_t v = 0;
            for (int i = 3; i >= 0; --i) v = (v << 8) | p[i];
            return v;
        }

        /*
         * The last command entry a space's files hold, as "<group> <index>" in a meta
         * key - TODO 673. Another group's number means another log, whose indexes say
         * nothing about this one's, and counts as none.
         */
        constexpr const char* ran_meta = "raft.ran";
        /*
         * The marker's name - TODO 690. One key for a space, holding "<group> <index>",
         * was written by every group of a split space from its own log, though it sits
         * in one group's shard. So a split space's group keeps its own, in its first
         * shard.
         */
        std::string ran_name(const barch::key_space_ptr& ks, uint32_t group, bool split, size_t from) {
            if (!split)
                return ran_meta;
            auto name = barch::meta::name_in_shard(ks, std::string(ran_meta) + "/" + std::to_string(group), from);
            return name.empty() ? std::string(ran_meta) + "/" + std::to_string(group) : name;
        }

        uint64_t ran_through(const barch::key_space_ptr& ks, uint32_t group, const std::string& name) {
            std::string have;
            if (!barch::meta::get(ks, name, have))
                return 0;
            const auto space_at = have.find(' ');
            if (space_at == std::string::npos)
                return 0;
            try {
                if (std::stoull(have.substr(0, space_at)) != group)
                    return 0;
                return std::stoull(have.substr(space_at + 1));
            } catch (const std::exception&) {
                return 0;
            }
        }

        /** a batch's or a command's strings: a u32 count after the 9 byte head, then each with its length */
        bool read_strings(const uint8_t* p, size_t n, std::vector<std::string>& out, std::string& why) {
            if (n < 13) {
                why = "an entry too short for its count";
                return false;
            }
            const uint32_t count = get_u32(p + 9);
            size_t at = 13;
            for (uint32_t i = 0; i < count; ++i) {
                if (n - at < 4 || n - at - 4 < get_u32(p + at)) {
                    why = "an entry cut short";
                    return false;
                }
                const uint32_t len = get_u32(p + at);
                out.emplace_back((const char*) p + at + 4, len);
                at += 4 + (size_t) len;
            }
            if (at != n) {
                why = "an entry longer than its count";
                return false;
            }
            return true;
        }

        /*
         * The one shard a writer-applied batch's records all go to, held for writing,
         * with shard_hold saying so - TODO 738. apply_record finds it held and doesn't
         * lock it again, so the records go in under this one hold and a read here
         * sees all of them or none. Records that go to more than one shard, or a
         * function's key (its install rescans the space), are applied one lock each.
         */
        void hold_whole(const std::vector<aof::record>& records, std::optional<storage_release>& whole,
                        bool& holding) {
            if (barch::shard_hold::current().space)
                return;
            std::string name = barch::ks_undecorate(records[0].space);
            if (name.empty()) name = "0";
            const auto ks = barch::get_keyspace(name);
            if (!ks)
                return;
            size_t at = 0;
            for (size_t i = 0; i < records.size(); ++i) {
                const auto& r = records[i];
                if (r.space != records[0].space
                    || (r.type != aof::record_type::set && r.type != aof::record_type::erase)
                    || barch::functions::is_function_key(art::value_type{r.key.data(), (unsigned) r.key.size()}))
                    return;
                barch::key_space::placed how;
                const size_t here = ks->place(r, how);
                if (i > 0 && here != at)
                    return;
                at = here;
            }
            if (at >= ks->get_shards().size())
                return;
            const auto t = ks->get(at);
            {
                // as the records are applied: the commit thread doesn't wait at the
                // gate for a write that's waiting on it
                repl::applying applying;
                whole.emplace(t);
            }
            auto& hold = barch::shard_hold::current();
            hold.space = ks.get();
            hold.shard = t.get();
            hold.all = false;
            holding = true;
        }

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
            /** several records as one entry, which every member applies whole - TODO 665 */
            binding::result commit_batch(const std::vector<std::string>& records,
                                         const std::vector<size_t>& shards, std::string& why);
            /** several records of one shard this writer applied, as one entry - TODO 738 */
            binding::result commit_applied(const std::vector<std::string>& records, size_t shard,
                                           std::string& why);
            /** a command every member runs, and what it answered here - TODO 669 */
            binding::result commit_command(const std::string& space, const std::vector<std::string>& argv,
                                           reply_ptr& reply, std::string& why);
            /** what a command this node wrote answered when it ran here, by log index */
            void keep_reply(uint64_t idx, reply_ptr reply);

            /*
             * Batches across groups, as this group knows them - TODO 666. Kept with each
             * snapshot (captured before its SAVE, so it never says more than the files
             * hold) and sent with one, so a restart or a copy keeps knowing what it has
             * applied. `index` is where it landed in this group's log: the same id at the
             * same index is the log applied again after a restart, and at another index a
             * roll forward of one this group has.
             */
            struct xbatch_info {
                uint64_t index{0};
                bool open{true};
                std::string groups;
                std::vector<std::string> words;     // the entry's strings, while it's open
                std::chrono::steady_clock::time_point seen{};
                uint64_t closed_ms{0};
            };
            mutable std::mutex x_lock;
            std::map<std::string, xbatch_info> xbatches;    // by id, under x_lock
            // a snapshot being received brings the sender's, for apply_snapshot
            std::string received_xstate;                    // under copy_lock
            bool apply_x(uint8_t version, const std::vector<std::string>& words, uint64_t idx, std::string& why);
            [[nodiscard]] std::string encode_xstate() const;
            void load_xstate(const std::string& bytes);
            /** "open", "closed" or "none" */
            [[nodiscard]] std::string x_status(const std::string& id) const;
            bool x_words(const std::string& id, std::vector<std::string>& words) const;
            [[nodiscard]] std::vector<std::string> stale_xbatches() const;
            /** the entry into this group's log; `shards` this group's, for the fence */
            binding::result commit_xbatch(const std::vector<std::string>& words,
                                          const std::vector<size_t>& shards, std::string& why);
            binding::result commit_xclose(const std::string& id, std::string& why);

            /*
             * Transactions in this group - TODO 693. A prepared one holds its records
             * as intents (by space and key, the transaction holding each), unapplied,
             * until it commits or aborts. Kept with the batches above, under "tx:"
             * ids, so a snapshot and a copy carry them.
             */
            struct tx_info {
                uint64_t index{0};              // the prepare's place in the log
                std::string state;              // PENDING, COMMITTED or ABORTED
                std::vector<uint32_t> groups;   // every group it's in, its home first - TODO 694
                std::string space;
                std::vector<std::string> records;   // this group's, while pending
                std::vector<std::string> reads;     // the keys it read, as tx_read_word - TODO 695
                std::chrono::steady_clock::time_point seen{};
                uint64_t closed_ms{0};
            };
            std::map<std::string, tx_info> txs;                 // by id, under x_lock
            std::map<std::string, std::string> intents;         // space\0key -> id, under x_lock
            // a key a pending transaction read: the ones that read it - TODO 695, under x_lock
            std::map<std::string, std::set<std::string>> read_intents;
            /*
             * A range a pending transaction walked: what keeps a write into it out until the
             * transaction is in - TODO 701. By the transaction's id, under x_lock.
             */
            struct range_read {
                std::string space, lo, hi;      // an empty hi has no end
                [[nodiscard]] bool holds(const std::string& sp, const std::string& key) const {
                    return sp == space && key >= lo && (hi.empty() || key < hi);
                }
            };
            std::map<std::string, std::vector<range_read>> range_intents;
            /** under x_lock */
            void drop_read_intents(const std::string& id, const std::vector<std::string>& reads);
            /** the answer for the writer: empty when done */
            bool apply_tx(uint8_t version, const std::vector<std::string>& words, uint64_t idx,
                          std::string& answer, std::string& why);
            /** pending ones whose coordinator has had long enough, with their groups */
            [[nodiscard]] std::vector<std::pair<std::string, std::vector<uint32_t>>> stale_txs() const;
            /** none, PENDING, COMMITTED or ABORTED */
            [[nodiscard]] std::string tx_status(const std::string& id) const;
            /** how long ago this node applied its prepare, in ms; 0 if unknown - TODO 696 */
            [[nodiscard]] uint64_t tx_age(const std::string& id) const;
            /** a pending transaction has a write in shard `m` or after */
            [[nodiscard]] bool pending_tx_from(size_t m) const;
            binding::result commit_tx(int op, const std::vector<std::string>& words, std::string& answer,
                                      std::string& why);
            /**
             * A LOAD or RETRIEVE the leader ran at `idx`, on a member that doesn't lead
             * - TODO 681: wait for the leader to have applied it, then copy the space
             * from there, on the commit thread, and go on from the next entry. A
             * rebuild instead (stop, copy, apply the log again from the last snapshot)
             * applied the records from before the entry again over the copy, and
             * brought back keys the LOAD had taken away.
             */
            bool copy_replaced(const std::string& space, uint64_t idx, std::string& why);
        private:
            binding::result commit_entry(uint8_t version, const std::vector<const std::string*>& records,
                                         const std::vector<size_t>& shards, std::string& why,
                                         uint64_t* index = nullptr);
            std::mutex replies_lock;
            std::map<uint64_t, reply_ptr> replies;          // under replies_lock
        public:
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
            } else if (n >= 9 && p[0] == entry_version_command) {
                uint64_t inc = 0;
                for (int i = 7; i >= 0; --i) inc = (inc << 8) | p[1 + i];
                std::vector<std::string> words;
                std::string why;
                bool ok = read_strings(p, n, words, why);
                if (ok && words.size() < 2) {
                    why = "a command with no space or no name";
                    ok = false;
                }
                if (ok) {
                    const std::string space = words.front();
                    words.erase(words.begin());
                    reply_ptr reply;
                    try {
                        auto ks = barch::keyspace_exists(space) ? barch::get_keyspace(space) : nullptr;
                        if (!ks) {
                            why = "no space " + space;
                            ok = false;
                        } else {
                            /*
                             * Once per entry, whatever the files hold - TODO 673. A run
                             * isn't safe to repeat (a function's increment would count
                             * twice), and a member applies its log again from its last
                             * snapshot over files that may be ahead of it. The space
                             * keeps the last run it holds, with this group's number, and
                             * a save doesn't freeze it partway through one (log_run).
                             */
                            std::string name = words.size() > 2 ? words[2] : std::string();
                            for (auto& c : name) c = (char) std::toupper((unsigned char) c);
                            auto lead = g.rg();
                            const bool copying = barch::raft_writes::replaced_on_leader(name)
                                                 && !(lead && lead->is_leader());
                            /*
                             * Not around a copy of a replaced space - TODO 674. A copy asks
                             * the leader for its keys (EXPORTSTART), which waits out the
                             * leader's runs; in a split space that leader is copying
                             * another group from this node at the same time, and the two
                             * waited on each other. A copy isn't a function's writes, and
                             * one a save catches part way is made again, since its marker
                             * isn't written until it's done.
                             */
                            std::optional<barch::key_space::log_run> run;
                            if (!copying)
                                run.emplace(*ks);
                            repl::applying applying;
                            if (ran_through(ks, g.number, ran_name(ks, g.number, g.split(), g.from.load())) >= idx) {
                                barch::log({"raft group", (uint64_t) g.number, "entry", (uint64_t) idx,
                                            "already ran in the files of", space});
                            } else {
                                // no shard saves on its own until a whole space save
                                ks->note_cross_shard_write();
                                take_diverged();
                                /*
                                 * LOAD and RETRIEVE replace the space from this node's
                                 * files or another server - TODO 681. The member
                                 * leading the group now runs it; any other would end up
                                 * with something else, so it copies the space from the
                                 * leader once the leader has applied this entry, and
                                 * goes on from the next one (copy_replaced).
                                 */
                                // a split space's group acts on its own shards - TODO 690
                                std::optional<run_shards> only_these;
                                if (g.split())
                                    only_these.emplace(g.from.load(), g.to.load());
                                if (copying)
                                    ok = g.copy_replaced(space, idx, why);
                                else
                                    ok = run_command(space, words, idx, reply, why);
                                // this member doesn't hold what the others do - TODO 677
                                if (auto d = take_diverged(); ok && !d.empty()) {
                                    why = d;
                                    ok = false;
                                }
                                // never back: a copy can bring a later one with it
                                const auto marker = ran_name(ks, g.number, g.split(), g.from.load());
                                if (ok && ran_through(ks, g.number, marker) < idx) {
                                    // this member's own write, as the run's are
                                    repl::applying applying;
                                    ok = barch::meta::set(ks, marker, std::to_string(g.number) + " "
                                                          + std::to_string(idx), why);
                                }
                            }
                        }
                    } catch (const std::exception& e) {
                        why = e.what();
                        ok = false;
                    }
                    ++g.applied_here;
                    // the client waiting on it is on this node only if this node wrote it
                    if (ok && inc == g.incarnation.load())
                        g.keep_reply(idx, std::move(reply));
                }
                if (!ok) {
                    ++g.apply_failures;
                    g.needs_rebuild = true;
                    barch::err({"raft group", (uint64_t) g.number, "could not run the command in entry",
                                (uint64_t) idx, "-", why});
                }
            } else if (n >= 9 && (p[0] == entry_version_txprepare || p[0] == entry_version_txcommit
                                  || p[0] == entry_version_txabort)) {
                // a transaction's step - TODO 693
                uint64_t inc = 0;
                for (int i = 7; i >= 0; --i) inc = (inc << 8) | p[1 + i];
                std::vector<std::string> words;
                std::string why, answer;
                bool ok = read_strings(p, n, words, why);
                try {
                    if (ok)
                        ok = g.apply_tx(p[0], words, idx, answer, why);
                } catch (const std::exception& e) {
                    why = e.what();
                    ok = false;
                }
                ++g.applied_here;
                if (ok && inc == g.incarnation.load())
                    g.keep_reply(idx, std::make_shared<tx_reply>(answer));
                if (!ok) {
                    ++g.apply_failures;
                    g.needs_rebuild = true;
                    barch::err({"raft group", (uint64_t) g.number, "could not apply entry",
                                (uint64_t) idx, "-", why});
                }
            } else if (n >= 9 && (p[0] == entry_version_xbatch || p[0] == entry_version_xclose)) {
                // a batch across groups, or its close - TODO 666
                std::vector<std::string> words;
                std::string why;
                bool ok = read_strings(p, n, words, why);
                try {
                    if (ok)
                        ok = g.apply_x(p[0], words, idx, why);
                } catch (const std::exception& e) {
                    why = e.what();
                    ok = false;
                }
                ++g.applied_here;
                if (!ok) {
                    ++g.apply_failures;
                    g.needs_rebuild = true;
                    barch::err({"raft group", (uint64_t) g.number, "could not apply entry",
                                (uint64_t) idx, "-", why});
                }
            } else if (n >= 9 && (p[0] == entry_version || p[0] == entry_version_unapplied
                                  || p[0] == entry_version_batch || p[0] == entry_version_batch_applied)) {
                uint64_t inc = 0;
                for (int i = 7; i >= 0; --i) inc = (inc << 8) | p[1 + i];
                const bool applied_by_writer = p[0] == entry_version || p[0] == entry_version_batch_applied;
                // this leader's own entries were applied as they were written, unless
                // the writer took them back out to commit first - TODO 632, 665
                if (!applied_by_writer || inc != g.incarnation.load()) {
                    // every record read before any is applied, so a batch that can't
                    // be read leaves nothing of itself behind
                    std::vector<aof::record> records;
                    std::string why;
                    bool ok = true;
                    auto read_one = [&](const uint8_t* at, size_t len) {
                        aof::record r;
                        const auto d = aof::decode(at, (uint32_t) len, r);
                        if (d != aof::decoded::ok) {
                            why = std::string(aof::describe(d));
                            ok = false;
                        } else {
                            records.push_back(std::move(r));
                        }
                    };
                    if (p[0] != entry_version_batch && p[0] != entry_version_batch_applied) {
                        read_one(p + 9, n - 9);
                    } else {
                        std::vector<std::string> each;
                        ok = read_strings(p, n, each, why);
                        for (size_t i = 0; ok && i < each.size(); ++i)
                            read_one((const uint8_t*) each[i].data(), each[i].size());
                    }
                    // test knob, TODO 735: the cluster group applying slowly, so other
                    // threads here see a write made of several entries half done for
                    // longer
                    static const long gap_ms = [] {
                        const char* v = std::getenv("BARCH_TEST_CLUSTER_APPLY_GAP_MS");
                        return v ? std::atol(v) : 0L;
                    }();
                    if (gap_ms > 0 && g.number == 0)
                        std::this_thread::sleep_for(std::chrono::milliseconds(gap_ms));
                    // one command's writes to one shard, under one hold of it - TODO 738
                    std::optional<storage_release> whole;
                    struct let_go {
                        bool on = false;
                        ~let_go() { if (on) barch::shard_hold::current() = {}; }
                    } hold_off;
                    if (ok && p[0] == entry_version_batch_applied && records.size() > 1) {
                        try {
                            hold_whole(records, whole, hold_off.on);
                        } catch (const std::exception& e) {
                            // a lock wait that timed out: one lock a record, as before
                            barch::err({"raft group", (uint64_t) g.number, "entry", (uint64_t) idx,
                                        "applied a record at a time -", e.what()});
                        }
                    }
                    for (size_t i = 0; ok && i < records.size(); ++i) {
                        const auto& r = records[i];
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
                            ok = false;
                        }
                    }
                    if (!records.empty())
                        ++g.applied_here;
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
                    if (auto snap = r->last_snapshot()) {
                        g.applied = snap->get_last_log_idx();
                        // and the batches across groups that snapshot knew - TODO 666
                        g.load_xstate(r->xstate());
                    }
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
            result commit_batch(const std::vector<std::string>& records,
                                const std::vector<size_t>& shards, std::string& why) override {
                return g->commit_batch(records, shards, why);
            }
            result commit_applied(const std::vector<std::string>& records, size_t shard,
                                  std::string& why) override {
                return g->commit_applied(records, shard, why);
            }
            // a group led elsewhere runs it through its leader - TODO 690
            result commit_command(const std::string& space, const std::vector<std::string>& argv,
                                  reply_ptr& reply, std::string& why) override;
            [[nodiscard]] uint32_t group_number() const override { return g->number; }
            [[nodiscard]] bool owns_shard(size_t shard) const override {
                return shard >= g->from.load() && shard < g->to.load();
            }
            // a group led elsewhere gets it through its leader - TODO 666
            result commit_xbatch(const std::vector<std::string>& words, const std::vector<size_t>& shards,
                                 std::string& why) override;
            result commit_xclose(const std::string& id, std::string& why) override;
            // a group led elsewhere gets it through its leader - TODO 694
            result commit_tx(int op, const std::vector<std::string>& words, std::string& answer,
                             std::string& why) override;
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
            void quiesce() override { quiescing = true; }
            // shutdown has begun: no heartbeats, and Raft writes refused - TODO 642
            std::atomic<bool> quiescing{false};
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
            bool export_start(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err);
            // the copies EXPORTSTART took, by id - TODO 674
            struct export_copy {
                std::string space;
                size_t from{0};
                std::vector<std::vector<std::vector<std::string>>> shards;     // shard, page, record
                std::chrono::steady_clock::time_point at;
            };
            std::mutex exports_lock;
            std::map<std::string, export_copy> exports;                       // under exports_lock
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
        public:
            /** roll a stale batch across groups forward into the groups without it - TODO 666 */
            void recover_xbatches(const std::shared_ptr<group_rt>& g);
            /** group `n`'s answer for batch `id`: open, closed or none; false when no leader answered */
            bool x_status_of(uint32_t n, const std::string& id, std::string& status, std::string& err);
            bool x_forward(uint32_t n, const std::vector<std::string>& words, std::string& err);
            bool x_close(uint32_t n, const std::string& id, std::string& err);
            /*
             * Transactions over several groups - TODO 694: a step in group `n`, through its
             * leader when that's another node, and what group `n` knows of one.
             */
            bool tx_to(uint32_t n, int op, const std::vector<std::string>& words, std::string& answer,
                       std::string& err);
            bool tx_status_of(uint32_t n, const std::string& id, std::string& status, std::string& err);
            /** its state and age at group `n`'s leader - TODO 696 */
            bool tx_info_of(uint32_t n, const std::string& id, std::string& status, uint64_t& age_ms,
                            std::string& err);
            bool push_tx(uint32_t home, uint32_t at, const std::string& holder, const std::string& mine,
                         std::string& why) override;
            /** a pending transaction whose coordinator has gone: settled the way its home says */
            void recover_txs(const std::shared_ptr<group_rt>& g);
            /** a command entry in group `n`'s log, through its leader; false when it wasn't committed */
            bool x_command(uint32_t n, const std::string& space, const std::vector<std::string>& argv,
                           reply_ptr& reply, std::string& err);
            std::shared_ptr<group_rt> group_numbered(uint32_t n) {
                std::lock_guard l(groups_lock);
                auto i = groups.find(n);
                return i == groups.end() ? nullptr : i->second;
            }
            /** the RESP address of the node leading group `g`, empty when none is known */
            std::string leader_address(const std::shared_ptr<group_rt>& g) {
                auto r = g->rg();
                const int32_t l = r ? r->leader_id() : -1;
                return l > 0 ? address_of(l) : std::string();
            }
        private:
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
            // the nodes whose heartbeat the leader is writing now - TODO 642
            std::mutex heartbeats_lock;
            std::set<std::string> heartbeats_writing;
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
            /*
             * A transaction prepared here with writes in the shards going: its commit would
             * apply only the shards this group still owns, and the new group never had
             * the prepare - TODO 694. Prepares past the fence are refused (they're
             * fenced like writes), so once none is pending, none will be.
             */
            if (pending_tx_from(m)) {
                fence_from = SIZE_MAX;
                err = "TRYAGAIN a transaction holds keys in the shards being handed off";
                return false;
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
                // the batches across groups it holds, after a newline - TODO 666. A
                // receiver from before reads the first line and leaves the rest
                if (auto* r = g.live.load()) {
                    const auto xs = r->xstate();
                    if (!xs.empty())
                        what += "\n" + xs;
                }
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
            std::string body((const char*) data.data_begin(), data.size());
            if (obj_id == 0) {
                // the snapshot's batches across groups, after the first line - TODO 666
                const auto nl = body.find('\n');
                std::lock_guard cl(g.copy_lock);
                g.received_xstate = nl == std::string::npos ? std::string() : body.substr(nl + 1);
                if (nl != std::string::npos)
                    body.resize(nl);
            }
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
                // to stop its source; the ones after don't. With ..._DELAY_GROUP, every
                // copy of that group waits, and no other - TODO 636
                static std::atomic<long> copy_delay_ms{[] {
                    const char* v = std::getenv("BARCH_TEST_SNAPSHOT_COPY_DELAY_MS");
                    return v ? std::atol(v) : 0L;
                }()};
                static const long delay_group = [] {
                    const char* v = std::getenv("BARCH_TEST_SNAPSHOT_COPY_DELAY_GROUP");
                    return v ? std::atol(v) : -1L;
                }();
                const long d = delay_group < 0 ? copy_delay_ms.exchange(0)
                             : delay_group == (long) gp->number ? copy_delay_ms.load() : 0;
                if (d > 0)
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
            std::string xs;
            {
                std::lock_guard cl(g.copy_lock);
                xs.swap(g.received_xstate);
            }
            g.load_xstate(xs);
            if (auto* r = g.live.load())
                r->save_snapshot(s, &xs);
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
            /*
             * Not while one of its spaces is in a transaction - TODO 677. A save or a
             * copy made then holds the space as it was at BEGIN (a page walk in a
             * transaction reads the BEGIN view), not as it is at this index, so a
             * member started from it would skip everything between. Without one, the
             * last snapshot is from before BEGIN: a member that restarts or copies the
             * space meanwhile gets the BEGIN view and applies BEGIN and all after it
             * from the log, mark included. NuRaft asks again later.
             */
            for (const auto& sp : spaces) {
                if (barch::keyspace_exists(sp) && barch::get_keyspace(sp)->in_transaction()) {
                    bool ok = false;
                    nuraft::ptr<std::exception> none;
                    done(ok, none);
                    return;
                }
            }
            // NuRaft asks for one at a time, so the last one has finished by now
            if (snap_thread.joinable())
                snap_thread.join();
            snap_thread = std::thread([this, copy, done]() mutable {
                bool ok = true;
                std::string err;
                // before the SAVE: what it says applied is in the files - TODO 666
                const std::string xs = encode_xstate();
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
                        r->save_snapshot(*copy, &xs);
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
            return commit_entry(writer_applied ? entry_version : entry_version_unapplied, {&record},
                                {shard}, why);
        }

        binding::result group_rt::commit_batch(const std::vector<std::string>& records,
                                               const std::vector<size_t>& shards, std::string& why) {
            std::vector<const std::string*> each;
            each.reserve(records.size());
            for (const auto& r : records)
                each.push_back(&r);
            return commit_entry(entry_version_batch, each, shards, why);
        }

        binding::result group_rt::commit_applied(const std::vector<std::string>& records, size_t shard,
                                                 std::string& why) {
            std::vector<const std::string*> each;
            each.reserve(records.size());
            for (const auto& r : records)
                each.push_back(&r);
            return commit_entry(entry_version_batch_applied, each, {shard}, why);
        }

        binding::result group_rt::commit_command(const std::string& space, const std::vector<std::string>& argv,
                                                 reply_ptr& reply, std::string& why) {
            std::vector<const std::string*> words{&space};
            for (const auto& a : argv)
                words.push_back(&a);
            uint64_t idx = 0;
            const auto r = commit_entry(entry_version_command, words, {}, why, &idx);
            if (r != binding::result::committed)
                return r;
            std::lock_guard l(replies_lock);
            const auto i = replies.find(idx);
            if (i == replies.end()) {
                // it committed, but this node didn't run it as its writer: it lost the
                // lead and came back between the append and the commit
                why = "UNKNOWN the command committed at " + std::to_string(idx)
                      + " but its answer here was lost - it ran on every member";
                return binding::result::unknown;
            }
            reply = std::move(i->second);
            replies.erase(i);
            return r;
        }

        bool group_rt::copy_replaced(const std::string& space, uint64_t idx, std::string& why) {
            const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(60);
            for (;;) {
                if (stopping) {
                    why = "the group is stopping";
                    return false;
                }
                auto r = rg();
                const int32_t leader = r ? r->leader_id() : -1;
                const std::string source = leader > 0 ? rt.address_of(leader) : std::string();
                if (!source.empty()) {
                    // its own copy, at this entry or later: later entries are records
                    // (the same again) and runs (the copied marker skips them)
                    // this group's applied index there: in a split space DIGEST's is the
                    // first group's - TODO 690
                    std::vector<std::string> said;
                    std::string e;
                    uint64_t applied_there = 0;
                    if (remote(source, {"CLUSTER", "APPLIED", std::to_string(number)}, &said, e, 5000)
                        && !said.empty()) {
                        try {
                            applied_there = std::stoull(said[0]);
                        } catch (const std::exception&) {
                            applied_there = 0;
                        }
                    }
                    if (applied_there >= idx) {
                        if (rt.copy_from(source, {space}, why, from.load(), to.load()))
                            return true;
                        barch::err({"raft group", (uint64_t) number, "couldn't copy", space, "from", source,
                                    "after entry", (uint64_t) idx, "-", why});
                    }
                }
                if (std::chrono::steady_clock::now() >= until) {
                    if (why.empty())
                        why = "no leader had applied entry " + std::to_string(idx) + " to copy " + space + " from";
                    return false;           // rebuilt from the others, as for any entry not applied
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        // ---- batches across groups - TODO 666 -------------------------------------

        bool group_rt::apply_x(uint8_t version, const std::vector<std::string>& words, uint64_t idx,
                               std::string& why) {
            if (words.empty()) {
                why = "a batch entry with no id";
                return false;
            }
            const std::string& id = words[0];
            if (version == entry_version_xclose) {
                std::lock_guard l(x_lock);
                auto i = xbatches.find(id);
                if (i != xbatches.end() && i->second.open) {
                    i->second.open = false;
                    std::vector<std::string>().swap(i->second.words);
                    i->second.closed_ms = now_ms();
                }
                // closed ones are only asked about while another group has it open
                const uint64_t now = now_ms();
                for (auto j = xbatches.begin(); j != xbatches.end();) {
                    if (!j->second.open && now - j->second.closed_ms > xbatch_keep_ms)
                        j = xbatches.erase(j);
                    else
                        ++j;
                }
                return true;
            }
            if (words.size() < 2) {
                why = "a batch entry with no groups";
                return false;
            }
            {
                std::lock_guard l(x_lock);
                auto i = xbatches.find(id);
                if (i != xbatches.end() && i->second.index != idx)
                    return true;            // a roll forward of one this group has: once only
            }
            // every record read before any is applied, as for a batch
            std::vector<aof::record> records;
            records.reserve(words.size() - 2);
            for (size_t k = 2; k < words.size(); ++k) {
                aof::record r;
                const auto d = aof::decode((const uint8_t*) words[k].data(), (uint32_t) words[k].size(), r);
                if (d != aof::decoded::ok) {
                    why = std::string(aof::describe(d));
                    return false;
                }
                records.push_back(std::move(r));
            }
            const size_t lo = from.load(), hi = to.load();
            for (const auto& r : records) {
                // the other groups' records are theirs to apply
                if ((r.type == aof::record_type::set || r.type == aof::record_type::erase)
                    && (r.shard < lo || r.shard >= hi))
                    continue;
                repl::applying applying;
                if (!repl::apply_one(r, why))
                    return false;
            }
            std::lock_guard l(x_lock);
            auto& x = xbatches[id];
            if (x.index == 0) {
                x.index = idx;
                x.groups = words[1];
                x.words = words;
                x.open = true;
                x.seen = std::chrono::steady_clock::now();
            }
            return true;
        }

        namespace {
            void put_str(std::string& out, const std::string& v) {
                uint8_t b[4];
                put_u32(b, (uint32_t) v.size());
                out.append((const char*) b, 4);
                out.append(v);
            }
            bool get_str(const std::string& in, size_t& at, std::string& v) {
                if (in.size() - at < 4)
                    return false;
                const uint32_t n = get_u32((const uint8_t*) in.data() + at);
                at += 4;
                if (in.size() - at < n)
                    return false;
                v.assign(in, at, n);
                at += n;
                return true;
            }
        }

        // id, index, open, groups, closed_ms, then the entry's strings while open
        std::string group_rt::encode_xstate() const {
            std::lock_guard l(x_lock);
            std::string out;
            for (const auto& [id, x] : xbatches) {
                put_str(out, id);
                put_str(out, std::to_string(x.index));
                put_str(out, x.open ? "1" : "0");
                put_str(out, x.groups);
                put_str(out, std::to_string(x.closed_ms));
                put_str(out, std::to_string(x.open ? x.words.size() : 0));
                if (x.open)
                    for (const auto& w : x.words)
                        put_str(out, w);
            }
            // transactions, in the same layout: "tx:" id, its state in the groups field - TODO 693
            for (const auto& [id, t] : txs) {
                const bool pending = t.state == "PENDING";
                put_str(out, "tx:" + id);
                put_str(out, std::to_string(t.index));
                put_str(out, pending ? "1" : "0");
                // the state, its groups and its space - TODO 694
                std::string gs;
                for (const auto n : t.groups)
                    gs += (gs.empty() ? "" : ",") + std::to_string(n);
                // and how many of its words are records, the rest being its reads - TODO 695
                put_str(out, t.state + "|" + gs + "|" + std::to_string(t.records.size()) + "|" + t.space);
                put_str(out, std::to_string(t.closed_ms));
                put_str(out, std::to_string(pending ? t.records.size() + t.reads.size() : 0));
                if (pending) {
                    for (const auto& w : t.records)
                        put_str(out, w);
                    for (const auto& w : t.reads)
                        put_str(out, w);
                }
            }
            return out;
        }

        void group_rt::load_xstate(const std::string& bytes) {
            std::map<std::string, xbatch_info> got;
            size_t at = 0;
            try {
                while (at < bytes.size()) {
                    std::string id, index, open, groups, closed, count;
                    if (!get_str(bytes, at, id) || !get_str(bytes, at, index) || !get_str(bytes, at, open)
                        || !get_str(bytes, at, groups) || !get_str(bytes, at, closed) || !get_str(bytes, at, count))
                        throw std::runtime_error("cut short");
                    xbatch_info x;
                    x.index = std::stoull(index);
                    x.open = open == "1";
                    x.groups = groups;
                    x.closed_ms = std::stoull(closed);
                    x.seen = std::chrono::steady_clock::now();
                    for (uint64_t k = std::stoull(count); k > 0; --k) {
                        std::string w;
                        if (!get_str(bytes, at, w))
                            throw std::runtime_error("cut short");
                        x.words.push_back(std::move(w));
                    }
                    got[id] = std::move(x);
                }
            } catch (const std::exception& e) {
                barch::err({"raft group", (uint64_t) number, "couldn't read its batches across groups -", e.what()});
            }
            // the transactions among them, and their intents again - TODO 693
            std::map<std::string, tx_info> tx_got;
            std::map<std::string, std::string> held;
            std::map<std::string, std::set<std::string>> shared;
            std::map<std::string, std::vector<range_read>> ranged;
            for (auto i = got.begin(); i != got.end();) {
                if (i->first.rfind("tx:", 0) != 0) {
                    ++i;
                    continue;
                }
                tx_info t;
                t.index = i->second.index;
                /*
                 * STATE|groups|records|space since TODO 695, where `records` is how many of the
                 * words are records and the rest are reads; STATE|groups|space from TODO 694,
                 * all records; the state alone from TODO 693
                 */
                std::vector<std::string> parts;
                {
                    const auto& f = i->second.groups;
                    size_t at = 0;
                    while (parts.size() < 3) {
                        const auto bar = f.find('|', at);
                        if (bar == std::string::npos)
                            break;
                        parts.push_back(f.substr(at, bar - at));
                        at = bar + 1;
                    }
                    parts.push_back(f.substr(at));
                }
                size_t nrec = i->second.words.size();
                t.state = parts[0];
                if (parts.size() >= 3)
                    t.groups = group_list(parts[1]);
                if (parts.size() == 4) {
                    try {
                        nrec = std::min<size_t>(std::stoull(parts[2]), nrec);
                    } catch (const std::exception&) {
                    }
                    t.space = parts[3];
                } else if (parts.size() == 3) {
                    t.space = parts[2];
                }
                t.reads.assign(std::make_move_iterator(i->second.words.begin() + (ptrdiff_t) nrec),
                               std::make_move_iterator(i->second.words.end()));
                i->second.words.resize(nrec);
                t.records = std::move(i->second.words);
                t.closed_ms = i->second.closed_ms;
                t.seen = std::chrono::steady_clock::now();
                const std::string id = i->first.substr(3);
                if (t.state == "PENDING") {
                    std::set<std::string> written;
                    for (const auto& w : t.records) {
                        aof::record r;
                        if (aof::decode((const uint8_t*) w.data(), (uint32_t) w.size(), r) == aof::decoded::ok) {
                            held[r.space + std::string(1, '\0') + r.key] = id;
                            written.insert(r.space + std::string(1, '\0') + r.key);
                        }
                    }
                    for (const auto& w : t.reads) {
                        uint32_t shard = 0;
                        uint64_t hash = 0;
                        std::string sp, key;
                        if (!tx_read_from(w, shard, sp, key, hash))
                            continue;
                        if (shard & tx_range_bit) {
                            // a range it walked - TODO 701
                            range_read rr;
                            rr.space = sp;
                            auto& walked = ranged[id];
                            if (tx_range_from(key, rr.lo, rr.hi)
                                && std::none_of(walked.begin(), walked.end(), [&](const range_read& w) {
                                       return w.space == rr.space && w.lo == rr.lo && w.hi == rr.hi;
                                   }))
                                walked.push_back(std::move(rr));
                            continue;
                        }
                        if (!written.count(sp + std::string(1, '\0') + key))
                            shared[sp + std::string(1, '\0') + key].insert(id);
                    }
                }
                tx_got[id] = std::move(t);
                i = got.erase(i);
            }
            // and this node's view of the ones over several groups, from what this group says now
            forget_spreads_of(number);
            for (const auto& [id, t] : tx_got)
                note_spread(id, t.space, t.groups, number,
                            t.state == "COMMITTED" ? 'C' : t.state == "ABORTED" ? 'A' : 'P');
            std::lock_guard l(x_lock);
            xbatches = std::move(got);
            txs = std::move(tx_got);
            intents = std::move(held);
            read_intents = std::move(shared);
            range_intents = std::move(ranged);
        }

        std::string group_rt::x_status(const std::string& id) const {
            std::lock_guard l(x_lock);
            auto i = xbatches.find(id);
            if (i == xbatches.end())
                return "none";
            return i->second.open ? "open" : "closed";
        }

        bool group_rt::x_words(const std::string& id, std::vector<std::string>& words) const {
            std::lock_guard l(x_lock);
            auto i = xbatches.find(id);
            if (i == xbatches.end() || !i->second.open)
                return false;
            words = i->second.words;
            return true;
        }

        std::vector<std::string> group_rt::stale_xbatches() const {
            std::vector<std::string> out;
            const auto now = std::chrono::steady_clock::now();
            std::lock_guard l(x_lock);
            for (const auto& [id, x] : xbatches)
                if (x.open && now - x.seen >= xbatch_stale)
                    out.push_back(id);
            return out;
        }

        std::vector<std::pair<std::string, std::vector<uint32_t>>> group_rt::stale_txs() const {
            std::vector<std::pair<std::string, std::vector<uint32_t>>> out;
            const auto now = std::chrono::steady_clock::now();
            std::lock_guard l(x_lock);
            for (const auto& [id, t] : txs)
                if (t.state == "PENDING" && now - t.seen >= tx_stale)
                    out.emplace_back(id, t.groups);
            return out;
        }

        void group_rt::drop_read_intents(const std::string& id, const std::vector<std::string>& reads) {
            range_intents.erase(id);
            for (const auto& w : reads) {
                uint32_t shard = 0;
                uint64_t hash = 0;
                std::string sp, key;
                if (!tx_read_from(w, shard, sp, key, hash))
                    continue;
                auto i = read_intents.find(sp + std::string(1, '\0') + key);
                if (i == read_intents.end())
                    continue;
                i->second.erase(id);
                if (i->second.empty())
                    read_intents.erase(i);
            }
        }

        bool group_rt::pending_tx_from(size_t m) const {
            std::lock_guard l(x_lock);
            for (const auto& [id, t] : txs) {
                if (t.state != "PENDING")
                    continue;
                for (const auto& w : t.records) {
                    aof::record r;
                    if (aof::decode((const uint8_t*) w.data(), (uint32_t) w.size(), r) == aof::decoded::ok
                        && r.shard >= m)
                        return true;
                }
                // a read is a lock too: the new group would let a write in under it - TODO 695
                for (const auto& w : t.reads) {
                    uint32_t shard = 0;
                    uint64_t hash = 0;
                    std::string sp, key;
                    if (tx_read_from(w, shard, sp, key, hash) && shard >= m)
                        return true;
                }
            }
            return false;
        }

        uint64_t group_rt::tx_age(const std::string& id) const {
            std::lock_guard l(x_lock);
            const auto i = txs.find(id);
            if (i == txs.end())
                return 0;
            return (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - i->second.seen).count();
        }

        std::string group_rt::tx_status(const std::string& id) const {
            std::lock_guard l(x_lock);
            const auto i = txs.find(id);
            return i == txs.end() ? std::string("none") : i->second.state;
        }

        bool group_rt::apply_tx(uint8_t version, const std::vector<std::string>& words, uint64_t idx,
                                std::string& answer, std::string& why) {
            if (words.empty()) {
                why = "a transaction entry with no id";
                return false;
            }
            const std::string& id = words[0];
            const size_t lo = from.load(), hi = to.load();
            // this group's shards only: a split while it was open moves the rest - TODO 694
            auto ours = [&](const aof::record& r) { return r.shard >= lo && r.shard < hi; };
            if (version == entry_version_txprepare) {
                /*
                 * id, its groups (its home first), how many records, this group's records,
                 * then the keys it read in this group's shards - TODO 694, 695
                 */
                size_t nrec = 0;
                try {
                    nrec = words.size() >= 3 ? std::stoull(words[2]) : SIZE_MAX;
                } catch (const std::exception&) {
                    nrec = SIZE_MAX;
                }
                if (words.size() < 3 || nrec > words.size() - 3) {
                    why = "a transaction's prepare that doesn't say what it holds";
                    return false;
                }
                std::vector<aof::record> records;
                for (size_t k = 3; k < 3 + nrec; ++k) {
                    aof::record r;
                    const auto d = aof::decode((const uint8_t*) words[k].data(), (uint32_t) words[k].size(), r);
                    if (d != aof::decoded::ok) {
                        why = std::string(aof::describe(d));
                        return false;
                    }
                    records.push_back(std::move(r));
                }
                struct read_key {
                    std::string lock;           // space\0key, as an intent is keyed
                    std::string space, key;
                    uint64_t hash{0};
                };
                std::vector<read_key> reads;
                // the ranges it walked in this group's shards - TODO 701
                struct range_key {
                    uint32_t shard{0};
                    range_read range;
                    uint64_t hash{0};
                };
                std::vector<range_key> ranges;
                for (size_t k = 3 + nrec; k < words.size(); ++k) {
                    uint32_t shard = 0;
                    read_key rk;
                    if (!tx_read_from(words[k], shard, rk.space, rk.key, rk.hash)) {
                        why = "a transaction's read that doesn't decode";
                        return false;
                    }
                    if (shard & tx_range_bit) {
                        range_key rr;
                        rr.shard = shard & ~tx_range_bit;
                        rr.hash = rk.hash;
                        rr.range.space = rk.space;
                        if (!tx_range_from(rk.key, rr.range.lo, rr.range.hi)) {
                            why = "a transaction's range that doesn't decode";
                            return false;
                        }
                        if (rr.shard >= lo && rr.shard < hi)
                            ranges.push_back(std::move(rr));
                        continue;
                    }
                    if (shard < lo || shard >= hi)
                        continue;
                    rk.lock = rk.space + std::string(1, '\0') + rk.key;
                    reads.push_back(std::move(rk));
                }
                /*
                 * The read check - TODO 695: is each key it read still as it was? Only the
                 * answer: the node that wrote the entry hears it and aborts the transaction
                 * with an entry of its own, and everyone else ignores it. Every member
                 * still takes the same locks below, since that's decided by the log alone;
                 * a key near its expiry can read differently on two members' clocks.
                 */
                std::string changed;
                for (const auto& rk : reads) {
                    const auto name = barch::ks_undecorate(rk.space);
                    if (tx_hash_now(barch::get_keyspace(name.empty() ? std::string("0") : name), rk.key) != rk.hash) {
                        changed = "TXCONFLICT a key the transaction read has changed since it was read";
                        break;
                    }
                }
                // and each range it walked: a key written into it, or out of it, is a phantom
                for (const auto& rr : ranges) {
                    if (!changed.empty())
                        break;
                    const auto name = barch::ks_undecorate(rr.range.space);
                    const auto& r = rr.range;
                    if (tx_range_hash_now(barch::get_keyspace(name.empty() ? std::string("0") : name), rr.shard,
                                          art::value_type{r.lo.data(), r.lo.size()},
                                          art::value_type{r.hi.data(), r.hi.size()}) != rr.hash)
                        changed = "TXCONFLICT a range the transaction walked has changed since it was walked";
                }
                std::lock_guard l(x_lock);
                // the one in the way, and its home, for the waiter to push it - TODO 696
                auto holder = [&](const std::string& other) {
                    const auto o = txs.find(other);
                    const uint32_t home = o != txs.end() && !o->second.groups.empty() ? o->second.groups[0] : number;
                    return other + " home " + std::to_string(home);
                };
                // the same id again: the log applied again after a restart, or a retry; or
                // one its home aborted before this prepare got here, for which this is late
                if (auto known = txs.find(id); known != txs.end()) {
                    if (known->second.state == "ABORTED" && known->second.index != idx)
                        answer = "TXABORTED transaction " + id + " was aborted";
                    return true;
                }
                /*
                 * Locks, the same on every member: a key it writes can't be held by another
                 * transaction's write or read; a key it reads can't be held by another's
                 * write. Reads share - TODO 695.
                 */
                std::set<std::string> written;
                for (const auto& r : records) {
                    if (!ours(r))
                        continue;
                    const auto key = r.space + std::string(1, '\0') + r.key;
                    written.insert(key);
                    auto held = intents.find(key);
                    if (held != intents.end() && held->second != id) {
                        answer = "TXCONFLICT a key in the transaction is held by transaction " + holder(held->second);
                        return true;            // refused, the same on every member
                    }
                    auto shared = read_intents.find(key);
                    if (shared != read_intents.end())
                        for (const auto& other : shared->second)
                            if (other != id) {
                                answer = "TXCONFLICT a key in the transaction is read by transaction " + holder(other);
                                return true;
                            }
                    for (const auto& [other, walked] : range_intents) {
                        if (other == id)
                            continue;
                        for (const auto& rr : walked)
                            if (rr.holds(r.space, r.key)) {
                                answer = "TXCONFLICT a key in the transaction is in a range walked by transaction "
                                         + holder(other);
                                return true;
                            }
                    }
                }
                for (const auto& rk : reads) {
                    auto held = intents.find(rk.lock);
                    if (held != intents.end() && held->second != id) {
                        answer = "TXCONFLICT a key the transaction read is held by transaction "
                                 + holder(held->second);
                        return true;
                    }
                }
                for (const auto& rr : ranges) {
                    const auto& r = rr.range;
                    const std::string prefix = r.space + std::string(1, '\0');
                    for (auto i = intents.lower_bound(prefix + r.lo); i != intents.end(); ++i) {
                        if (i->first.compare(0, prefix.size(), prefix) != 0)
                            break;
                        if (!r.hi.empty() && i->first.compare(prefix.size(), std::string::npos, r.hi) >= 0)
                            break;
                        if (i->second != id) {
                            answer = "TXCONFLICT a range the transaction walked holds a key of transaction "
                                     + holder(i->second);
                            return true;
                        }
                    }
                }
                tx_info t;
                t.index = idx;
                t.state = "PENDING";
                t.groups = group_list(words[1]);
                t.space = !records.empty() ? barch::ks_undecorate(records[0].space)
                        : !reads.empty() ? barch::ks_undecorate(reads[0].space)
                        : !ranges.empty() ? barch::ks_undecorate(ranges[0].range.space) : std::string();
                t.records.assign(words.begin() + 3, words.begin() + 3 + (ptrdiff_t) nrec);
                t.reads.assign(words.begin() + 3 + (ptrdiff_t) nrec, words.end());
                t.seen = std::chrono::steady_clock::now();
                for (const auto& key : written)
                    intents[key] = id;
                for (const auto& rk : reads)
                    if (!written.count(rk.lock))
                        read_intents[rk.lock].insert(id);
                for (const auto& rr : ranges) {
                    // one range comes as a word per shard: kept once
                    auto& walked = range_intents[id];
                    if (std::none_of(walked.begin(), walked.end(), [&](const range_read& w) {
                            return w.space == rr.range.space && w.lo == rr.range.lo && w.hi == rr.range.hi;
                        }))
                        walked.push_back(rr.range);
                }
                note_spread(id, t.space, t.groups, number, 'P');
                txs[id] = std::move(t);
                answer = changed;
                return true;
            }
            std::vector<std::string> records, reads;
            std::vector<uint32_t> groups;
            std::string space;
            {
                std::lock_guard l(x_lock);
                auto i = txs.find(id);
                if (i == txs.end()) {
                    if (version == entry_version_txcommit) {
                        answer = "TXUNKNOWN no transaction " + id + " here";
                        return true;
                    }
                    /*
                     * An abort for one not prepared here: kept, aborted, so a prepare that
                     * comes after it is refused - TODO 694. A coordinator slower than its
                     * home's patience can't then commit what the home gave up on.
                     */
                    tx_info t;
                    t.index = idx;
                    t.state = "ABORTED";
                    t.closed_ms = now_ms();
                    txs[id] = std::move(t);
                    return true;
                }
                if (i->second.state != "PENDING") {
                    if (version == entry_version_txcommit && i->second.state != "COMMITTED")
                        answer = "TXABORTED transaction " + id + " was aborted";
                    return true;
                }
                if (version == entry_version_txabort) {
                    for (const auto& w : i->second.records) {
                        aof::record r;
                        if (aof::decode((const uint8_t*) w.data(), (uint32_t) w.size(), r) == aof::decoded::ok) {
                            auto held = intents.find(r.space + std::string(1, '\0') + r.key);
                            if (held != intents.end() && held->second == id)
                                intents.erase(held);
                        }
                    }
                    drop_read_intents(id, i->second.reads);
                    i->second.state = "ABORTED";
                    std::vector<std::string>().swap(i->second.records);
                    std::vector<std::string>().swap(i->second.reads);
                    i->second.closed_ms = now_ms();
                    note_spread(id, i->second.space, i->second.groups, number, 'A');
                    return true;
                }
                records = i->second.records;
                reads = i->second.reads;
                groups = i->second.groups;
                space = i->second.space;
            }
            // commit: this group's records in, under each space's tx_gate, so a read command
            // sees all of them or none; and in a split space, waits for the other groups'
            // (note_spread)
            std::vector<aof::record> decoded;
            std::set<std::string> names;
            for (const auto& w : records) {
                aof::record r;
                if (aof::decode((const uint8_t*) w.data(), (uint32_t) w.size(), r) != aof::decoded::ok) {
                    why = "a transaction's record that doesn't decode";
                    return false;
                }
                names.insert(barch::ks_undecorate(r.space));
                decoded.push_back(std::move(r));
            }
            std::vector<std::unique_lock<std::shared_mutex>> gates;
            for (const auto& name : names)              // in name order, one way round
                if (auto ks = barch::get_keyspace(name.empty() ? std::string("0") : name))
                    gates.emplace_back(ks->tx_gate);
            for (const auto& r : decoded) {
                if (!ours(r))
                    continue;
                repl::applying applying;
                if (!repl::apply_one(r, why))
                    return false;
            }
            // still holding the gates: a read sees this group's part and the count together
            note_spread(id, space, groups, number, 'C');
            gates.clear();
            std::lock_guard l(x_lock);
            auto i = txs.find(id);
            for (const auto& r : decoded) {
                auto held = intents.find(r.space + std::string(1, '\0') + r.key);
                if (held != intents.end() && held->second == id)
                    intents.erase(held);
            }
            drop_read_intents(id, reads);
            if (i != txs.end()) {
                i->second.state = "COMMITTED";
                std::vector<std::string>().swap(i->second.records);
                std::vector<std::string>().swap(i->second.reads);
                i->second.closed_ms = now_ms();
            }
            const uint64_t now = now_ms();
            for (auto j = txs.begin(); j != txs.end();) {
                if (j->second.state != "PENDING" && now - j->second.closed_ms > xbatch_keep_ms)
                    j = txs.erase(j);
                else
                    ++j;
            }
            return true;
        }

        binding::result group_rt::commit_tx(int op, const std::vector<std::string>& words, std::string& answer,
                                            std::string& why) {
            const uint8_t version = op == tx_prepare ? entry_version_txprepare
                                  : op == tx_commit ? entry_version_txcommit : entry_version_txabort;
            std::vector<const std::string*> each;
            for (const auto& w : words)
                each.push_back(&w);
            // a prepare's shards go through the fence, as a write's do - TODO 694
            std::vector<size_t> shards;
            if (op == tx_prepare && words.size() >= 3) {
                size_t nrec = 0;
                try {
                    nrec = std::stoull(words[2]);
                } catch (const std::exception&) {
                }
                for (size_t k = 3; k < words.size(); ++k) {
                    if (k < 3 + nrec) {
                        aof::record rec;
                        if (aof::decode((const uint8_t*) words[k].data(), (uint32_t) words[k].size(), rec)
                            == aof::decoded::ok)
                            shards.push_back(rec.shard);
                    } else {
                        uint32_t shard = 0;
                        uint64_t hash = 0;
                        std::string sp, key;
                        if (tx_read_from(words[k], shard, sp, key, hash))
                            shards.push_back(shard);
                    }
                }
            }
            uint64_t idx = 0;
            const auto r = commit_entry(version, each, shards, why, &idx);
            if (r != binding::result::committed)
                return r;
            std::lock_guard l(replies_lock);
            const auto i = replies.find(idx);
            if (i == replies.end()) {
                why = "UNKNOWN the transaction's step committed at " + std::to_string(idx)
                      + " but its answer here was lost";
                return binding::result::unknown;
            }
            answer = i->second ? i->second->error() : std::string();
            replies.erase(i);
            return r;
        }

        binding::result group_rt::commit_xbatch(const std::vector<std::string>& words,
                                                const std::vector<size_t>& shards, std::string& why) {
            std::vector<const std::string*> each;
            each.reserve(words.size());
            for (const auto& w : words)
                each.push_back(&w);
            return commit_entry(entry_version_xbatch, each, shards, why);
        }

        binding::result group_rt::commit_xclose(const std::string& id, std::string& why) {
            return commit_entry(entry_version_xclose, {&id}, {}, why);
        }

        void group_rt::keep_reply(uint64_t idx, reply_ptr reply) {
            std::lock_guard l(replies_lock);
            // a client that gave up on its command (UNKNOWN) never takes its reply;
            // keep the last few hundred so those don't pile up
            while (replies.size() >= 256)
                replies.erase(replies.begin());
            replies[idx] = std::move(reply);
        }

        binding::result group_rt::commit_entry(uint8_t version, const std::vector<const std::string*>& records,
                                               const std::vector<size_t>& shards, std::string& why,
                                               uint64_t* index) {
            const bool writer_applied = version == entry_version || version == entry_version_batch_applied;
            if (rt.quiescing) {
                why = "TRYAGAIN this node is shutting down";
                return binding::result::refused;
            }
            if (!ready()) {
                why = not_leader();
                return binding::result::refused;
            }
            // from the fence check until the entry is committed, so no write for a
            // shard being handed off lands in the log after the hand-off - TODO 616
            std::shared_lock appending(append_lock);
            for (const auto shard : shards) {
                if (shard >= fence_from.load()) {
                    why = "TRYAGAIN this shard is moving to another raft group";
                    return binding::result::refused;
                }
            }
            // test knob, TODO 617: hold writes between the fence check and the log
            // long enough for a hand-off to land among them
            static const long delay_ms = [] {
                const char* v = std::getenv("BARCH_TEST_COMMIT_DELAY_MS");
                return v ? std::atol(v) : 0L;
            }();
            if (delay_ms > 0 && number != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            const bool batch = version == entry_version_batch || version == entry_version_batch_applied
                               || version == entry_version_command
                               || version == entry_version_xbatch || version == entry_version_xclose
                               || version == entry_version_txprepare || version == entry_version_txcommit
                               || version == entry_version_txabort;
            size_t size = 9 + (batch ? 4 : 0);
            for (const auto* r : records)
                size += r->size() + (batch ? 4 : 0);
            if (batch && (records.size() > UINT32_MAX || size > UINT32_MAX)) {
                why = "ERR the batch is too large for one raft entry";
                return binding::result::refused;
            }
            auto buf = nuraft::buffer::alloc(size);
            auto* p = buf->data_begin();
            p[0] = version;
            const uint64_t inc = incarnation.load();
            for (int i = 0; i < 8; ++i) p[1 + i] = (uint8_t) ((inc >> (8 * i)) & 0xff);
            size_t at = 9;
            if (batch) {
                put_u32(p + at, (uint32_t) records.size());
                at += 4;
            }
            for (const auto* r : records) {
                if (batch) {
                    put_u32(p + at, (uint32_t) r->size());
                    at += 4;
                }
                std::memcpy(p + at, r->data(), r->size());
                at += r->size();
            }
            uint64_t idx = 0;
            std::string w;
            auto raft = rg();
            if (!raft) {
                why = not_leader();
                return binding::result::refused;
            }
            switch (raft->append(buf, idx, w)) {
                case raft_group::outcome::committed:
                    if (index)
                        *index = idx;
                    // for the session whose command this is - TODO 613, by group - TODO 615
                    note_committed(label, idx);
                    if (!split())
                        for (const auto& sp : spaces)
                            if (sp != label) note_committed(sp, idx);
                    return binding::result::committed;
                case raft_group::outcome::refused:
                    why = raft->is_leader() && w != "not the leader" ? "ERR the write was refused: " + w
                                                                      : not_leader();
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
                // the cluster's own copy, not a primary's - TODO 684
                repl::raft_copy cluster_copy;
                const bool ok = same && whole ? local(s, {"RETRIEVE", h, std::to_string(p)}, nullptr, err)
                                              : logical_copy(rpc_address, s, err, shard_from, shard_to);
                if (!ok) {
                    err = "copying " + s + " from " + rpc_address + ": " + err;
                    return false;
                }
                /*
                 * The copy has the data but not this node's index paths, which are
                 * built from the data on each member - TODO 669. Built again here,
                 * by the definitions this member holds; local writes, since every
                 * member derives its own.
                 */
                // and the functions it brought, which no install ran for - TODO 667
                barch::functions::function_keys_changed(barch::get_keyspace(s));
                try {
                    repl::applying applying;
                    if (!barch::pindex::rebuild(barch::get_keyspace(s), err)) {
                        err = "building the indexes of " + s + " again after copying it: " + err;
                        return false;
                    }
                } catch (const std::exception& e) {
                    err = "building the indexes of " + s + " again after copying it: " + e.what();
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
            /*
             * The sender's keys at one point - TODO 674. A sender from before answers
             * EXPORTSTART with an error, and is paged live, as before.
             */
            std::string id;
            // test knob: page live, as before, to show what that did
            static const bool live = std::getenv("BARCH_TEST_LIVE_EXPORT") != nullptr;
            if (!live) {
                std::vector<std::string> started;
                std::string e;
                if (remote(from, {"CLUSTER", "EXPORTSTART", space, std::to_string(shard_from), std::to_string(end)},
                           &started, e, 120000) && !started.empty())
                    id = started[0];
                else
                    barch::warn({"cluster: copying", space, "from", from, "page by page as it changes -", e});
            }
            struct end_copy {
                const std::string& from;
                const std::string& id;
                ~end_copy() {
                    if (id.empty()) return;
                    std::string e;
                    remote(from, {"CLUSTER", "EXPORTEND", id}, nullptr, e, 5000);
                }
            } ended{from, id};
            for (size_t shard = shard_from; shard < end; ++shard) {
                uint64_t page = 0;
                for (;;) {
                    std::vector<std::string> got;
                    std::vector<std::string> ask{"CLUSTER", "EXPORT", space, std::to_string(shard), std::to_string(page)};
                    if (!id.empty())
                        ask.push_back(id);
                    if (!remote(from, ask, &got, err, 30000))
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

        /** one leaf as an EXPORT record, hex; empty for one that isn't sent */
        std::string export_leaf(const barch::shard_ptr& t, size_t shard, const art::leaf& l) {
            const auto key = l.get_key();
            if (barch::meta::is_meta(key)) {
                // the dictionary is each node's own, as it is for PUBLISH
                std::string name;
                if (barch::meta::name_of(key, name) && name == dictionary::meta_name)
                    return {};
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
            return to_hex(std::string((const char*) encoded.data(), encoded.size()));
        }

        // EXPORT <space> <shard> <page> [<id>] - one page of a shard's leaves, as records;
        // with an id, from the copy EXPORTSTART took - TODO 674
        bool runtime_impl::export_page(const std::vector<std::string>& args, std::vector<std::string>& out,
                                       std::string& err) {
            if (args.size() != 4 && args.size() != 5) {
                err = "ERR CLUSTER EXPORT space shard page [id]";
                return false;
            }
            size_t shard = 0, page = 0;
            try {
                shard = std::stoull(args[2]);
                page = std::stoull(args[3]);
            } catch (...) {
                err = "ERR shard and page are numbers";
                return false;
            }
            if (args.size() == 5) {
                std::lock_guard l(exports_lock);
                auto i = exports.find(args[4]);
                if (i == exports.end() || i->second.space != args[1]) {
                    err = "ERR no copy " + args[4] + " here - it was ended or dropped";
                    return false;
                }
                const auto& pages = i->second.shards;
                if (shard < i->second.from || shard - i->second.from >= pages.size()) {
                    err = "ERR shard " + std::to_string(shard) + " isn't in copy " + args[4];
                    return false;
                }
                const auto& mine = pages[shard - i->second.from];
                out.emplace_back(page + 1 < mine.size() ? std::to_string(page + 1) : std::string("0"));
                if (page < mine.size())
                    out.insert(out.end(), mine[page].begin(), mine[page].end());
                return true;
            }
            auto ks = barch::get_keyspace(args[1]);
            if (!ks || shard >= ks->get_shard_count()) {
                err = "ERR no such shard";
                return false;
            }
            auto t = ks->get(shard);
            barch::sharded_store store(ks);
            out.emplace_back();
            const bool more = store.leaf_page(shard, page, [&](const art::leaf& l) {
                auto hex = export_leaf(t, shard, l);
                if (!hex.empty())
                    out.push_back(std::move(hex));
            });
            out[0] = more ? std::to_string(page) : std::string("0");
            return true;
        }

        /*
         * EXPORTSTART <space> <from> <to> - TODO 674. A key by key copy paged through the
         * live shards, so it could hold some of a run from the log and not the rest, and
         * the run marker from before or after it; the log applied from the snapshot then
         * ran it again or not at all. This takes the shards' keys at one point instead:
         * any run under way is waited out, as a save's freeze waits, and the keys are
         * read under the space write lock, so no write and no run is part way in. They
         * go into a list in memory, a page per shard page, and the copy is served from
         * it. Writes wait only for that local walk. A list left behind goes after ten
         * minutes.
         */
        bool runtime_impl::export_start(const std::vector<std::string>& args, std::vector<std::string>& out,
                                        std::string& err) {
            if (args.size() != 4) {
                err = "ERR CLUSTER EXPORTSTART space from to";
                return false;
            }
            auto ks = barch::get_keyspace(args[1]);
            if (!ks) {
                err = "ERR no such space";
                return false;
            }
            size_t from = 0, to = 0;
            try {
                from = std::stoull(args[2]);
                to = std::min<size_t>(std::stoull(args[3]), ks->get_shard_count());
            } catch (...) {
                err = "ERR from and to are numbers";
                return false;
            }
            export_copy copy;
            copy.space = args[1];
            copy.from = from;
            copy.at = std::chrono::steady_clock::now();
            barch::sharded_store store(ks);
            for (;;) {
                {
                    barch::sharded_store::write_guard all = store.lock_space_write();
                    if (!ks->log_run_under_way()) {
                        for (size_t shard = from; shard < to; ++shard) {
                            auto t = ks->get(shard);
                            std::vector<std::vector<std::string>> pages;
                            size_t page = 0;
                            bool first = true;
                            while (t && t->get_size() > 0 && (first || page > 0)) {
                                first = false;
                                heap::vector<uint8_t> buffer;
                                t->page(page, buffer);         // the latch is held, with the rest
                                std::vector<std::string> recs;
                                if (!buffer.empty()) {
                                    art::page_iterator_ptr(buffer.data(), (unsigned) buffer.size(),
                                        [&](const art::leaf* l, uint32_t) -> bool {
                                            if (!l->deleted() && !l->is_tomb() && !l->expired()) {
                                                auto hex = export_leaf(t, shard, *l);
                                                if (!hex.empty())
                                                    recs.push_back(std::move(hex));
                                            }
                                            return true;
                                        });
                                }
                                pages.push_back(std::move(recs));
                                page = t->next_page(page);
                            }
                            copy.shards.push_back(std::move(pages));
                        }
                        break;
                    }
                }
                ks->wait_log_runs();            // nothing held while a run finishes
            }
            const std::string id = to_hex(std::to_string(random_u64()));
            std::lock_guard l(exports_lock);
            const auto now = std::chrono::steady_clock::now();
            for (auto i = exports.begin(); i != exports.end();)
                i = now - i->second.at > std::chrono::minutes(10) ? exports.erase(i) : std::next(i);
            exports[id] = std::move(copy);
            out.push_back(id);
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
            // this node's users become the cluster's, so every member takes them - TODO 676
            barch::seed_cluster_users();
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
            if (quiescing) {
                err = "TRYAGAIN this node is shutting down";
                return false;
            }
            /*
             * One heartbeat per node at a time - TODO 642. Each is a Raft commit for
             * `seen` plus one per space, and on a slow cluster group they arrived
             * faster than they committed: RESP threads piled up waiting on them, and
             * at shutdown each sat out its timeout. A heartbeat carries the node's
             * whole state, so one that comes while the last is still being written
             * isn't needed: the next brings it all.
             */
            {
                std::lock_guard l(heartbeats_lock);
                if (!heartbeats_writing.insert(id).second)
                    return true;
            }
            struct done_writing {
                runtime_impl& rt;
                const std::string& id;
                ~done_writing() {
                    std::lock_guard l(rt.heartbeats_lock);
                    rt.heartbeats_writing.erase(id);
                }
            } writing{*this, id};
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
                // configuration has caught up with it (see open_spaces).
                // `group` goes in last, in a write of its own - TODO 735. A member
                // sees an HSET's fields arrive one at a time, so it could read the
                // record with group and creator but no groups yet, and open a split
                // space's first group over the whole space. read_spaces skips a record with
                // no group, so this way a member sees all of it or none of it.
                if (local(cluster_space, {"HSET", "space:" + name, "mode", "raft", "creator", std::to_string(me),
                                          "groups", std::to_string(of), "shards", shards},
                          nullptr, err)
                    && local(cluster_space, {"HSET", "space:" + name, "group", std::to_string(top + 1)}, nullptr,
                             err)) {
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
             * Caught up means within a few entries of the leader, or at an index the
             * leader had up to 3s ago. Under steady writes a member that keeps up is
             * still a batch behind at any moment, ~30 entries since TODO 632, so the
             * first test alone never passed - TODO 635. And one on a slow disk, its
             * syncs taking a second, is a second or two behind: the first version
             * looked at the previous tick, 250ms to 1s back, and never promoted it,
             * which left a group with its leader as the only voter - TODO 653.
             *
             * Each learner's mark is the leader's last index and when it was read.
             * It's kept until the learner reaches it, and replaced once it's 3s old
             * without being reached, so a learner that's further behind than that,
             * or copying a snapshot, isn't promoted.
             */
            const uint64_t top = r->last_index();
            const uint64_t now = now_ms();
            for (const auto& m : r->members()) {
                if (!m.learner) continue;
                const uint64_t at = r->member_index(m.id);
                auto [mark, fresh] = g->learner_marks.try_emplace(m.id, top, now);
                const bool kept_up = !fresh && at >= mark->second.first;
                if (!kept_up && now - mark->second.second > 3000)
                    mark->second = {top, now};
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
            // shutdown has begun: nothing new for the groups to write - TODO 642
            if (quiescing) return;
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
                    recover_xbatches(g);
                    // a transaction prepared and never committed: its coordinator is gone - TODO 693
                    recover_txs(g);
                }
            }
            expire_spreads();
            if (now_ms() - last_heartbeat >= heartbeat_ms) {
                last_heartbeat = now_ms();
                send_heartbeat();
            }
        }

        namespace {
            /** what a command run through another node's group answered - TODO 690 */
            struct text_reply : command_reply {
                text_reply(std::string e, std::string r) : e(std::move(e)), r(std::move(r)) {}
                [[nodiscard]] std::string error() const override { return e; }
                [[nodiscard]] std::string result() const override { return r; }
                std::string e, r;
            };
        }

        binding::result space_binding::commit_command(const std::string& space, const std::vector<std::string>& argv,
                                                      reply_ptr& reply, std::string& why) {
            // only a split space's: elsewhere a member that doesn't lead answers NOTLEADER,
            // and the client goes to the one that does
            if (g->ready() || !g->split())
                return g->commit_command(space, argv, reply, why);
            return g->rt.x_command(g->number, space, argv, reply, why) ? result::committed : result::refused;
        }

        binding::result space_binding::commit_xbatch(const std::vector<std::string>& words,
                                                     const std::vector<size_t>& shards, std::string& why) {
            if (g->ready())
                return g->commit_xbatch(words, shards, why);
            return g->rt.x_forward(g->number, words, why) ? result::committed : result::refused;
        }

        binding::result space_binding::commit_tx(int op, const std::vector<std::string>& words, std::string& answer,
                                                 std::string& why) {
            return g->rt.tx_to(g->number, op, words, answer, why) ? result::committed : result::refused;
        }

        binding::result space_binding::commit_xclose(const std::string& id, std::string& why) {
            if (g->ready())
                return g->commit_xclose(id, why);
            return g->rt.x_close(g->number, id, why) ? result::committed : result::refused;
        }

        /*
         * A batch across groups whose writer went before every group had it - TODO 666.
         * The leader of a group that has had one open for a while asks each of its
         * other groups. One without it gets the same entry (a roll forward), which it
         * applies once even if the writer's own entry turns up after all. Once every
         * group has it, each gets a close. Groups led elsewhere are asked over RESP,
         * signed with the cluster secret, since a forward puts records in a log.
         */
        void runtime_impl::recover_xbatches(const std::shared_ptr<group_rt>& g) {
            if (!g->ready())
                return;
            for (const auto& id : g->stale_xbatches()) {
                std::vector<std::string> words;
                if (!g->x_words(id, words) || words.size() < 2)
                    continue;
                std::vector<uint32_t> numbers;
                std::istringstream in(words[1]);
                for (std::string n; std::getline(in, n, ',');) {
                    try {
                        numbers.push_back((uint32_t) std::stoul(n));
                    } catch (const std::exception&) {
                    }
                }
                bool everywhere = true;
                for (const auto n : numbers) {
                    if (n == g->number)
                        continue;
                    std::string status, err;
                    if (!x_status_of(n, id, status, err)) {
                        everywhere = false;         // no leader there now: the next tick
                        continue;
                    }
                    if (status != "none")
                        continue;
                    if (x_forward(n, words, err)) {
                        barch::log({"raft group", (uint64_t) g->number, "rolled batch", id, "forward into group",
                                    (uint64_t) n});
                    } else {
                        everywhere = false;
                        barch::warn({"raft group", (uint64_t) g->number, "couldn't roll batch", id,
                                     "forward into group", (uint64_t) n, "-", err});
                    }
                }
                if (!everywhere)
                    continue;
                for (const auto n : numbers) {
                    std::string err;
                    if (!x_close(n, id, err))
                        barch::warn({"raft group", (uint64_t) g->number, "couldn't close batch", id, "in group",
                                     (uint64_t) n, "-", err});
                }
            }
        }

        bool runtime_impl::x_status_of(uint32_t n, const std::string& id, std::string& status, std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready()) {
                status = h->x_status(id);
                return true;
            }
            const auto where = leader_address(h);
            if (where.empty()) {
                err = "group " + std::to_string(n) + " has no leader";
                return false;
            }
            std::vector<std::string> out;
            if (!remote(where, {"CLUSTER", "XSTATE", std::to_string(n), id}, &out, err, 5000) || out.empty())
                return false;
            status = out[0];
            return true;
        }

        bool runtime_impl::x_forward(uint32_t n, const std::vector<std::string>& words, std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready())
                return h->commit_xbatch(words, {}, err) == binding::result::committed;
            const auto where = leader_address(h);
            if (where.empty()) {
                err = "group " + std::to_string(n) + " has no leader";
                return false;
            }
            std::vector<std::string> cmd{"CLUSTER", "XFORWARD", std::to_string(n),
                                         sign_words(barch::get_cluster_secret(), "xforward", words)};
            cmd.insert(cmd.end(), words.begin(), words.end());
            return remote(where, cmd, nullptr, err, 10000);
        }

        bool runtime_impl::x_command(uint32_t n, const std::string& space, const std::vector<std::string>& argv,
                                     reply_ptr& reply, std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            const auto where = leader_address(h);
            if (where.empty()) {
                err = h->not_leader();
                return false;
            }
            std::vector<std::string> words{space};
            words.insert(words.end(), argv.begin(), argv.end());
            std::vector<std::string> cmd{"CLUSTER", "XCOMMAND", std::to_string(n),
                                         sign_words(barch::get_cluster_secret(), "xcommand", words)};
            cmd.insert(cmd.end(), words.begin(), words.end());
            std::vector<std::string> out;
            if (!remote(where, cmd, &out, err, 30000))
                return false;
            // its error, or OK and its first result
            const std::string said = out.empty() ? std::string() : out[0];
            reply = std::make_shared<text_reply>(said.rfind("ERR:", 0) == 0 ? said.substr(4) : std::string(),
                                                 out.size() > 1 ? out[1] : std::string());
            return true;
        }

        /*
         * A transaction pending in `g` whose coordinator has had five seconds - TODO 693,
         * 694. In its home group (or its only one) it's aborted. In any other, its home
         * decides: committed there, it's committed here (rolled forward); aborted there,
         * or never prepared there, it's aborted here, after making sure the home has it
         * aborted too, so a slow coordinator can't commit it afterwards. Still pending
         * at home, the home's own timer settles it first.
         */
        /** what a forwarded transaction step is signed as */
        const char* tx_sign_tag(int op) {
            return op == tx_prepare ? "txprepare" : op == tx_commit ? "txcommit" : "txabort";
        }

        void runtime_impl::recover_txs(const std::shared_ptr<group_rt>& g) {
            if (!g->ready())
                return;
            for (const auto& [id, groups] : g->stale_txs()) {
                int op = tx_abort;
                std::string because = "prepared, and not committed within 5 s";
                if (!groups.empty() && groups[0] != g->number) {
                    std::string status, err;
                    if (!tx_status_of(groups[0], id, status, err))
                        continue;                       // no leader at its home now: the next tick
                    if (status == "PENDING")
                        continue;
                    if (status == "none") {
                        std::string answer;
                        if (!tx_to(groups[0], tx_abort, {id}, answer, err))
                            continue;
                        status = "ABORTED";
                    }
                    op = status == "COMMITTED" ? tx_commit : tx_abort;
                    because = "its home, group " + std::to_string(groups[0]) + ", has it " + status;
                }
                std::string answer, why;
                if (g->commit_tx(op, {id}, answer, why) == binding::result::committed)
                    barch::log({"raft group", (uint64_t) g->number, op == tx_commit ? "committed" : "aborted",
                                "transaction", id, "-", because});
            }
        }

        bool runtime_impl::tx_to(uint32_t n, int op, const std::vector<std::string>& words, std::string& answer,
                                 std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready())
                return h->commit_tx(op, words, answer, err) == binding::result::committed;
            const auto where = leader_address(h);
            if (where.empty()) {
                err = h->not_leader();
                return false;
            }
            const std::string step = std::to_string(op);
            std::vector<std::string> cmd{"CLUSTER", "TXFORWARD", std::to_string(n), step,
                                         sign_words(barch::get_cluster_secret(), tx_sign_tag(op), words)};
            cmd.insert(cmd.end(), words.begin(), words.end());
            std::vector<std::string> out;
            if (!remote(where, cmd, &out, err, 30000))
                return false;
            // "=" and the step's answer, which is empty when it went through
            answer = !out.empty() && !out[0].empty() ? out[0].substr(1) : std::string();
            return true;
        }

        bool runtime_impl::tx_status_of(uint32_t n, const std::string& id, std::string& status, std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready()) {
                status = h->tx_status(id);
                return true;
            }
            const auto where = leader_address(h);
            if (where.empty()) {
                err = "group " + std::to_string(n) + " has no leader";
                return false;
            }
            std::vector<std::string> out;
            if (!remote(where, {"CLUSTER", "TXSTATE", std::to_string(n), id}, &out, err, 5000) || out.empty())
                return false;
            status = out[0];
            return true;
        }

        bool runtime_impl::tx_info_of(uint32_t n, const std::string& id, std::string& status, uint64_t& age_ms,
                                      std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready()) {
                status = h->tx_status(id);
                age_ms = h->tx_age(id);
                return true;
            }
            const auto where = leader_address(h);
            if (where.empty()) {
                err = "group " + std::to_string(n) + " has no leader";
                return false;
            }
            std::vector<std::string> out;
            if (!remote(where, {"CLUSTER", "TXINFO", std::to_string(n), id}, &out, err, 5000) || out.size() < 2)
                return false;
            status = out[0];
            try {
                age_ms = std::stoull(out[1]);
            } catch (const std::exception&) {
                age_ms = 0;
            }
            return true;
        }

        /*
         * A transaction in the way of another - TODO 696, choice 4. The one asking waited
         * first (COMMITTX, about 50 ms). Now the older of the two wins: ids start with
         * their BEGINTX time, so the smaller is older, by the coordinators' clocks. A
         * holder pending a second at its home has a quiet coordinator, and anyone may
         * push it. The abort goes to its home first, which decides it; only once the
         * home has it aborted is it dropped where it's in the way. A holder its home
         * has committed is left alone: its commit is on the way there.
         */
        bool runtime_impl::push_tx(uint32_t home, uint32_t at, const std::string& holder, const std::string& mine,
                                   std::string& why) {
            std::string status;
            uint64_t age = 0;
            if (!tx_info_of(home, holder, status, age, why))
                return false;
            if (status == "COMMITTED") {
                why = "transaction " + holder + " is committed, and landing";
                return false;
            }
            if (status == "PENDING") {
                const bool older = mine < holder;
                if (!older && age < tx_quiet_ms) {
                    why = "transaction " + holder + " is older, and not quiet";
                    return false;
                }
                std::string answer;
                if (!tx_to(home, tx_abort, {holder}, answer, why))
                    return false;
                if (!tx_status_of(home, holder, status, why))
                    return false;
                if (status != "ABORTED") {
                    why = "transaction " + holder + " is " + status + " at its home";
                    return false;
                }
                barch::log({"transaction", mine, "pushed transaction", holder, "aside -",
                            older ? "it's younger" : "it was pending " + std::to_string(age) + " ms"});
            } else if (status == "none") {
                // never prepared at its home, or long gone: aborted there, so it can't be later
                std::string answer;
                if (!tx_to(home, tx_abort, {holder}, answer, why))
                    return false;
            }
            if (at != home) {
                std::string answer;
                if (!tx_to(at, tx_abort, {holder}, answer, why))
                    return false;
            }
            return true;
        }

        bool runtime_impl::x_close(uint32_t n, const std::string& id, std::string& err) {
            auto h = group_numbered(n);
            if (!h) {
                err = "no group " + std::to_string(n) + " here";
                return false;
            }
            if (h->ready())
                return h->commit_xclose(id, err) == binding::result::committed;
            const auto where = leader_address(h);
            if (where.empty()) {
                err = "group " + std::to_string(n) + " has no leader";
                return false;
            }
            return remote(where, {"CLUSTER", "XCLOSE", std::to_string(n),
                                  sign_words(barch::get_cluster_secret(), "xclose", {id}), id}, nullptr, err, 5000);
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
                 * members. A hash, list or ordered set answers GET with WRONGTYPE,
                 * and is counted by its contents instead - TODO 668: its fields and
                 * values (sorted, since an unordered space keeps them in any order),
                 * its items, and its members with their scores. Only the reads for
                 * its own kind answer anything.
                 */
                std::vector<std::string> keys;
                if (!local(args[1], {"KEYS", "*"}, &keys, err)) return false;
                std::sort(keys.begin(), keys.end());
                uint32_t crc = 0;
                for (const auto& k : keys) {
                    std::vector<std::string> v;
                    std::string why;
                    std::string both = k;
                    both.push_back('\0');
                    if (local(args[1], {"GET", k}, &v, why)) {
                        if (!v.empty()) both += v[0];
                    } else {
                        std::vector<std::string> fields, items, members;
                        std::string ignored;
                        local(args[1], {"HGETALL", k}, &fields, ignored);
                        local(args[1], {"LRANGE", k, "0", "-1"}, &items, ignored);
                        local(args[1], {"ZRANGE", k, "0", "-1", "WITHSCORES"}, &members, ignored);
                        std::vector<std::string> pairs;
                        for (size_t i = 0; i + 1 < fields.size(); i += 2)
                            pairs.push_back(fields[i] + '\0' + fields[i + 1]);
                        std::sort(pairs.begin(), pairs.end());
                        both += "h";
                        for (const auto& p : pairs) both += p + '\0';
                        both += "l";
                        for (const auto& i : items) both += i + '\0';
                        both += "z";
                        for (const auto& m : members) both += m + '\0';
                    }
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
            /*
             * Batches across groups, between members - TODO 666: XSTATE asks the group's
             * leader whether it has one, XFORWARD puts one in its log and XCLOSE closes
             * it. The two that write are signed with the cluster secret.
             */
            // how far a group has applied here - TODO 690
            if (sub == "APPLIED" && args.size() == 2) {
                std::shared_ptr<group_rt> h;
                try {
                    h = group_numbered((uint32_t) std::stoul(args[1]));
                } catch (const std::exception&) {
                }
                if (!h) {
                    err = "ERR no such group";
                    return false;
                }
                out.push_back(std::to_string(h->applied.load()));
                return true;
            }
            /*
             * Transactions over several groups - TODO 694: TXSTATE asks a group's leader what
             * it knows of one, TXFORWARD puts a step of one in its log, signed with the
             * cluster secret since a prepare puts records there.
             */
            if ((sub == "TXSTATE" && args.size() == 3) || (sub == "TXINFO" && args.size() == 3)
                || (sub == "TXFORWARD" && args.size() >= 5)) {
                std::shared_ptr<group_rt> h;
                try {
                    h = group_numbered((uint32_t) std::stoul(args[1]));
                } catch (const std::exception&) {
                }
                if (!h) {
                    err = "ERR no such group";
                    return false;
                }
                if (!h->ready()) {
                    err = h->not_leader();
                    return false;
                }
                if (sub == "TXSTATE") {
                    out.push_back(h->tx_status(args[2]));
                    return true;
                }
                if (sub == "TXINFO") {
                    out.push_back(h->tx_status(args[2]));
                    out.push_back(std::to_string(h->tx_age(args[2])));
                    return true;
                }
                int op = 0;
                try {
                    op = std::stoi(args[2]);
                } catch (const std::exception&) {
                }
                if (op != tx_prepare && op != tx_commit && op != tx_abort) {
                    err = "ERR no such transaction step";
                    return false;
                }
                const std::vector<std::string> words(args.begin() + 4, args.end());
                const auto secret = barch::get_cluster_secret();
                if (secret.empty() || !same_signature(args[3], sign_words(secret, tx_sign_tag(op), words))) {
                    err = "ERR not signed with this cluster's secret";
                    return false;
                }
                std::string answer;
                if (h->commit_tx(op, words, answer, err) != binding::result::committed)
                    return false;
                out.push_back("=" + answer);
                return true;
            }
            if (sub == "XSTATE" && args.size() == 3) {
                std::shared_ptr<group_rt> h;
                try {
                    h = group_numbered((uint32_t) std::stoul(args[1]));
                } catch (const std::exception&) {
                }
                if (!h) {
                    err = "ERR no such group";
                    return false;
                }
                if (!h->ready()) {
                    err = h->not_leader();
                    return false;
                }
                out.push_back(h->x_status(args[2]));
                return true;
            }
            // a command entry for a group led here, from a member that doesn't lead it - TODO 690
            if (sub == "XCOMMAND" && args.size() >= 6) {
                std::shared_ptr<group_rt> h;
                try {
                    h = group_numbered((uint32_t) std::stoul(args[1]));
                } catch (const std::exception&) {
                }
                if (!h) {
                    err = "ERR no such group";
                    return false;
                }
                const std::vector<std::string> words(args.begin() + 3, args.end());
                const auto secret = barch::get_cluster_secret();
                if (secret.empty() || !same_signature(args[2], sign_words(secret, "xcommand", words))) {
                    err = "ERR not signed with this cluster's secret";
                    return false;
                }
                reply_ptr reply;
                const std::vector<std::string> argv(words.begin() + 1, words.end());
                if (h->commit_command(words[0], argv, reply, err) != binding::result::committed)
                    return false;
                const auto e = reply ? reply->error() : std::string();
                out.push_back(e.empty() ? std::string("OK") : "ERR:" + e);
                out.push_back(reply ? reply->result() : std::string());
                return true;
            }
            if ((sub == "XFORWARD" && args.size() >= 6) || (sub == "XCLOSE" && args.size() == 4)) {
                std::shared_ptr<group_rt> h;
                try {
                    h = group_numbered((uint32_t) std::stoul(args[1]));
                } catch (const std::exception&) {
                }
                if (!h) {
                    err = "ERR no such group";
                    return false;
                }
                const std::vector<std::string> words(args.begin() + 3, args.end());
                const auto secret = barch::get_cluster_secret();
                if (secret.empty() || !same_signature(args[2], sign_words(secret, sub == "XFORWARD" ? "xforward" : "xclose",
                                                                            words))) {
                    err = "ERR not signed with this cluster's secret";
                    return false;
                }
                const auto r = sub == "XFORWARD" ? h->commit_xbatch(words, {}, err) : h->commit_xclose(words[0], err);
                if (r != binding::result::committed)
                    return false;
                out.push_back("OK");
                return true;
            }
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
            if (sub == "EXPORTSTART")
                return export_start(args, out, err);
            if (sub == "EXPORTEND" && args.size() == 2) {
                std::lock_guard l(exports_lock);
                exports.erase(args[1]);
                out.push_back("OK");
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
