//
// CLUSTER - TODO 610. The command is always there; what it does is the cluster's,
// when this build has one (see cluster_hooks.h).
//
#include "raft_writes.h"
#include "rpc/barch_functions.h"
#include <algorithm>
#include "cluster_api.h"

#include "cluster_hooks.h"
#include "rpc_caller.h"
#include "function_api.h"
#include "queue_service.h"
#include "auth_api.h"
#include "tx_session.h"
#include "keys.h"
#include "statistics.h"
#include "shard.h"
#include "keyspace_locks.h"
#include "keyspec.h"
#include "dictionary_compressor.h"
#include "sharded_store.h"
#include "meta_keys.h"
#include "aof_log.h"
#include "repl_api.h"
#include "foreign/foreign.h"

#include <map>
#include <iterator>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <thread>
#include <set>
#include <random>
#include <regex>
#include <memory>
#include <optional>

namespace {
    /*
     * How this node runs a command from a Raft log - TODO 669: in the space it was
     * logged for, as the server, so the leader's client was the one whose rights
     * were checked. Raft's own gate is off for it, as for the cluster's other local
     * calls: it's already in the log, and its writes are this member's to make.
     */
    bool run_logged(const std::string& space, const std::vector<std::string>& argv, uint64_t index,
                    barch::cluster::reply_ptr& reply, std::string& err) {
        auto ks = barch::get_keyspace(space);
        if (!ks) {
            err = "no space " + space;
            return false;
        }
        /*
         * A caller of its own for every entry - TODO 667. A kept one held compiled
         * functions from earlier entries: a member just started compiled the source
         * as it is now, and one that had been running kept the version it compiled
         * before a SETF, so the two ran different code. A new one compiles what the
         * store holds at this entry, on every member.
         */
        rpc_caller sc;
        sc.raft_local_reads = true;
        sc.set_kspace(ks);
        // the caller's user and rights, from the leader that logged it - TODO 667
        if (argv.size() < 3) {
            err = "a command entry with no user and rights";
            return false;
        }
        heap::vector<bool> rights;
        for (char c : argv[1])
            rights.push_back(c == '1');
        sc.set_acl(argv[0], rights);
        sc.space_acl = {};
        const std::vector<std::string> words(argv.begin() + 2, argv.end());
        // a stored function seeds math.random from the entry's index - TODO 673
        barch::functions::from_log logged(index);
        // a RELOAD's publish, for every member - TODO 667
        if (words.at(0) == barch::functions::publish_word) {
            for (size_t i = 1; i < words.size(); ++i)
                barch::functions::publish_compiled(words[i]);
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        // a replicated queue's push or settle, for every member - TODO 682
        if (words.at(0) == barch::mq::log_word)
            return barch::mq::run_from_log(ks, words, reply, err);
        // the cluster's first users - TODO 676
        if (words.at(0) == barch::auth_seed_word) {
            if (!barch::seed_users_from_log(words, err))
                return false;
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        // a miss a fetch found, kept by every member - TODO 685
        if (words.at(0) == barch::foreign::miss_word) {
            if (!barch::foreign::miss_from_log(ks, words, err))
                return false;
            auto done = std::make_shared<rpc_reply>();
            done->results.emplace_back(std::string("OK"));
            reply = std::move(done);
            return true;
        }
        std::string name = words.at(0);
        if (const auto colon = name.find_last_of(':'); colon != std::string::npos && colon + 1 < name.size())
            name = name.substr(colon + 1);
        std::string upper = name;
        for (auto& c : upper) c = (char) std::toupper((unsigned char) c);
        auto r = std::make_shared<rpc_reply>();
        auto fi = barch::barch_functions->find(upper);
        if (fi != barch::barch_functions->end() && name.find('.') == std::string::npos) {
            r->code = sc.call(words, fi->second.call);
        } else {
            // a stored function, looked up the way a client's call finds it
            std::string from_space, fname = name;
            if (const auto dot = name.find('.'); dot != std::string::npos) {
                from_space = name.substr(0, dot);
                fname = name.substr(dot + 1);
            }
            const caller::resolved* fn = barch::functions::resolve(sc, from_space, fname);
            if (!fn) {
                // the same on every member: the function isn't there at this entry
                sc.errors.clear();
                sc.results.clear();
                sc.errors.emplace_back("unknown command " + name);
                r->code = -1;
            } else {
                r->code = sc.call(words, fn->call);
            }
        }
        r->results = sc.results;
        r->errors = sc.errors;
        reply = std::move(r);
        return true;
    }
    const bool runner_installed = [] {
        barch::cluster::set_command_runner(run_logged);
        return true;
    }();
}

extern "C" {
    int CLUSTER(caller& call, const arg_t& argv) {
        if (argv.size() < 2)
            return call.wrong_arity();
        std::vector<std::string> args;
        args.reserve(argv.size() - 1);
        for (size_t i = 1; i < argv.size(); ++i)
            args.push_back(argv[i].to_string());
        std::string sub = args[0];
        for (auto& c : sub) c = (char) std::toupper((unsigned char) c);
        /*
         * This connection's own settings - TODO 613. READS chooses whether a
         * follower may answer its reads. INDEX is the newest index it has seen in
         * a space, to hand to another connection, which AFTER makes read no older.
         */
        /*
         * Every write command and how a space replicated with Raft treats it - TODO
         * 663: replicated, server, or local (refused). One that's local only because
         * nobody has looked at it yet says `unlisted` too. See raft_writes.h.
         */
        if (sub == "WRITES" && args.size() == 1) {
            std::vector<std::string> names;
            for (const auto& [name, f] : *barch::barch_functions)
                if (f.is_write()) names.emplace_back(name.c_str());
            std::sort(names.begin(), names.end());
            call.start_array();
            for (const auto& n : names) {
                std::string line = n + " " + barch::raft_writes::name_of(barch::raft_writes::of(n, "", ""));
                if (!barch::raft_writes::listed(n)) line += " unlisted";
                call.push_simple(line);
                // the subcommands decided on their own - TODO 669
                for (const auto& [sub, k] : barch::raft_writes::subcommands(n))
                    call.push_simple(n + "/" + sub + " " + barch::raft_writes::name_of(k));
            }
            return call.end_array();
        }
        if (sub == "READS" || sub == "INDEX" || sub == "AFTER") {
            auto* session = call.cluster_session();
            if (!session)
                return call.push_error("ERR this connection has no cluster session");
            if (sub == "READS" && args.size() == 2) {
                std::string how = args[1];
                for (auto& c : how) c = (char) std::toupper((unsigned char) c);
                if (how != "FOLLOWER" && how != "LEADER")
                    return call.push_error("ERR CLUSTER READS FOLLOWER|LEADER");
                session->follower_reads = how == "FOLLOWER";
                return call.push_simple("OK");
            }
            if (sub == "INDEX" && args.size() == 2)
                return call.push_ll((int64_t) session->after[args[1]]);
            if (sub == "AFTER" && args.size() == 3) {
                uint64_t at = 0;
                try {
                    at = std::stoull(args[2]);
                } catch (...) {
                    return call.push_error("ERR the index is a number");
                }
                auto& seen = session->after[args[1]];
                if (at > seen) seen = at;
                return call.push_simple("OK");
            }
            return call.wrong_arity();
        }
        std::vector<std::string> out;
        std::string err;
        if (!barch::cluster::command(args, out, err))
            return call.push_error(err.c_str());
        if (out.size() == 1)
            return call.push_simple(out[0]);
        call.start_array();
        for (const auto& line : out)
            call.push_simple(line);
        return call.end_array();
    }
}

/*
 * BEGINTX, COMMITTX, ABORTTX - TODO 693. See docs/TRANSACTIONS.md. A transaction on the
 * connection's space: its writes are gathered, not made, and COMMITTX puts them in the
 * space's group as a record and intents (prepare), then makes them (commit). In a split
 * space - TODO 694 - every group with a write in it is prepared, its home (the lowest
 * numbered) first; the commit in the home decides it, and the others follow, or are
 * rolled forward by their leaders if this node goes first. A space with no Raft is
 * TODO 450.
 */
namespace {
    /** its start time in ms, then random: the smaller id is the older one - TODO 696 */
    std::string tx_id() {
        // seeded once per thread: a random_device opened /dev/urandom on every BEGINTX - TODO 710
        thread_local std::mt19937_64 random{[] {
            std::random_device rd;
            return ((uint64_t) rd() << 32) ^ rd();
        }()};
        const auto ms = (unsigned long long) std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        char buf[48];
        std::snprintf(buf, sizeof(buf), "%016llx%016llx", ms, (unsigned long long) random());
        return buf;
    }
}

namespace {
    /*
     * A transaction's writes, from its overlay - TODO 701: every leaf the overlay holds
     * itself is a set of that key, every tomb an erase, as records for `space`, in shard
     * then key order. Numbered from 0 in `out`.
     */
    bool overlay_writes(const barch::key_space_ptr& overlay, const barch::key_space_ptr& space,
                        const barch::tx_overlay_reads* reads, barch::scratch::map<uint64_t, std::string>& out,
                        std::string& why) {
        if (!overlay)
            return true;
        uint64_t at = 0;
        for (size_t i = 0; i < overlay->get_shard_count(); ++i) {
            auto t = overlay->get(i);
            auto* sh = dynamic_cast<barch::shard*>(t.get());
            if (!sh) {
                why = "a transaction's overlay shard that isn't one";
                return false;
            }
            read_lock release(t);
            // on this thread: glob's queue and worker threads were most of what a small
            // commit cost, and its callback isn't safe to run on several at once - TODO 710
            art::each_leaf(sh, [&](const art::leaf& l) -> bool {
                if (l.deleted() || l.expired())     // as glob skipped them
                    return true;
                const auto key = l.get_key();
                // kept from a read and never changed since: not a write - TODO 701
                uint64_t read_hash = 0;
                if (reads && reads->hash_of(std::string(key.chars(), key.size), read_hash)
                    && read_hash == barch::tx_hash(!l.is_tomb(), l.is_tomb() ? art::value_type{} : l.get_value(),
                                                   l.is_tomb() ? 0 : (int64_t) l.expiry_ms()))
                    return true;
                barch::aof::record r;
                r.type = l.is_tomb() ? barch::aof::record_type::erase : barch::aof::record_type::set;
                r.space = space->space_name();
                const auto k = l.get_key();
                r.key.assign(k.chars(), k.size);
                if (!l.is_tomb()) {
                    const auto v = l.get_value();
                    r.value.assign(v.chars(), v.size);
                    r.expiry_ms = (int64_t) l.expiry_ms();
                    r.options = art::key_options(r.expiry_ms, false, l.is_volatile(), l.is_hashed(), false).flags;
                }
                r.shard = (uint32_t) i;
                r.shard_count = (uint32_t) space->get_shard_count();
                r.routing = barch::aof::routing_hash;
                std::vector<uint8_t> encoded;
                barch::aof::encode(r, encoded);
                out.set(at++, std::string((const char*) encoded.data(), encoded.size()));
                return true;
            });
        }
        return true;
    }

    /** the write that puts `r`'s key back as `space` holds it now - TODO 450 */
    std::string prior_record(const barch::key_space_ptr& space, const barch::aof::record& r) {
        barch::aof::record back = r;
        back.type = barch::aof::record_type::erase;
        back.value.clear();
        back.expiry_ms = 0;
        back.options = 0;
        barch::sharded_store store(space);
        store.search(art::value_type(r.key.data(), r.key.size()), [&](const art::node_ptr& n) {
            auto cl = n.const_leaf();
            if (cl->expired())
                return;
            auto vt = cl->get_value();
            if (cl->is_compressed())
                vt = dictionary::decompress(art::space_of(n), vt);
            back.type = barch::aof::record_type::set;
            back.value.assign(vt.chars(), vt.size);
            back.expiry_ms = (int64_t) cl->expiry_ms();
            back.options = art::key_options(back.expiry_ms, false, cl->is_volatile(), cl->is_hashed(), false).flags;
        });
        std::vector<uint8_t> encoded;
        barch::aof::encode(back, encoded);
        return {(const char*) encoded.data(), encoded.size()};
    }

    /*
     * How a COMMITTX ended, for the statistics - TODO 706: it answers through these, so
     * the count is made where the answer is. A refusal that says TXCONFLICT is a
     * conflict as well as an abort.
     */
    struct tx_outcome {
        bool done{false};
        bool ok{false};
        bool conflict{false};
    };
    int refused(caller& call, tx_outcome& out, const char* why) {
        out.done = true;
        out.conflict = std::strstr(why, "TXCONFLICT") != nullptr;
        return call.push_error(why);
    }
    int committed(caller& call, tx_outcome& out) {
        out.done = true;
        out.ok = true;
        return call.push_simple("OK");
    }

    /** the meta key a commit marker names, which no space holds - TODO 450 */
    std::string tx_marker_key() {
        composite q;
        const auto k = barch::meta::key(q, "tx.commit");
        return {k.chars(), k.size};
    }

    /*
     * COMMITTX in a space with no Raft - TODO 450, choice 6: the same protocol, the node
     * its only member. With no other member and the coordinator right here, the prepare
     * and the commit are one step: under the space's tx_gate held alone, so no read
     * command and no other transaction's commit is between them, the reads are checked
     * and the writes made.
     *
     * Atomic in the change log: each write carries the transaction's tag, and a commit
     * marker follows them; a replay applies tagged writes only when it finds the marker.
     * No save holds part of it: the commit counts as a run (key_space::log_run), which a
     * whole space save waits for, and as a write across shards, after which no shard
     * saves on its own. A write that fails part way puts back what the others changed
     * (the keys as they were before), tagged too, so a replay drops both.
     */
    int commit_here(caller& call, const barch::key_space_ptr& space, const std::string& id,
                    barch::scratch::map<uint64_t, std::string>& writes, const barch::tx_overlay_reads& reads,
                    tx_outcome& out, barch::tx_status* status) {
        std::unique_lock gate(space->tx_gate, std::defer_lock);
        {
            // a commit or a read in this space holds it - TODO 720
            barch::tx_waiting_scope waiting(status, "the commit gate of " + space->get_canonical_name(), "", true);
            gate.lock();
        }
        /*
         * Every shard of the space held for writing, from the check to the last write -
         * TODO 701. Plain writes don't take the gate, so without it one could land on a
         * key the transaction read between the check and its own write there, and be
         * lost. The writes below find their shards held and don't lock them again.
         */
        heap::vector<storage_release> held;
        barch::sharded_store(space).each_shard([&](const barch::shard_ptr& t) { held.emplace_back(t); });
        auto& hold = barch::shard_hold::current();
        hold.space = space.get();
        hold.all = true;
        struct let_go {
            ~let_go() { barch::shard_hold::current() = {}; }
        } let_go_of_hold;
        if (status) {
            std::lock_guard l(status->m);
            status->holds_what = "every shard of " + space->get_canonical_name();
        }
        bool changed = false;
        if (reads.versions) {
            // against the layers from its snapshot up: any key it read that's in one has
            // changed since - TODO 701. A clear or a load since leaves nothing there
            changed = reads.versions->clears_now() != reads.clears_at;
            reads.each([&](const std::string& key, uint32_t shard, uint64_t) {
                if (!changed && reads.versions->changed_since(reads.snapshot, shard,
                                                              art::value_type{key.data(), key.size()}))
                    changed = true;
            });
            // a range it walked that a key has been written into since, or out of: a phantom
            reads.each_range([&](art::value_type lo, art::value_type hi) {
                if (!changed && reads.versions->changed_in(reads.snapshot, lo, hi))
                    changed = true;
            });
        } else {
            reads.each([&](const std::string& key, uint32_t, uint64_t hash) {
                if (!changed && barch::tx_hash_now(space, key) != hash)
                    changed = true;
            });
        }
        if (changed)
            return refused(call, out, "ERR TXCONFLICT a key the transaction read has changed since it was read");
        /*
         * A key it writes that another transaction holds a lock on - TODO 719: that one
         * read it, or wrote it, and goes on as if it hadn't changed. So this one is the
         * conflict, as if that one had committed first. Its own locks, if it has them,
         * don't count.
         */
        std::shared_ptr<barch::tx_locks> locks;
        {
            std::lock_guard l(space->versions_mu);
            locks = space->locks;
        }
        if (locks && !locks->any())
            locks.reset();
        // what each key held, as the write that puts it back if one fails
        barch::scratch::map<uint64_t, std::string> before;
        uint64_t count = 0;
        uint64_t bytes_of_writes = 0;
        for (auto [at, record] : writes) {
            barch::aof::record r;
            if (barch::aof::decode((const uint8_t*) record.data(), (uint32_t) record.size(), r)
                != barch::aof::decoded::ok)
                return refused(call, out, "ERR a transaction's write that doesn't decode");
            if (locks) {
                const auto other = locks->held_by_other(id, barch::tx_locks::key_of(r.key.data(), r.key.size()));
                if (!other.empty())
                    return refused(call, out, ("ERR TXCONFLICT a key the transaction writes is locked by"
                                               " transaction " + other).c_str());
            }
            before.set(at, prior_record(space, r));
            bytes_of_writes += r.space.size() + r.key.size() + r.value.size();
            ++count;
        }
        uint32_t tag = 0;
        {
            std::random_device rd;
            while (tag == 0) tag = rd();
        }
        /*
         * Room in the change log for every write, each one's put back, and the marker,
         * held before the first write - TODO 450, with log::reservation (TODO 470). A
         * full disk then refuses the transaction before anything changes. Without it, a
         * log that stopped taking writes partway left the first ones made in memory,
         * and the put backs, through the same log, failed as well.
         */
        std::optional<barch::aof::log::reservation> room;
        if (const auto& log = space->get_change_log()) {
            const auto& sp = space->space_name();
            uint64_t bytes = barch::aof::log::record_bytes(sp.size(), tx_marker_key().size(),
                                                           barch::aof::tx_commit_value.size());
            for (auto [at, record] : before)
                bytes += barch::aof::log::record_bytes(sp.size(), 0, record.size());
            bytes += bytes_of_writes + count * barch::aof::log::record_bytes(0, 0, 0);
            try {
                room.emplace(*log, bytes);
            } catch (const std::exception& e) {
                return refused(call, out, ("ERR the transaction wasn't made: the change log has no room for it - "
                                        + std::string(e.what())).c_str());
            }
        }
        barch::key_space::log_run run(*space);
        space->note_cross_shard_write();
        std::string why;
        {
            barch::aof::log::tx_tag tagged(tag);
            /*
             * Checked against the locks above, every shard held since: what the shards
             * would refuse now is a lock taken by one that hasn't read yet, and reads
             * after this - TODO 719.
             */
            barch::tx_locks::guard_scope unchecked(false);
            // test knobs: a pause halfway, for a test to kill barchd in, and a write that fails
            static const long half_pause_ms = [] {
                const char* v = std::getenv("BARCH_TEST_TX_HALF_PAUSE_MS");
                return v ? std::atol(v) : 0L;
            }();
            static const long fail_at = [] {
                const char* v = std::getenv("BARCH_TEST_TX_FAIL_AT");
                return v ? std::atol(v) : -1L;
            }();
            uint64_t k = 0;
            for (auto [at, record] : writes) {
                if (half_pause_ms > 0 && k == count / 2)
                    std::this_thread::sleep_for(std::chrono::milliseconds(half_pause_ms));
                const bool failing = fail_at >= 0 && (long) k == fail_at && count > 1;
                barch::aof::record r;
                barch::aof::decode((const uint8_t*) record.data(), (uint32_t) record.size(), r);
                if (failing)
                    why = "BARCH_TEST_TX_FAIL_AT";
                // into `space` itself, not looked up by the record's name: that takes the
                // keyspace registry lock under the gate, and a USE making a new space
                // holds the registry lock while it takes the default space's gate - a
                // deadlock, which TSan found in TestRespClientLocal - TODO 714
                if (!failing && barch::repl::apply_one(space, r, why)) {
                    ++k;
                    continue;
                }
                // the ones already made go back as they were, in the order they were made
                for (auto [j, back] : before) {
                    if (j >= at)
                        break;
                    barch::aof::record b;
                    barch::aof::decode((const uint8_t*) back.data(), (uint32_t) back.size(), b);
                    std::string ignored;
                    barch::repl::apply_one(space, b, ignored);
                }
                return refused(call, out, ("ERR the transaction wasn't made, and nothing of it is left: " + why).c_str());
            }
        }
        if (const auto& log = space->get_change_log()) {
            try {
                log->append_tx_commit(space->space_name(), tx_marker_key(), tag);
            } catch (const std::exception& e) {
                barch::err({"transaction", id, "in", space->get_canonical_name(),
                            "is made, but its commit marker isn't in the change log:", e.what()});
                return refused(call, out, "ERR UNKNOWN the transaction is made, but a restart may drop it");
            }
        }
        return committed(call, out);
    }
}

extern "C" {
    int BEGINTX(caller& call, const arg_t& argv) {
        if (argv.size() != 1 && argv.size() != 2)
            return call.wrong_arity();
        // BEGINTX PESSIMISTIC: it locks its keys as it goes - TODO 719
        bool pessimistic = false;
        if (argv.size() == 2) {
            std::string how(argv[1].chars(), argv[1].size);
            for (auto& c : how) c = (char) std::toupper((unsigned char) c);
            if (how != "PESSIMISTIC")
                return call.push_error("ERR BEGINTX takes PESSIMISTIC or nothing");
            pessimistic = true;
        }
        auto* tx = call.transaction();
        if (!tx)
            return call.push_error("ERR this connection can't hold a transaction");
        if (tx->open)
            return call.push_error("ERR a transaction is already open on this connection");
        /*
         * A run with no client session behind it, cron or HTTP, has a session of its own
         * for the run (TODO 709). One from the Raft log never gets here: BEGINTX changes
         * the server, which a log run refuses, since every member would coordinate one.
         */
        auto space = call.kspace();
        if (!space)
            return call.push_error("ERR no space to begin a transaction in");
        if (pessimistic && space->has_raft())
            return call.push_error("ERR a space replicated with Raft doesn't take BEGINTX PESSIMISTIC yet: its"
                                   " transactions are optimistic (TODO 719)");
        // from here its reads take the gate, which a space with Raft always has - TODO 450
        if (!space->has_raft())
            space->tx_used = true;
        std::string why;
        auto overlay = barch::key_space::make_overlay(space, why);
        if (!overlay)
            return call.push_error(("ERR " + why).c_str());
        tx->open = true;
        tx->by_run = false;
        tx->failed.clear();
        tx->began = std::chrono::steady_clock::now();
        tx->id = tx_id();
        tx->space = space;
        tx->overlay = overlay;
        tx->reads = std::make_shared<barch::tx_overlay_reads>(overlay);
        /*
         * A snapshot of the space, which its reads are answered as of - TODO 701. In a Raft
         * space it's this node's: what it has applied, whose before-images it keeps as it
         * applies more. Never halfway through a commit, which holds the gate while it
         * writes, nor through one over several groups landed here in some and not yet the
         * rest (TODO 694), which is waited for as a read waits for it.
         */
        /*
         * Not one that locks its keys - TODO 719: it reads the latest, which a lock keeps
         * as it is until the transaction ends, and a snapshot would only be older.
         */
        if (pessimistic) {
            tx->pessimistic = true;
            tx->locks = space->locks_for_tx();
        } else {
            tx->versions = space->versions_for_tx(space);
        }
        if (!pessimistic) {
            std::shared_lock whole(space->tx_gate);
            if (space->tx_partial.load() > 0) {
                const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (space->tx_partial.load() > 0 && std::chrono::steady_clock::now() < until) {
                    whole.unlock();
                    {
                        std::unique_lock w(space->tx_settle_lock);
                        space->tx_settled.wait_until(w, until, [&] { return space->tx_partial.load() <= 0; });
                    }
                    whole.lock();
                }
            }
            if (space->tx_partial.load() > 0) {
                tx->open = false;
                tx->overlay.reset();
                tx->reads.reset();
                tx->space.reset();
                tx->versions.reset();
                barch::key_space::give_back(std::move(overlay));
                return call.push_error("TRYAGAIN a transaction is landing in this space's groups");
            }
            tx->snapshot = tx->versions->begin();
        }
        if (tx->versions) {
            tx->reads->versions = tx->versions;
            tx->reads->snapshot = tx->snapshot;
            tx->reads->clears_at = tx->versions->clears_now();
        }
        // listed in INFO transactions until it ends - TODO 720
        {
            auto st = std::make_shared<barch::tx_status>();
            st->id = tx->id;
            st->space = space->get_canonical_name();
            st->who = tx->who.empty() ? "unknown" : tx->who;
            st->began = tx->began;
            st->since = tx->began;
            tx->status = st;
            barch::tx_list(st);
        }
        ++statistics::tx_live;
        ++statistics::tx_begins;
        return call.push_simple("OK");
    }

    int ABORTTX(caller& call, const arg_t& argv) {
        if (argv.size() != 1)
            return call.wrong_arity();
        auto* tx = call.transaction();
        // the overlay's shards, which hold its writes, go with it
        if (!tx || !tx->abort_open())
            return call.push_error("ERR ABORTTX without BEGINTX");
        return call.push_simple("OK");
    }

    int commit_counted(caller& call, const arg_t& argv, tx_outcome& out, barch::tx_status* status);

    /*
     * Counted here, whichever way it ends - TODO 706. It stays live, and active, until
     * it's answered: the transaction is closed on the session at the start, so nothing
     * else counts it out.
     */
    int COMMITTX(caller& call, const arg_t& argv) {
        auto* tx = call.transaction();
        const bool was_open = tx && tx->open;
        // listed, as committing, until it's answered - TODO 720
        struct unlist_after {
            std::shared_ptr<barch::tx_status> s;
            ~unlist_after() {
                if (s) barch::tx_unlist(s);
            }
        } listed{tx ? std::move(tx->status) : nullptr};
        // its key locks, held until its writes are in, then let go of - TODO 719
        struct release_after {
            std::string id;
            barch::key_space_ptr space;
            std::shared_ptr<barch::tx_locks> locks;
            std::set<std::string> keys;
            ~release_after() {
                if (!locks)
                    return;
                // whoever parked on them - TODO 721, or behind one of its spans - TODO 749
                barch::tx_lock_wake(space, locks->release(id, keys));
            }
        } locked;
        if (tx && tx->open && tx->locks) {
            locked.id = tx->id;
            locked.space = tx->space;
            locked.locks = std::move(tx->locks);
            locked.keys = std::move(tx->locked);
        }
        if (tx) {
            tx->locks.reset();
            tx->locked.clear();
            tx->pessimistic = false;
        }
        if (listed.s) {
            std::lock_guard l(listed.s->m);
            listed.s->committing = true;
            listed.s->since = std::chrono::steady_clock::now();
        }
        tx_outcome out;
        int r = 0;
        {
            barch::tx_active_scope running(was_open);
            try {
                r = commit_counted(call, argv, out, listed.s.get());
            } catch (...) {
                if (was_open) {
                    --statistics::tx_live;
                    ++statistics::tx_aborts;
                }
                throw;
            }
        }
        if (was_open) {
            --statistics::tx_live;
            if (out.ok) {
                ++statistics::tx_commits;
            } else {
                ++statistics::tx_aborts;
                if (out.conflict) ++statistics::tx_conflicts;
            }
        }
        return r;
    }

    int commit_counted(caller& call, const arg_t& argv, tx_outcome& out, barch::tx_status* status) {
        if (argv.size() != 1)
            return call.wrong_arity();
        auto* tx = call.transaction();
        if (!tx || !tx->open)
            return refused(call, out, "ERR COMMITTX without BEGINTX");
        // past a limit, or open too long: it can only be ended, and this ends it - TODO 701
        const std::string failed = barch::tx_before(*tx);
        auto space = tx->space;
        const std::string id = tx->id;
        auto overlay = std::move(tx->overlay);
        auto reads = std::move(tx->reads);
        auto versions = std::move(tx->versions);
        const uint64_t snapshot = tx->snapshot;
        // back for the next transaction once this is done with it - TODO 711, and its
        // snapshot let go of once its writes are in - TODO 701
        struct give_back_overlay {
            barch::key_space_ptr& overlay;
            std::shared_ptr<barch::tx_overlay_reads>& reads;
            std::shared_ptr<barch::tx_versions>& versions;
            uint64_t snapshot;
            ~give_back_overlay() {
                reads.reset();
                barch::key_space::give_back(std::move(overlay));
                if (versions)
                    versions->end(snapshot);
            }
        } give_back{overlay, reads, versions, snapshot};
        tx->open = false;
        tx->failed.clear();
        tx->overlay.reset();
        tx->reads.reset();
        tx->space.reset();
        if (!failed.empty())
            return refused(call, out, failed.c_str());
        /*
         * Held from here to the end, so the maintenance thread can't let its snapshot go
         * while the commit checks against its layers - TODO 701. Gone already, it was open
         * too long, which the check above may have missed by a moment.
         */
        if (versions && !versions->claim(snapshot))
            return refused(call, out, ("TXABORTED the transaction was open longer than tx_max_age_ms (" +
                                       std::to_string(barch::get_tx_max_age_ms()) +
                                       " ms)").c_str());
        // its writes, from the overlay: a leaf a set, a tomb an erase - TODO 701
        barch::scratch::map<uint64_t, std::string> writes;
        std::string why;
        bool walked = false;
        {
            /*
             * Its overlay's shards are read with their source chain, the space's own, so
             * in a space with no Raft a commit holding every shard of it holds this up
             * too. Said as a wait on that commit while it reads, which is all it's doing
             * then - TODO 720.
             */
            barch::tx_waiting_scope waiting(space->has_raft() ? nullptr : status,
                                            "the shards of " + space->get_canonical_name()
                                            + " while it reads its own writes", "", true);
            walked = overlay_writes(overlay, space, reads.get(), writes, why);
        }
        if (!walked)
            return refused(call, out, ("ERR " + why).c_str());
        if (writes.empty() && (!reads || (reads->size() == 0 && reads->range_count() == 0)))
            return committed(call, out);
        /*
         * It read its snapshot, which is the store as some moment saw it, and it writes
         * nothing: it happened then, and there's nothing to check - TODO 701.
         */
        if (writes.empty() && reads && reads->versions && !space->has_raft())
            return committed(call, out);
        if (!space->has_raft())
            return commit_here(call, space, id, writes, *reads, out, status);
        /*
         * A Raft space - TODO 701. Its snapshot is this node's, so a change this node has
         * applied since is in its layers: a conflict found here needs no Raft step. The
         * groups' prepares still decide, since this node may not have the latest. A read
         * only transaction still asks them: a follower's snapshot can be behind its
         * leader's, and its reads would be stale.
         */
        static const bool no_local_check = std::getenv("BARCH_TEST_TX_NO_LOCAL_CHECK") != nullptr;
        if (reads && reads->versions && !no_local_check) {
            bool changed = reads->versions->clears_now() != reads->clears_at;
            reads->each([&](const std::string& key, uint32_t shard, uint64_t) {
                if (!changed && reads->versions->changed_since(reads->snapshot, shard,
                                                               art::value_type{key.data(), key.size()}))
                    changed = true;
            });
            reads->each_range([&](art::value_type lo, art::value_type hi) {
                if (!changed && reads->versions->changed_in(reads->snapshot, lo, hi))
                    changed = true;
            });
            if (changed)
                return refused(call, out, "ERR TXCONFLICT a key the transaction read has changed since it was read");
        }
        // the last write to each key, in the order the keys were last written, by group;
        // and the keys read from the store, each in its own shard's group - TODO 695
        struct part {
            barch::cluster::binding_ptr via;
            std::vector<std::string> records;
            std::vector<std::string> reads;
            // the keys it locks once prepared, as INFO transactions shows them - TODO 720.
            // Each once: a key written was read first, so it's in both lists
            std::vector<std::string> keys;
            std::set<std::string> noted;
            void note(const std::string& key) {
                if (!noted.insert(key).second)
                    return;
                if (keys.size() < barch::tx_status::held_shown)    // as the client wrote it
                    keys.push_back(encoded_key_as_string(art::value_type(key.data(), key.size())));
            }
        };
        std::map<uint32_t, part> parts;
        {
            const auto groups_now = space->raft_groups();
            auto owner_of = [&](uint32_t shard) {
                barch::cluster::binding_ptr owner;
                for (const auto& g : groups_now)
                    if (g->owns_shard(shard))
                        owner = g;
                return owner;
            };
            for (auto [at, record] : writes) {
                barch::aof::record r;
                if (barch::aof::decode((const uint8_t*) record.data(), (uint32_t) record.size(), r)
                    != barch::aof::decoded::ok)
                    return refused(call, out, "ERR a transaction's write that doesn't decode");
                auto owner = owner_of(r.shard);
                if (!owner)
                    return refused(call, out, "ERR a transaction's write that's in no raft group");
                auto& p = parts[owner->group_number()];
                p.via = owner;
                p.note(r.key);
                p.records.push_back(std::move(record));
            }
            bool lost = false;
            reads->each([&](const std::string& key, uint32_t shard, uint64_t hash) {
                auto owner = owner_of(shard);
                if (!owner) {
                    lost = true;
                    return;
                }
                auto& p = parts[owner->group_number()];
                p.via = owner;
                p.note(key);
                p.reads.push_back(barch::tx_read_word(shard, space->space_name(), key, hash));
            });
            if (lost)
                return refused(call, out, "ERR a key the transaction read is in no raft group");
            /*
             * Each range it walked, as a word per shard, with the hash of what the shard
             * held in it as the snapshot saw it: the shard's group compares that with what
             * it holds, and keeps the range locked while the transaction is pending - TODO
             * 701. Every shard, since a hashed space has keys of any range in each.
             */
            reads->each_range([&](art::value_type lo, art::value_type hi) {
                const auto packed = barch::tx_range_key(lo, hi);
                for (uint32_t shard = 0; shard < (uint32_t) space->get_shard_count(); ++shard) {
                    auto owner = owner_of(shard);
                    if (!owner) {
                        lost = true;
                        return;
                    }
                    auto base = space->get(shard);
                    uint64_t hash = 0;
                    {
                        // the base shard held holds its layers too, which its writers write
                        read_lock hold(base);
                        heap::vector<barch::shard_ptr> levels;
                        for (auto& l : reads->versions ? reads->versions->layers_from(reads->snapshot, shard)
                                                       : heap::vector<barch::shard_ptr>{})
                            levels.push_back(std::move(l));
                        for (auto t = base; t; t = t->sources())
                            levels.push_back(t);
                        hash = barch::tx_range_hash(levels, lo, hi);
                    }
                    auto& p = parts[owner->group_number()];
                    p.via = owner;
                    p.reads.push_back(barch::tx_read_word(shard | barch::tx_range_bit, space->space_name(),
                                                          packed, hash));
                }
            });
            if (lost)
                return refused(call, out, "ERR a range the transaction walked is in no raft group");
        }
        std::string groups;
        for (const auto& [n, part] : parts)
            groups += (groups.empty() ? "" : ",") + std::to_string(n);
        auto& home = parts.begin()->second.via;
        // what a step answered, as an error for the client, or empty
        auto step = [&](const barch::cluster::binding_ptr& g, int op, const std::vector<std::string>& words) {
            std::string answer, why;
            if (g->commit_tx(op, words, answer, why) != barch::cluster::binding::result::committed)
                return why.empty() ? std::string("ERR the transaction's step wasn't written") : why;
            return answer.empty() ? std::string() : "ERR " + answer;
        };
        /*
         * An abort, tried for a couple of seconds: one that doesn't land (a leader moving)
         * leaves the keys locked until the group's leader settles it, five seconds on.
         * The home is always first in `prepared`.
         */
        auto abort_in = [&](const barch::cluster::binding_ptr& g) {
            for (int tries = 0; tries < 20; ++tries) {
                std::string answer, why;
                if (g->commit_tx(barch::cluster::tx_abort, {id}, answer, why)
                    == barch::cluster::binding::result::committed)
                    return;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        };
        // test knobs: a pause between prepare and commit, and between the home's commit and
        // the rest, for a test to kill the coordinator in
        static const long pause_ms = [] {
            const char* v = std::getenv("BARCH_TEST_TX_PAUSE_MS");
            return v ? std::atol(v) : 0L;
        }();
        static const long spread_pause_ms = [] {
            const char* v = std::getenv("BARCH_TEST_TX_SPREAD_PAUSE_MS");
            return v ? std::atol(v) : 0L;
        }();
        // prepare: the home first, so a group prepared anywhere means its home was asked
        std::vector<barch::cluster::binding_ptr> prepared;
        for (auto& [n, p] : parts) {
            // id, its groups, how many records, the records, then the reads
            std::vector<std::string> words{id, groups, std::to_string(p.records.size())};
            words.insert(words.end(), std::make_move_iterator(p.records.begin()),
                         std::make_move_iterator(p.records.end()));
            words.insert(words.end(), std::make_move_iterator(p.reads.begin()), std::make_move_iterator(p.reads.end()));
            /*
             * A key another transaction holds - TODO 696, choice 4: try again for about
             * 50 ms, then push it aside if this one is older or it's gone quiet, and try
             * once more. Waiting can't deadlock: a group's prepare takes all its locks or
             * none, and every transaction prepares its groups lowest first.
             */
            std::string said;
            {
                const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
                bool pushed = false;
                std::optional<barch::tx_waiting_scope> waiting;
                for (;;) {
                    said = step(p.via, barch::cluster::tx_prepare, words);
                    std::smatch m;
                    static const std::regex in_way(R"(transaction (\S+) home (\d+))");
                    if (said.rfind("ERR TXCONFLICT", 0) != 0 || !std::regex_search(said, m, in_way))
                        break;
                    // a key in this group another holds - TODO 720
                    if (!waiting)
                        waiting.emplace(status, "a key in group " + std::to_string(n), m[1].str());
                    if (std::chrono::steady_clock::now() < until) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                        continue;
                    }
                    if (pushed)
                        break;
                    std::string why;
                    pushed = true;
                    if (!barch::cluster::push_tx((uint32_t) std::stoul(m[2].str()), n, m[1].str(), id, why))
                        break;
                }
            }
            prepared.push_back(p.via);
            if (said.empty() && status) {
                // its keys in this group are locked now, on every member
                std::lock_guard l(status->m);
                for (auto& k : p.keys)
                    if (status->held.size() < barch::tx_status::held_shown)
                        status->held.push_back(std::move(k));
                status->holds += p.noted.size();
                if (reads && reads->range_count())
                    status->holds_what = std::to_string(reads->range_count()) + " ranges walked";
            }
            if (!said.empty()) {
                /*
                 * Aborted at home first: that decides it, whatever the others hear. This
                 * group too: a read that changed is only an answer, and its locks were
                 * taken on every member all the same - TODO 695. One refused for a held
                 * key took none, and the abort leaves it an aborted id, which is harmless.
                 */
                for (const auto& g : prepared)
                    abort_in(g);
                return refused(call, out, said.c_str());
            }
        }
        if (pause_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
        // the home's commit is the transaction's: from here it has happened
        if (auto said = step(home, barch::cluster::tx_commit, {id}); !said.empty()) {
            if (said.rfind("ERR TXABORTED", 0) == 0)
                for (size_t k = 1; k < prepared.size(); ++k)
                    abort_in(prepared[k]);
            return refused(call, out, said.c_str());
        }
        if (prepared.size() > 1 && spread_pause_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(spread_pause_ms));
        // a group that doesn't take it now gets it from its leader, which asks the home
        for (size_t k = 1; k < prepared.size(); ++k)
            step(prepared[k], barch::cluster::tx_commit, {id});
        return committed(call, out);
    }
}

void register_cluster_api(function_map& r) {
    r["CLUSTER"] = {::CLUSTER, {"write", "dangerous"}};
    r["BEGINTX"] = {::BEGINTX, {"write", "connection"}};
    r["ABORTTX"] = {::ABORTTX, {"write", "connection"}};
    r["COMMITTX"] = {::COMMITTX, {"write", "data"}};
}
