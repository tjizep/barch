//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "repl_api.h"
#include "replace_file.h"
#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>
#include <fstream>
#include <set>
#include <sstream>

#include "barch_apis.h"
#include "caller.h"
#include "vk_caller.h"
#include "module.h"
#include "conversion.h"
#include "version.h"
#include "glob.h"
#include "function_sync.h"
#include "http_api.h"
#include "keys.h"
#include "art/art.h"
#include "art/iterator.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "sharded_store.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"
#include "statistics.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "auth_api.h"
#include "rpc/server.h"
#include "rpc/restarter.h"
#include "rpc/redis_parser.h"
#include "hash_arena.h"
#include "memory_limit.h"
#include "data_dir.h"

extern "C" {
#include "../external/include/valkeymodule.h"
}

extern "C" {

static restarter restart;
}

void barch::stop_repl_restarts() {
    restart.shutdown();
}

extern "C" {

/* B.LOAD
 * loads and overwrites the data from files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int LOAD(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    std::atomic<size_t> errors = 0;
    auto ks = call.kspace();
    barch::sharded_store store(ks);
    const auto& change_log = ks->get_change_log();
    uint64_t loaded_at = 0;
    {
        /*
         * Every shard locked, whatever the sharding, and each cleared before it
         * loads - TODO 479. After a LOAD the files are the state, so the change
         * log gets a checkpoint at this moment and a replay starts from the files
         * again. That's only true with nothing half written when the mark is
         * taken, and with nothing kept that the files don't have. A range sweep
         * is held off too, and the range table is rebuilt before the lock drops,
         * because it is nothing but each shard's first key.
         */
        barch::sharded_store::write_guard held = store.lock_space_write();
        loaded_at = change_log ? change_log->mark() : 0;
        // the dictionary is a key in the files - TODO 527 - so it's whatever they hold
        dictionary::files_replaced(ks->get_name());
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->load_holding_lock()) ++errors;
        });
        if (ks->is_range_sharded()) {
            ks->routes().rebuild(store.shards());
        }
        dictionary::load_holding_lock(ks);
    }
    /*
     * Not after a shard failed: what it holds now isn't its file, so the
     * checkpoint would be a claim the files can't back up.
     */
    if (errors == 0 && change_log) {
        try {
            change_log->checkpoint(ks->space_name(), loaded_at);
            change_log->trim_to_last_checkpoint();
        } catch (const std::exception& e) {
            barch::err({"loaded, but could not checkpoint the change log:", e.what()});
        }
    }
    // what's in memory is the files now, which can be behind what this copy had
    // applied from a primary - TODO 502
    barch::repl::positions_loaded(ks->get_canonical_name());
    return errors>0 ? call.push_error("some shards did not load") : call.push_simple("OK");
}
int cmd_LOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LOAD);
}
int RELOAD(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    std::atomic<size_t> errors = 0;
    barch::sharded_store store(call.kspace());
    // freeze only when the partition is state. a range sweep needs two write
    // locks to move a key, so it waits, and a key cannot leave a shard that
    // has already been replaced from disk for one that has not. hash sharding
    // reloads under each shard's own latch. the range table is rebuilt before
    // the space lock drops
    barch::sharded_store::write_guard held;
    if (call.kspace()->is_stateful_sharding()) {
        held = store.lock_space_write();
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->reload_holding_lock()) ++errors;
        });
        if (call.kspace()->is_range_sharded()) {
            // the routing table describes the shards, and the shards were just
            // replaced by what was on disk. rebuilding is what a load does
            // anyway - the table is never written down, only derived
            call.kspace()->routes().rebuild(store.shards());
        }
    } else {
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->reload()) ++errors;
        });
    }
    return errors>0 ? call.push_error("some shards did not reload") : call.push_simple("OK");
}
int START(caller& call, const arg_t& argv) {
    if (argv.size() > 5 || argv.size() < 3)
        return call.wrong_arity();
    if (argv.size() >= 4 && argv[3] != "SSL") {
        return call.push_error("invalid argument");
    }
    if (argv.size() == 5 && argv[4] != "ASYNCH") {
        return call.push_error("invalid argument");
    };
    auto interface = argv[1];
    auto port = conversion::as_variable(argv[2]).ui();
    bool ssl = argv.size() == 4 && argv[3] == "SSL";
    bool async = argv.size() == 5 && argv[4] == "ASYNCH";
    if (call.is_remote()) async = true;
    if (async) {
        restart.asynch_restart(interface.chars(), port, ssl);
    } else {
        auto failed = restart.inline_restart(interface.chars(), port, ssl);
        if (!failed.empty())
            return call.push_error(("could not listen: " + failed).c_str());
    }
    return call.push_simple("OK");
}
int cmd_START(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, START);
}
int PUBLISH(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    barch::repl::publish(interface.s(), port.i());
    return call.push_simple("OK");
}
int cmd_PUBLISH(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PUBLISH);
}
int PULL(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    auto ks = call.kspace();
    barch::sharded_store store(call.kspace());
    store.each_shard([&](const barch::shard_ptr& t) {
        if (!t->pull(interface.s(), port.i())) {}
    });
    return call.push_simple("OK");
}
int cmd_PULL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PULL);
}
int STOP(caller& call, const arg_t& ) {
    if (call.get_context() == ctx_resp) {
        return call.push_error("Cannot stop server");
    }
    barch::stop_function_sync();
    barch::stop_http_servers();
    if (call.is_remote()) {
        restart.asynch_stop();
    }else {
        barch::server::stop();
    }

    return call.push_simple("OK");
}
int cmd_STOP(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, STOP);
}
/* RPING <host> <port>
 *
 * reach out to another barch and check it answers. This was called PING until the name
 * was given back to redis's health check, which is what every client sends and what a
 * connection pool sends before handing a connection out. The two are unrelated: this
 * one opens a connection to somewhere else, over the binary replication protocol, and
 * says nothing about the server being asked.
 */
/*
 * The replica's side of TODO 498: records another node's shards made, applied the
 * way a change log replay applies them.
 *
 *     REPLAPPLY <origin> <first sequence> <record>...
 *
 * Only over the barch rpc port, since a record names its own space and so goes round
 * the space rights a client would be checked against. Nothing applied here is
 * recorded for replication again - a replica isn't a relay, and two nodes publishing
 * to each other would pass writes back and forth for good.
 *
 * What a batch is allowed to do - TODO 502. The origin is `<node>/<incarnation>`,
 * with `/fresh` on the first batch after a PUBLISH. The node is the primary's own id,
 * kept in a file beside its data, and the incarnation is new each time it starts, so
 * a restart shows up here as a new incarnation of a node already seen. For each node
 * this keeps the incarnation it's following and the last sequence applied from it.
 *
 *   - Records at or below that are skipped: a batch sent again after a lost reply.
 *   - A batch that starts past the next one, or a new incarnation, or a node there's
 *     no usable position for, is refused with NEEDSYNC and nothing is applied. Each
 *     means writes this copy doesn't have. The sender stops and says the replica
 *     needs a full copy. It used to be logged here and applied anyway, so the copy
 *     looked right and wasn't.
 *   - A fresh batch from a node not being followed starts following it, the way
 *     the first PUBLISH to a new replica always did.
 *   - Any refusal leaves that primary refused until every space it writes here has
 *     been retrieved from it, and then a fresh batch from it starts over - TODO
 *     504. A refused primary drops what it had queued, for all its spaces, so
 *     starting over any sooner would go on without those writes. RETRIEVE and
 *     LOAD of a space touch only the primary that space takes writes from; any
 *     other primary's stream carries on.
 *   - A space takes writes from one primary. A record for a space another primary
 *     writes here is refused (REPLFAILED), since the two can't both be copied.
 *   - The first record that can't be applied stops the batch with REPLFAILED, and
 *     the position stays before it. It used to be skipped for good.
 *   - A container entry this space can't place where the primary did - another
 *     shard count, or range routing there and hash routing here - stops the batch
 *     the same way. It used to be stored where no read would find it - TODO 503.
 *   - An empty record holds the place of a plain command, which the primary sends
 *     on its own once this has taken the number. Those went round every check.
 *
 * Positions last across a restart, in `repl_positions.dat`, only as far as the
 * shard files are known to hold them: written when a node is first followed, and
 * after a SAVEALL with the positions taken before it started. Which spaces each
 * primary writes, and what it's still waiting on, are written as they change. A replica killed
 * between two of those comes back behind what the primary thinks it sent, so the
 * next batch is a gap and it's refused, rather than carrying on over lost writes.
 *
 * Answers with how many records were applied.
 */
namespace {
    // in the data directory, not the working one - TODO 526. Worked out on first
    // use rather than at start, which is before barchd has moved to --dir
    const std::string& positions_file() {
        static const auto* path = new std::string(barch::data_path("repl_positions.dat"));
        return *path;          // never destroyed, like the rest a shutdown reads - TODO 57
    }

    /*
     * What this replica knows about one primary - TODO 502, TODO 504.
     *
     * A space takes writes from one primary here. The first record for a space
     * claims it, and a record from another primary for it is refused: two
     * primaries writing one space can't both be copied, and a RETRIEVE of it
     * would put one's writes over the other's.
     *
     * Once a primary is refused anything, it has dropped what it had queued for
     * this replica, so every space it writes here may be missing writes. It
     * stays refused until each of them has been retrieved from it (`pending`),
     * and only then may a fresh PUBLISH from it start over. A RETRIEVE touches
     * only the space's own primary, so another primary's stream carries on.
     */
    struct position {
        std::string incarnation{};
        uint64_t last{0};                   // the last sequence applied
        uint64_t durable{0};                // what the shard files are known to hold
        bool valid{false};
        std::set<std::string> spaces{};     // the spaces it writes here
        std::set<std::string> pending{};    // while not valid: still to RETRIEVE from it
        // retrieved from it since it was last refused, with the last of its
        // sequences the copy is known to hold
        std::map<std::string, uint64_t> retrieved{};
        // the incarnation those copies were taken from, and the least of their
        // unpublished counts: writes it made with nothing published, after a
        // copy, are in no stream - TODO 505
        std::string copies_incarnation{};
        uint64_t copy_unpublished{UINT64_MAX};
        std::string held_said{};            // the last HOLD reason logged
    };

    std::mutex repl_seen_lock;
    std::map<std::string, position> repl_seen;       // node -> what's known of it
    bool positions_read{false};
    // spaces a RETRIEVE is copying right now: batches that touch one are held,
    // so nothing lands in a space the copy is about to replace - TODO 505
    std::map<std::string, int> copying;

    // the file keeps an empty set as "-", so it's still a field
    std::string join(const std::set<std::string>& names) {
        return names.empty() ? std::string("-") : barch::repl::encode_spaces(names);
    }
    std::set<std::string> split(const std::string& field) {
        return barch::repl::decode_spaces(field);
    }

    void read_positions_locked() {
        if (positions_read)
            return;
        positions_read = true;
        std::ifstream in(positions_file());
        if (!in)
            return;
        std::string head;
        if (!std::getline(in, head) || head != "barch-repl-positions 3") {
            barch::err({positions_file(), "isn't a replication positions file this build"
                        " knows. Every primary will need a full copy (RETRIEVE)"});
            return;
        }
        std::string node, incarnation, spaces, pending, retrieved, copies_inc;
        uint64_t durable = 0, copy_unpublished = UINT64_MAX;
        int valid = 0;
        while (in >> node >> incarnation >> durable >> valid >> spaces >> pending >> retrieved
                  >> copies_inc >> copy_unpublished) {
            position p;
            p.incarnation = incarnation;
            p.last = p.durable = durable;
            p.valid = valid != 0;
            p.spaces = split(spaces);
            p.pending = split(pending);
            p.retrieved = barch::repl::decode_marks(retrieved);
            p.copies_incarnation = copies_inc == "-" ? std::string() : copies_inc;
            p.copy_unpublished = copy_unpublished;
            repl_seen[node] = p;
        }
    }

    bool write_positions_locked() {
        const std::string tmp = positions_file() + ".tmp";
        {
            std::ofstream out(tmp, std::ios::trunc);
            out << "barch-repl-positions 3\n";
            for (const auto& [node, p] : repl_seen)
                out << node << ' ' << p.incarnation << ' ' << p.durable << ' ' << (p.valid ? 1 : 0)
                    << ' ' << join(p.spaces) << ' ' << join(p.pending) << ' '
                    << (p.retrieved.empty() ? std::string("-") : barch::repl::encode_marks(p.retrieved))
                    << ' ' << (p.copies_incarnation.empty() ? std::string("-") : p.copies_incarnation)
                    << ' ' << p.copy_unpublished << '\n';
            out.flush();
            if (!out) {
                barch::err({"could not write", tmp});
                return false;
            }
        }
        if (!arena::sync_file(tmp) || barch::replace_file(tmp.c_str(), positions_file().c_str()) != 0
            || !arena::sync_dir_of(positions_file())) {
            barch::err({"could not put", positions_file(), "in place"});
            return false;
        }
        return true;
    }

    /** the primary a space takes writes from here, or null */
    const std::string* owner_of_locked(const std::string& space) {
        for (const auto& [node, p] : repl_seen)
            if (p.spaces.contains(space))
                return &node;
        return nullptr;
    }

    /** refused: every space it writes needs a copy from it before it starts over */
    void take_back_locked(position& p) {
        if (!p.valid)
            return;
        p.valid = false;
        p.pending = p.spaces;
        p.retrieved.clear();
        p.copies_incarnation.clear();
        p.copy_unpublished = UINT64_MAX;
    }

    /**
     * A copy of `space` from this primary, holding its writes up to `seq`, taken
     * from incarnation `inc` with `unpublished` writes counted - TODO 504, 505.
     * Copies from another incarnation than the ones before don't add up with
     * them, so those spaces are needed again.
     */
    void add_copy_locked(position& p, const std::string& space, const std::string& inc,
                         uint64_t seq, uint64_t unpublished) {
        if (p.copies_incarnation != inc) {
            for (const auto& [s, c] : p.retrieved)
                if (p.spaces.contains(s)) p.pending.insert(s);
            p.retrieved.clear();
            p.copies_incarnation = inc;
            p.copy_unpublished = UINT64_MAX;
        }
        p.pending.erase(space);
        p.retrieved[space] = seq;
        p.copy_unpublished = std::min(p.copy_unpublished, unpublished);
    }

    std::string names(const std::set<std::string>& spaces) {
        std::string out;
        for (const auto& s : spaces) {
            if (!out.empty()) out += ", ";
            out += s.empty() ? "the default space" : s;
        }
        return out;
    }

    /** a space as a record names it, by the name RETRIEVE and LOAD know it by */
    std::string canonical_space(const std::string& recorded) {
        std::string name = barch::ks_undecorate(recorded);
        if (name.empty()) name = "0";       // the default space undecorates to nothing
        const auto ks = barch::get_keyspace(name);
        return ks ? ks->get_canonical_name() : name;
    }
    std::string space_of(const barch::aof::record& r) {
        return canonical_space(r.space);
    }

    bool apply_record(const barch::aof::record& r, std::string& why) {
        std::string name = barch::ks_undecorate(r.space);
        if (name.empty()) name = "0";       // the default space undecorates to nothing
        auto ks = barch::get_keyspace(name);
        barch::sharded_store store(ks);
        if (r.type == barch::aof::record_type::clear) {
            store.clear_space();
            return true;
        }
        if (r.type != barch::aof::record_type::set && r.type != barch::aof::record_type::erase) {
            why = "a record that isn't a write";
            return false;
        }
        const art::value_type key{r.key.data(), (unsigned) r.key.size()};
        for (;;) {
            barch::key_space::placed how;
            const size_t at = ks->place(r, how);
            if (at >= ks->get_shards().size()) {
                why = "a record naming a shard this space doesn't have";
                return false;
            }
            /*
             * A container's entry that can't go where the primary put it - TODO
             * 503. It lives on the shard its container's *name* routes to, and the
             * name can't be had back from the entry's key, so routing the entry by
             * itself stores it where no read will look. That happens here when the
             * primary had another shard count, or routed by range while this space
             * hashes. A plain key is routed by itself and is right either way.
             */
            if (how == barch::key_space::placed::rerouted && !r.key.empty()) {
                const auto lead = (uint8_t) r.key[0];
                if (lead == art::tcomposite || lead == art::tcomposite_list
                    || lead == art::tcomposite_hash || lead == art::tcomposite_ordered_map) {
                    why = "a list, hash or ordered set entry for space " + name + ", which is "
                          + std::to_string(ks->get_shards().size()) + " shards hashed here and was "
                          + std::to_string(r.shard_count) + " shards "
                          + (r.routing == barch::aof::routing_range ? "by range" : "hashed")
                          + " on the primary. Give the replica's space the primary's layout";
                    return false;
                }
            }
            auto t = ks->get(at);
            storage_release held(t);
            // a recorded placement is kept whatever the key routes to, since a
            // container entry lives where its container's name routes. A key placed
            // by itself is routed again under the lock, as route_locked does
            if (how != barch::key_space::placed::recorded && ks->route_moved(key, t))
                continue;
            if (r.type == barch::aof::record_type::set) {
                const art::key_options opts(r.options, (uint64_t) r.expiry_ms);
                const art::value_type value{r.value.data(), (unsigned) r.value.size()};
                t->insert(opts, key, value, true, [](const art::node_ptr&) {});
            } else {
                t->remove(key, [](const art::node_ptr&) {});
            }
            return true;
        }
    }
}

} // extern "C"

namespace barch::repl {
    bool apply_one(const aof::record& r, std::string& why) {
        return apply_record(r, why);
    }
    std::string encode_spaces(const std::set<std::string>& spaces) {
        static const char* d = "0123456789abcdef";
        std::string out;
        for (const auto& s : spaces) {
            if (!out.empty()) out += ',';
            out += 'x';
            for (unsigned char c : s) {
                out += d[c >> 4];
                out += d[c & 15];
            }
        }
        return out;
    }
    std::string encode_marks(const std::map<std::string, uint64_t>& marks) {
        std::string out;
        for (const auto& [space, seq] : marks) {
            if (!out.empty()) out += ',';
            out += encode_spaces({space}) + "@" + std::to_string(seq);
        }
        return out;
    }
    std::map<std::string, uint64_t> decode_marks(const std::string& field) {
        std::map<std::string, uint64_t> out;
        if (field.empty() || field == "-")
            return out;
        size_t at = 0;
        while (at <= field.size()) {
            const size_t comma = std::min(field.find(',', at), field.size());
            const std::string item = field.substr(at, comma - at);
            const size_t mark = item.find('@');
            const auto names = decode_spaces(item.substr(0, mark));
            const uint64_t seq = mark == std::string::npos ? 0 : std::stoull(item.substr(mark + 1));
            for (const auto& n : names)
                out[n] = seq;
            at = comma + 1;
        }
        return out;
    }
    std::set<std::string> decode_spaces(const std::string& field) {
        std::set<std::string> out;
        if (field.empty() || field == "-")
            return out;
        size_t at = 0;
        while (at <= field.size()) {
            const size_t comma = std::min(field.find(',', at), field.size());
            std::string name;
            for (size_t i = at + 1; i + 2 <= comma; i += 2)
                name += (char) std::stoi(field.substr(i, 2), nullptr, 16);
            out.insert(name);
            at = comma + 1;
        }
        return out;
    }

    std::map<std::string, uint64_t> take_positions() {
        std::lock_guard l(repl_seen_lock);
        read_positions_locked();
        std::map<std::string, uint64_t> out;
        for (const auto& [node, p] : repl_seen) {
            if (p.valid)
                out[node + "/" + p.incarnation] = p.last;
        }
        return out;
    }

    void positions_saved(const std::map<std::string, uint64_t>& taken) {
        std::lock_guard l(repl_seen_lock);
        read_positions_locked();
        bool moved = false;
        for (auto& [node, p] : repl_seen) {
            auto at = taken.find(node + "/" + p.incarnation);
            if (p.valid && at != taken.end() && at->second > p.durable) {
                p.durable = at->second;
                moved = true;
            }
        }
        if (moved)
            write_positions_locked();
    }

    void positions_loaded(const std::string& space) {
        std::lock_guard l(repl_seen_lock);
        read_positions_locked();
        const std::string* owner = owner_of_locked(space);
        if (!owner)
            return;
        auto& p = repl_seen[*owner];
        take_back_locked(p);
        // a LOAD can take the space back past what a RETRIEVE brought
        p.pending.insert(space);
        p.retrieved.erase(space);
        write_positions_locked();
        barch::log({"LOAD of", names({space}), "- it may be behind what primary", *owner, "sent, so"
                    " that primary is held until", names(p.pending), "are retrieved from it"});
    }

    void positions_copy_begin(const std::string& space) {
        std::lock_guard l(repl_seen_lock);
        ++copying[space];
    }

    void positions_copy_end(const std::string& space) {
        std::lock_guard l(repl_seen_lock);
        if (auto at = copying.find(space); at != copying.end() && --at->second <= 0)
            copying.erase(at);
    }

    void positions_retrieved(const std::string& space, const std::string& source,
                             const std::string& incarnation, uint64_t copy_seq,
                             uint64_t unpublished) {
        std::lock_guard l(repl_seen_lock);
        read_positions_locked();
        const std::string* owner = owner_of_locked(space);
        const std::string was = owner ? *owner : std::string();
        if (!was.empty() && was != source) {
            // another node's copy now, so it isn't this primary's space any more
            auto& p = repl_seen[was];
            take_back_locked(p);
            p.spaces.erase(space);
            p.pending.erase(space);
            p.retrieved.erase(space);
        }
        if (source.empty()) {
            write_positions_locked();
            return;
        }
        /*
         * The source's space from here - TODO 504, 505. A primary this follows,
         * whose stream is going and from the incarnation the copy came from,
         * carries on as it was: batches touching the space were held while the
         * copy ran, so nothing it applied is missing from the copy. Otherwise the
         * copy is one of those a held stream is waiting on. A node never seen is
         * written down, so its first stream is checked against the copy.
         */
        auto& p = repl_seen[source];
        if (p.incarnation.empty())
            p.incarnation = incarnation;
        p.spaces.insert(space);
        if (!(p.valid && p.incarnation == incarnation)) {
            take_back_locked(p);
            add_copy_locked(p, space, incarnation, copy_seq, unpublished);
        }
        write_positions_locked();
        if (p.valid)
            barch::log({"RETRIEVE of", names({space}), "- primary", source, "carries on"});
        else if (p.pending.empty())
            barch::log({"RETRIEVE of", names({space}), "- primary", source, "can go on once its"
                        " stream starts after this copy"});
        else
            barch::log({"RETRIEVE of", names({space}), "- primary", source, "is held until",
                        names(p.pending), "are retrieved from it"});
    }
}

extern "C" {

int REPLAPPLY(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    if (call.get_context() != ctx_rpc)
        return call.push_error("REPLAPPLY is only taken from another barch's replication");
    const std::string origin(argv[1].chars(), argv[1].size);
    const auto first = conversion::as_variable(argv[2]).i();
    if (first <= 0)
        return call.push_error("REPLAPPLY needs a sequence above 0");

    // `<node>/<incarnation>[/fresh]`. A bare origin is a primary from before TODO 502:
    // one incarnation, and fresh whenever it hasn't been seen.
    // `/fresh@<n>` is the primary's count of writes made with nothing published, as it
    // was at PUBLISH (TODO 505), and `:<spaces>` after it names the spaces it has
    // writes for that this replica was never sent (TODO 504)
    std::string node = origin, incarnation = origin;
    bool fresh = false, legacy = true, says_unpublished = false;
    uint64_t unpublished_at = 0;
    std::map<std::string, uint64_t> dropped;
    if (const auto slash = origin.find('/'); slash != std::string::npos) {
        legacy = false;
        node = origin.substr(0, slash);
        incarnation = origin.substr(slash + 1);
        if (const auto tail = incarnation.find('/'); tail != std::string::npos) {
            const std::string rest = incarnation.substr(tail + 1);
            fresh = rest.rfind("fresh", 0) == 0;
            size_t at = 5;
            if (fresh && at < rest.size() && rest[at] == '@') {
                const size_t colon = std::min(rest.find(':', at), rest.size());
                unpublished_at = std::stoull(rest.substr(at + 1, colon - at - 1));
                says_unpublished = true;
                at = colon;
            }
            if (fresh && at + 1 < rest.size() && rest[at] == ':')
                dropped = barch::repl::decode_marks(rest.substr(at + 1));
            incarnation.resize(tail);
        }
    }

    // one origin's batches arrive in order on one connection, but two origins, or a
    // reconnect racing the old session, can overlap - so the check and the apply
    // are one step
    std::lock_guard l(repl_seen_lock);
    read_positions_locked();
    // a space a RETRIEVE is copying takes nothing until the copy is in - TODO 505
    if (!copying.empty()) {
        for (size_t i = 3; i < argv.size(); ++i) {
            barch::aof::record r;
            if (argv[i].size == 0
                || barch::aof::decode((const uint8_t*) argv[i].bytes, argv[i].size, r) != barch::aof::decoded::ok)
                continue;
            const std::string space = space_of(r);
            if (copying.contains(space))
                return call.push_error(("HOLD a RETRIEVE of " + names({space}) + " is under way here").c_str());
        }
    }
    auto seen = repl_seen.find(node);
    const bool known = seen != repl_seen.end();
    /*
     * The spaces the primary says it has writes for that never came here join
     * what it's waiting on, unless a copy retrieved from it holds the last of
     * them - TODO 504. A copy taken before that write doesn't. A space another
     * primary writes here is left out: its writes to that space were refused,
     * not lost.
     */
    const auto take_dropped = [&](position& p) {
        bool changed = false;
        for (const auto& [d, last_dropped] : dropped) {
            const std::string space = canonical_space(d);
            const std::string* owner = owner_of_locked(space);
            if (owner && *owner != node)
                continue;
            if (auto copy = p.retrieved.find(space);
                copy != p.retrieved.end() && copy->second >= last_dropped)
                continue;
            changed = p.pending.insert(space).second || changed;
        }
        return changed;
    };
    // refused: the primary drops what it had queued for this replica, so every
    // space it writes here needs a copy from it before it starts over - TODO 504
    const auto refuse = [&](const std::string& kind, const std::string& why) {
        if (known) {
            take_back_locked(seen->second);
            take_dropped(seen->second);
            write_positions_locked();
        }
        std::string todo;
        if (known && !seen->second.pending.empty())
            todo = " - RETRIEVE " + names(seen->second.pending) + " from it here, then PUBLISH again";
        else
            todo = " - this copy needs a full copy (RETRIEVE), then PUBLISH again on the primary";
        barch::err({"replication from", node, "refused:", why + todo});
        return call.push_error((kind + " " + why + todo).c_str());
    };
    /*
     * Held: nothing applied, and the primary keeps what it has queued and asks
     * again - TODO 505. So a copy retrieved while it waits lines up with the
     * stream: the stream starts at `first`, and a copy taken after that was
     * numbered holds everything before it.
     */
    const auto hold = [&](position& p, const std::string& why) {
        const std::string text = why + " - RETRIEVE " + names(p.pending)
                                 + " from it here; its stream is held until then";
        if (p.held_said != text) {
            p.held_said = text;
            barch::log({"replication from", node, "held:", text});
        }
        return call.push_error(("HOLD " + text).c_str());
    };
    if (legacy && !known)
        fresh = true;
    if (!known) {
        if (!fresh)
            return refuse("NEEDSYNC", "no position for this primary here, so writes before this batch may be missing");
        /*
         * Following this stream from here. Written down before anything is
         * applied, so a replica killed partway is known to have been following it
         * and comes back holding, rather than taking a fresh start from nothing.
         * The position is the one before the batch: it's a new replica.
         */
        auto& p = repl_seen[node];
        p.incarnation = incarnation;
        p.last = p.durable = (uint64_t) first - 1;
        p.valid = true;
        if (!write_positions_locked())
            return call.push_error("REPLFAILED could not write the replication positions file");
        seen = repl_seen.find(node);
    } else {
        auto& p = seen->second;
        const bool continues = p.valid && p.incarnation == incarnation && (uint64_t) first <= p.last + 1;
        if (!continues) {
            if (p.valid && !fresh && p.incarnation != incarnation)
                return refuse("NEEDSYNC", "a batch from another incarnation that isn't a fresh start");
            /*
             * A gap, a restarted primary, or one this replica already waits on.
             * Its stream carries on from `first` once every space it writes here
             * has a copy from it that holds everything before `first` - TODO 504,
             * 505. Until then it's held, not refused, so nothing is dropped.
             */
            take_back_locked(p);
            bool changed = fresh && take_dropped(p);
            const auto again = [&](const std::string& space) {
                if (p.spaces.contains(space) || p.retrieved.contains(space))
                    p.pending.insert(space);
                changed = true;
            };
            if (!p.retrieved.empty() && p.copies_incarnation != incarnation) {
                // copies from another incarnation say nothing about this stream
                for (const auto& [s, c] : p.retrieved) again(s);
                p.retrieved.clear();
                p.copy_unpublished = UINT64_MAX;
            }
            for (auto it = p.retrieved.begin(); it != p.retrieved.end();) {
                // a copy older than where this stream starts misses what's between
                if (it->second + 1 < (uint64_t) first) {
                    again(it->first);
                    it = p.retrieved.erase(it);
                } else {
                    ++it;
                }
            }
            if (fresh && says_unpublished && !p.retrieved.empty()
                && unpublished_at > p.copy_unpublished) {
                // writes made with nothing published, after the copies, are in no
                // stream: the copies have to come after them
                for (const auto& [s, c] : p.retrieved) again(s);
                p.retrieved.clear();
                p.copy_unpublished = UINT64_MAX;
            }
            if (changed)
                write_positions_locked();
            if (!p.pending.empty())
                return hold(p, fresh ? "a new stream from this primary"
                                     : "this primary's stream has writes this copy doesn't");
            /*
             * Everything it writes here is a copy from after this stream began, so
             * the stream goes on from `first`. Records in it that the copies
             * already hold are applied again, in order, which ends where the
             * primary did. Written down before anything is applied.
             */
            if (p.incarnation != incarnation || (uint64_t) first > p.last + 1)
                p.last = (uint64_t) first - 1;
            p.durable = std::min(p.last, (uint64_t) first - 1);
            p.incarnation = incarnation;
            p.valid = true;
            p.pending.clear();
            p.retrieved.clear();
            p.copies_incarnation.clear();
            p.copy_unpublished = UINT64_MAX;
            p.held_said.clear();
            if (!write_positions_locked())
                return call.push_error("REPLFAILED could not write the replication positions file");
            barch::log({"replication from", node, "goes on from write", (uint64_t) first});
        }
    }
    auto& at = seen->second;
    uint64_t& last = at.last;

    barch::repl::applying quiet;
    // the primary took these writes, so this copy takes them too - TODO 502. The
    // first maintenance pass evicts back under the limit, as after a replay
    const barch::lift_memory_limit lifted;
    int64_t applied = 0;
    for (size_t i = 3; i < argv.size(); ++i) {
        const uint64_t seq = (uint64_t) first + (i - 3);
        if (seq <= last)
            continue;                       // sent again after a lost reply
        if (argv[i].size == 0) {
            // a plain command's place: it comes on its own once this is taken
            last = seq;
            continue;
        }
        barch::aof::record r;
        const auto d = barch::aof::decode((const uint8_t*) argv[i].bytes, argv[i].size, r);
        std::string why;
        bool ok = false;
        if (d != barch::aof::decoded::ok) {
            why = std::string(barch::aof::describe(d));
        } else {
            try {
                // one primary per space here - TODO 504. The first write claims
                // it, written down before it's applied
                const std::string space = space_of(r);
                const std::string* owner = owner_of_locked(space);
                if (owner && *owner != node) {
                    why = names({space}) + " takes writes from primary " + *owner
                          + " here, and a space can only have one";
                } else {
                    if (!owner) {
                        at.spaces.insert(space);
                        if (!write_positions_locked()) {
                            at.spaces.erase(space);
                            why = "could not write the replication positions file";
                        }
                    }
                    if (why.empty())
                        ok = apply_record(r, why);
                }
            } catch (const std::exception& e) {
                why = e.what();
            }
        }
        if (!ok) {
            /*
             * Stopped here, and the position stays before it - TODO 502. Skipping
             * it would leave this copy without the write and nothing would say so.
             */
            if (why.empty()) why = "unknown";
            ++statistics::repl::instructions_failed;
            barch::err({"replication from", node, "could not apply write", seq, "- stopped"
                        " there, after", applied, "of this batch"});
            return refuse("REPLFAILED", "write " + std::to_string(seq) + ": " + why);
        }
        last = seq;
        ++applied;
    }
    return call.push_ll(applied);
}

/* REPLNODE
 *
 * `<id> <incarnation> <last sequence> <unpublished>`: this node's replication id
 * and incarnation, the last sequence it has handed out, and how many writes it
 * has made with nothing published since it first answered this. RETRIEVE asks the
 * node it copies from before the copy, so it knows whose space it now has, which
 * of that primary's writes the copy holds (TODO 504), and whether writes were
 * made after it that no stream will carry (TODO 505).
 */
int REPLNODE(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    return call.push_string(barch::repl::copy_mark());
}

int RPING(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    //barch::server::start(interface.chars(), atoi(port.chars()));
    barch::repl::temp_client cli(interface.chars(), atoi(port.chars()), 0);
    if (!cli.ping()) {
        return call.push_error("could not ping");
    }
    return call.push_simple("OK");
}
int cmd_RPING(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, RPING);
}
/*
 * RETRIEVE host port [user secret] - TODO 480.
 *
 * Replaces this key space with the one of the same name on another barch. Every
 * shard comes over first, as shard files beside this space's own, and nothing
 * here changes until all of them have: a connection that drops, a login the other
 * side refuses or a different shard count leaves this space as it was. Then they
 * go in the way LOAD does it - every shard locked, the files installed and
 * loaded, the range table rebuilt, and a change log checkpoint at that moment,
 * since the files are the state now (TODO 479).
 *
 * `user` and `secret` log in on the other side, which needs read rights on the
 * space. Without them it's `default`, the way a RESP client starts out.
 */
int RETRIEVE(caller& call, const arg_t& argv) {

    if (argv.size() != 3 && argv.size() != 5)
        return call.wrong_arity();
    Variable host = argv[1];
    Variable port = argv[2];
    const std::string user = argv.size() == 5 ? argv[3].to_string() : std::string("default");
    const std::string secret = argv.size() == 5 ? argv[4].to_string() : std::string("empty");
    auto ks = call.kspace();
    barch::sharded_store store(ks);

    std::string err;
    const std::string space = ks->get_canonical_name();
    /*
     * Batches that touch this space are held from here until the copy is in -
     * TODO 505. Held before the source is asked for its sequence below, so
     * whatever this replica applied to the space was numbered before that, and
     * the copy holds it.
     */
    barch::repl::positions_copy_begin(space);
    struct copy_done {
        std::string space;
        ~copy_done() { barch::repl::positions_copy_end(space); }
    } done{space};
    /*
     * Which node this is a copy of, so a RETRIEVE only touches that primary's
     * standing here, and the last sequence it had handed out before the copy -
     * every write up to that is in the copy - TODO 504. Its incarnation and its
     * count of writes made with nothing published come too - TODO 505. Asked
     * before the copy is taken, never after. A source that doesn't answer is
     * nobody in particular.
     */
    std::string source_node, source_incarnation;
    uint64_t copy_seq = 0, copy_unpublished = 0;
    try {
        heap::vector<Variable> said;
        const std::vector<std::string> ask{"REPLNODE"};
        const auto link = barch::repl::create(host.s(), (int) port.i());
        if (link->call(said, ask).ok() && said.size() == 1) {
            std::istringstream answer(said[0].s());
            answer >> source_node >> source_incarnation >> copy_seq >> copy_unpublished;
            if (!answer)
                source_node.clear();        // an older node: nobody in particular
        }
    } catch (const std::exception&) {
        source_node.clear();
    }
    barch::repl::temp_client cli(host.s(), (int) port.i(), 0);
    if (!cli.receive_space(ks, user, secret, err))
        return call.push_error(("RETRIEVE: " + err + " - nothing here changed").c_str());

    std::atomic<size_t> errors = 0;
    std::mutex first_mut;
    std::string first;
    const auto& change_log = ks->get_change_log();
    uint64_t installed_at = 0;
    {
        barch::sharded_store::write_guard held = store.lock_space_write();
        installed_at = change_log ? change_log->mark() : 0;
        /*
         * Save, then checkpoint, then install - TODO 524.
         *
         * The checkpoint says every record up to `installed_at` is dead, and it
         * has to be on disk before the first received file goes in. Written after
         * the installs, as it was, a shard that failed to install or a crash
         * between two installs left the log's older records live, and a restart
         * replayed them over the files that did go in.
         *
         * Before that can be said, every shard's own files have to hold what the
         * log does, or a shard that doesn't install would lose the writes only
         * the log had. So every shard is saved first, under this lock, so the
         * files are one moment at `installed_at`. After the checkpoint each shard
         * is either the source's copy or its own save, whatever happens next.
         *
         * If a save fails, nothing is installed and nothing here changed. It costs
         * a save of what's about to be replaced, with writes held for it, which
         * only a space with a change log pays.
         */
        if (change_log) {
            std::atomic<size_t> not_saved = 0;
            try {
                change_log->sync_pending();     // before the files - TODO 523
                store.each_shard_parallel([&](const barch::shard_ptr& shard) {
                    if (!shard->save_holding_lock(true))
                        ++not_saved;
                });
                if (not_saved == 0) {
                    change_log->checkpoint(ks->space_name(), installed_at);
                    change_log->trim_to_last_checkpoint();
                }
            } catch (const std::exception& e) {
                barch::err({"RETRIEVE: could not save", space, "before the copy went in:", e.what()});
                not_saved = std::max<size_t>(not_saved, 1);
            }
            if (not_saved) {
                store.each_shard([](const barch::shard_ptr& shard) { shard->drop_received(); });
                return call.push_error(("RETRIEVE: could not save " + space
                                        + " before replacing it - nothing here changed").c_str());
            }
        }
        /*
         * The source's dictionary is a key in the files about to go in - TODO 527 -
         * so it comes with them, and what the old files needed is forgotten first.
         */
        dictionary::files_replaced(ks->get_name());
        store.each_shard_parallel([&](const barch::shard_ptr& shard) {
            std::string e;
            if (!shard->install_received_holding_lock(e)) {
                ++errors;
                std::lock_guard l(first_mut);
                if (first.empty()) first = e;
            }
        });
        /*
         * Read before the lock drops, so nothing reads the new files with the old
         * dictionary - TODO 521. A source from before TODO 527 has no key in its
         * files and sends the dictionary beside them instead, so that one goes in
         * as the key.
         */
        dictionary::load_holding_lock(ks);
        dictionary_compressor::buffer_type have;
        if (!cli.dictionary.empty() && !dictionary::get(ks->get_name(), have)) {
            std::string e;
            dictionary_compressor::buffer_type sent(cli.dictionary.begin(), cli.dictionary.end());
            if (!dictionary::install_holding_lock(ks, sent, e))
                barch::err({"RETRIEVE: could not take", space + "'s dictionary from the source -", e,
                            "- its compressed values won't read here"});
        }
        if (ks->is_range_sharded()) {
            ks->routes().rebuild(store.shards());
        }
    }
    /*
     * The space is a copy of the source's now, which only matters to the primary
     * this space takes writes from here - TODO 504. If that's the source, one of
     * its spaces is up to date again; if not, the space isn't that primary's any
     * more. A shard that didn't install leaves the space part changed, which is a
     * LOAD's case: behind, and to be retrieved again.
     */
    if (errors)
        barch::repl::positions_loaded(space);
    else
        barch::repl::positions_retrieved(space, source_node, source_incarnation, copy_seq,
                                         copy_unpublished);
    if (errors)
        return call.push_error(("RETRIEVE: " + first).c_str());
    return call.push_simple("OK");
}
int cmd_RETRIEVE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, RETRIEVE);
}
}

int add_repl_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(START), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(STOP), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(RPING), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PUBLISH), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PULL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    // it replaces the space's data - TODO 480
    if (ValkeyModule_CreateCommand(ctx, NAME(RETRIEVE), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(LOAD), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

void register_repl_api(function_map& r) {
    r["ADDROUTE"] = {::ADDROUTE,{"write","connection"}};
    r["ROUTE"] = {::ROUTE,{"read","connection"}};
    r["REMROUTE"] = {::REMROUTE,{"write","connection"}};
    r["PUBLISH"] = {::PUBLISH,{"write","connection"}};
    r["PULL"] = {::PULL,{"write","dangerous"}};
    r["LOAD"] = {::LOAD,{"write","dangerous"}};
    r["RELOAD"] = {::RELOAD,{"write","dangerous"}};
    r["START"] = {::START,{"write","connection","data"}};
    r["STOP"] = {::STOP,{"write","connection","data"}};
    r["RETRIEVE"] = {::RETRIEVE,{"write","dangerous","data"}};
    r["RPING"] = {::RPING,{"read","connection","data"}};
    r["REPLAPPLY"] = {::REPLAPPLY,{"write","dangerous","data"}};
    r["REPLNODE"] = {::REPLNODE,{"read","connection"}};
}
