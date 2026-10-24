//
// A transaction's reads through its overlay, and what they're checked by - TODO 695, 701.
// See docs/TRANSACTIONS.md.
//
#include "tx_session.h"
#include "shard.h"
#include "sharded_store.h"
#include "dictionary_compressor.h"
#include "configuration.h"
#include "key_range.h"
#include "graph.h"
#include "meta_keys.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <set>

namespace barch {
    namespace {
        void put_u32(std::string& out, uint32_t v) {
            for (int i = 0; i < 4; ++i) out.push_back((char) ((v >> (8 * i)) & 0xff));
        }
        bool get_u32(const std::string& in, size_t& at, uint32_t& v) {
            if (in.size() - at < 4) return false;
            v = 0;
            for (int i = 3; i >= 0; --i) v = (v << 8) | (uint8_t) in[at + i];
            at += 4;
            return true;
        }
        thread_local tx_read_log* open_log = nullptr;
    }

    namespace {
        // the open transactions, for INFO transactions - TODO 720. Taken before any
        // status's own lock, never while holding one
        std::mutex& listed_lock() {
            static std::mutex m;
            return m;
        }
        std::vector<std::shared_ptr<tx_status>>& listed() {
            static std::vector<std::shared_ptr<tx_status>> l;
            return l;
        }
        // a key or name as one INFO field: what would end the field or the line is \xHH
        std::string field(const std::string& s) {
            std::string out;
            for (const unsigned char c : s) {
                if (c >= 32 && c < 127 && c != ',' && c != '=' && c != '|' && c != '\\') {
                    out.push_back((char) c);
                } else {
                    char hex[5];
                    std::snprintf(hex, sizeof(hex), "\\x%02x", c);
                    out += hex;
                }
            }
            return out;
        }
        uint64_t ms_since(std::chrono::steady_clock::time_point t, std::chrono::steady_clock::time_point now) {
            if (t == std::chrono::steady_clock::time_point{} || now < t)
                return 0;
            return (uint64_t) std::chrono::duration_cast<std::chrono::milliseconds>(now - t).count();
        }
    }

    void tx_list(const std::shared_ptr<tx_status>& s) {
        std::lock_guard l(listed_lock());
        listed().push_back(s);
    }

    void tx_unlist(const std::shared_ptr<tx_status>& s) {
        std::lock_guard l(listed_lock());
        auto& all = listed();
        all.erase(std::remove(all.begin(), all.end(), s), all.end());
    }

    std::string tx_listing(bool waiting_only) {
        struct row {
            std::string id, space, who, function, command, waiting, waiting_on, holds_what;
            std::vector<std::string> held;
            uint64_t holds{0}, age_ms{0}, state_ms{0};
            bool committing{false}, waiting_gate{false};
        };
        std::vector<row> rows;
        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard l(listed_lock());
            for (const auto& s : listed()) {
                std::lock_guard one(s->m);
                rows.push_back(row{s->id, s->space, s->who, s->function, s->command, s->waiting, s->waiting_on,
                                   s->holds_what, s->held, s->holds, ms_since(s->began, now),
                                   ms_since(s->since, now), s->committing, s->waiting_gate});
            }
        }
        // the oldest first, which is the one most likely stuck
        std::sort(rows.begin(), rows.end(), [](const row& a, const row& b) { return a.id < b.id; });
        std::string out;
        size_t n = 0;
        for (auto& r : rows) {
            // a space's commit gate is held by the transaction committing in it, if a
            // listed one is; otherwise by a read command, which isn't listed
            if (r.waiting_gate && r.waiting_on.empty())
                for (const auto& o : rows)
                    if (&o != &r && o.space == r.space && o.committing && !o.holds_what.empty())
                        r.waiting_on = o.id;
            const char* state = !r.waiting.empty() ? "waiting"
                              : r.committing ? "committing"
                              : !r.command.empty() ? "running" : "idle";
            if (waiting_only && r.waiting.empty())
                continue;
            std::string held;
            for (const auto& k : r.held)
                held += (held.empty() ? "" : "|") + field(k);
            out += "tx" + std::to_string(n++) + ":id=" + r.id + ",session=" + field(r.who) + ",space=" + field(r.space)
                 + ",age_ms=" + std::to_string(r.age_ms) + ",state=" + state
                 + ",state_ms=" + std::to_string(r.state_ms) + ",function=" + field(r.function)
                 + ",command=" + field(r.command) + ",holds=" + std::to_string(r.holds)
                 + ",held=" + held + ",holds_also=" + field(r.holds_what)
                 + ",waiting=" + field(r.waiting) + ",waiting_on=" + field(r.waiting_on) + "\n";
        }
        return out;
    }

    tx_running_scope::tx_running_scope(std::shared_ptr<tx_status> st, const std::string& fn, const std::string& cmd)
        : s(std::move(st)) {
        if (!s)
            return;
        std::lock_guard l(s->m);
        function = s->function;
        command = s->command;
        if (!fn.empty())
            s->function = fn;
        s->command = cmd;
        s->since = std::chrono::steady_clock::now();
    }

    tx_running_scope::~tx_running_scope() {
        if (!s)
            return;
        std::lock_guard l(s->m);
        s->function = function;
        s->command = command;
        s->since = std::chrono::steady_clock::now();
    }

    tx_waiting_scope::tx_waiting_scope(tx_status* st, std::string what, std::string on, bool gate) : s(st) {
        if (!s)
            return;
        std::lock_guard l(s->m);
        s->waiting = std::move(what);
        s->waiting_on = std::move(on);
        s->waiting_gate = gate;
        s->since = std::chrono::steady_clock::now();
    }

    tx_waiting_scope::~tx_waiting_scope() {
        if (!s)
            return;
        std::lock_guard l(s->m);
        s->waiting.clear();
        s->waiting_on.clear();
        s->waiting_gate = false;
        s->since = std::chrono::steady_clock::now();
    }

    tx_read_log* tx_reading() { return open_log; }
    tx_reading_scope::tx_reading_scope(tx_read_log* log) : outer(open_log) { open_log = log; }
    tx_reading_scope::~tx_reading_scope() { open_log = outer; }

    bool tx_overlay_reads::as_of(const abstract_shard* source, art::value_type key, tx_seen& out) {
        if (!versions || !source)
            return false;
        // oldest first: the first layer holding the key has what it was at the snapshot
        for (const auto& t : versions->layers_from(snapshot, source->get_shard_number())) {
            auto* layer = dynamic_cast<barch::shard*>(t.get());
            if (!layer)
                continue;
            read_lock hold(t);
            const auto n = art::search(layer, key);
            if (n.null() || !n.is_leaf)
                continue;
            const art::leaf* l = n.const_leaf();
            out = tx_seen{};
            if (l->is_tomb() || l->expired())
                return true;            // missing then
            out.present = true;
            const auto v = l->get_value();
            out.value.assign(v.chars(), v.size);
            out.expiry = (int64_t) l->expiry_ms();
            out.is_volatile = l->is_volatile();
            out.hashed = l->is_hashed();
            return true;
        }
        return false;
    }

    void tx_overlay_reads::layers_under(const abstract_shard* top,
                                        heap::vector<std::shared_ptr<abstract_shard>>& out) {
        if (!versions || !overlay || !top)
            return;
        const auto n = top->get_shard_number();
        if (overlay->get(n).get() != top)
            return;                     // not this transaction's overlay
        for (auto& t : versions->layers_from(snapshot, n))
            out.push_back(std::move(t));
    }

    void tx_overlay_reads::saw_range(const void* space, art::value_type lo, art::value_type hi) {
        if (!versions || space != overlay.get())
            return;
        std::string packed(4, '\0');
        for (int i = 0; i < 4; ++i)
            packed[(size_t) i] = (char) ((lo.size >> (8 * i)) & 0xff);
        packed.append(lo.chars(), lo.size);
        packed.append(hi.chars(), hi.size);
        logged_bytes += packed.size();
        ranges.set(ranges.size(), packed);
    }

    bool tx_overlay_reads::overlays(const abstract_shard* shard) const {
        return shard && overlay && overlay->get(shard->get_shard_number()).get() == shard;
    }

    bool tx_overlay_reads::saw(const abstract_shard* shard, art::value_type key, bool present, art::value_type value,
                               int64_t expiry) {
        (void) present; (void) value; (void) expiry;
        // only the overlay's own reads: the space beneath may depend on another, and
        // its reading through is part of the same read
        if (!overlays(shard))
            return false;
        /*
         * Always the first read: the overlay reads through only for a key it doesn't
         * hold, and from then on it holds one until the end - TODO 755.
         */
        ++read_count;
        // what the log held when it kept its own copy of the key, so the limits don't move
        logged_bytes += key.size + 12;
        return true;
    }

    void tx_overlay_reads::writing(const abstract_shard* shard, art::value_type key, bool present,
                                   art::value_type value, int64_t expiry) {
        if (!overlays(shard))
            return;
        // INCR in a loop: the same key again, already kept
        if (!written_near.empty() && key.size == last_written.size()
            && memcmp(key.chars(), last_written.data(), key.size) == 0)
            return;
        last_written.assign(key.chars(), key.size);
        // the first write's: a later one finds the leaf an earlier write left
        if (written_near.contains(last_written))
            return;
        const uint64_t h = tx_hash(present, value, expiry);
        // a key and what the map holds with it, roughly
        const size_t cost = key.size + 48;
        if (written_near.empty() || near_used + cost <= near_bytes) {
            written_near.emplace(last_written, h);
            near_used += cost;
            return;
        }
        std::string packed;
        put_u32(packed, (uint32_t) (h & 0xffffffffu));
        put_u32(packed, (uint32_t) (h >> 32));
        written.insert(last_written, packed);
    }

    void tx_overlay_reads::each(const std::function<void(const std::string&, uint32_t, uint64_t)>& f) const {
        if (!overlay)
            return;
        std::string k;
        for (size_t i = 0; i < overlay->get_shard_count(); ++i) {
            auto t = overlay->get(i);
            auto* sh = dynamic_cast<barch::shard*>(t.get());
            if (!sh)
                continue;
            /*
             * Only the overlay's own shard: a read_lock takes its sources too, and they're
             * the space's shards, which COMMITTX holds for writing by now.
             */
            const bool lock = !barch::region_holds(t.get());
            if (lock)
                t->lock_shared();
            struct unlock_after {
                const shard_ptr& t;
                bool lock;
                ~unlock_after() { if (lock) t->unlock_shared(); }
            } release{t, lock};
            // every leaf was a read: the copy it took, or a write over one - TODO 755
            art::each_leaf(sh, [&](const art::leaf& l) -> bool {
                if (l.deleted())
                    return true;
                const auto key = l.get_key();
                k.assign(key.chars(), key.size);
                uint64_t hash = 0;
                if (!hash_of(k, hash))
                    hash = tx_hash(!l.is_tomb(), l.is_tomb() ? art::value_type{} : l.get_value(),
                                   l.is_tomb() ? 0 : (int64_t) l.expiry_ms());
                f(k, (uint32_t) i, hash);
                return true;
            });
        }
    }

    const std::string& tx_before(tx_session& t) {
        if (!t.open || !t.failed.empty())
            return t.failed;
        const uint64_t most = get_tx_max_age_ms();
        const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t.began).count();
        if ((most && (uint64_t) age > most) || (t.versions && t.versions->expired(t.snapshot)))
            t.failed = "TXABORTED the transaction was open longer than tx_max_age_ms (" + std::to_string(most) +
                       " ms)";
        return t.failed;
    }

    const std::string& tx_after(tx_session& t) {
        if (!t.open || !t.failed.empty() || !t.overlay)
            return t.failed;
        const uint64_t most_keys = get_tx_max_keys(), most_bytes = get_tx_max_bytes();
        if (!most_keys && !most_bytes)
            return t.failed;
        uint64_t keys = 0, bytes = 0;
        for (const auto& s : t.overlay->get_shards()) {
            keys += s->get_tree_size();         // a tomb is a key it holds too
            // what its leaves and nodes take, as INFO's virtual_allocated counts it
            if (auto* sh = dynamic_cast<barch::shard*>(s.get()))
                bytes += sh->get_ap().get_leaves().get_allocated() + sh->get_ap().get_nodes().get_allocated();
        }
        /*
         * A write reads its key first, so the keys written are among those read, and
         * each key counts once as the larger of the two. Bytes are both: the overlay and
         * the log each hold theirs.
         */
        if (t.reads) {
            keys = std::max<uint64_t>(keys, t.reads->size() + t.reads->range_count());
            bytes += t.reads->bytes();
        }
        if (most_keys && keys > most_keys)
            t.failed = "TXABORTED the transaction holds " + std::to_string(keys) + " keys, written and read, "
                       "more than tx_max_keys (" + std::to_string(most_keys) + ")";
        else if (most_bytes && bytes > most_bytes)
            t.failed = "TXABORTED the transaction holds " + std::to_string(bytes) + " bytes, written and read, "
                       "more than tx_max_bytes (" + std::to_string(most_bytes) + ")";
        if (!t.failed.empty())
            ++statistics::tx_over_limit;
        return t.failed;
    }

    bool tx_takes(const std::string& name) {
        static const std::set<std::string> s{
            "GET", "MGET", "GETFORUPDATE", "MGETFORUPDATE", "EXISTS", "STRLEN", "INCR", "DECR", "INCRBY",
            "DECRBY", "APPEND",
            "SET", "SETNX", "SETEX", "PSETEX", "GETSET", "MSET", "DEL",
        };
        return s.count(name) > 0;
    }

    bool tx_walks(const std::string& name) {
        return name == "RANGE" || name == "COUNT" || name == "KEYS" || name == "SCAN";
    }

    namespace {
        /*
         * The keys of a string command, each an index into its arguments, and whether
         * it's written (an exclusive lock) or only read (shared) - TODO 719. Empty for
         * one that isn't a string command.
         *
         * GETFORUPDATE and MGETFORUPDATE read, but lock as a write would - TODO 729. A
         * transaction that reads a key and then writes it holds the shared lock in
         * between, and two doing that at once each wait for the other's to go: a
         * deadlock. Taking the exclusive lock at the read makes the second one wait
         * for the first to finish instead.
         */
        std::vector<std::pair<size_t, bool>> lock_args(const std::string& name, size_t argc) {
            std::vector<std::pair<size_t, bool>> out;
            if (argc < 2)
                return out;
            if (name == "GET" || name == "STRLEN") {
                out.emplace_back(1, false);
            } else if (name == "MGET" || name == "EXISTS") {
                for (size_t i = 1; i < argc; ++i)
                    out.emplace_back(i, false);
            } else if (name == "GETFORUPDATE") {
                out.emplace_back(1, true);
            } else if (name == "MGETFORUPDATE") {
                for (size_t i = 1; i < argc; ++i)
                    out.emplace_back(i, true);
            } else if (name == "DEL") {
                for (size_t i = 1; i < argc; ++i)
                    out.emplace_back(i, true);
            } else if (name == "MSET") {
                for (size_t i = 1; i < argc; i += 2)
                    out.emplace_back(i, true);
            } else if (name == "SET" || name == "SETNX" || name == "SETEX" || name == "PSETEX" || name == "GETSET"
                       || name == "APPEND" || name == "INCR" || name == "DECR" || name == "INCRBY"
                       || name == "DECRBY") {
                out.emplace_back(1, true);
            }
            return out;
        }

        std::string lock_key(const key_space_ptr& ks, const std::string& arg) {
            const auto encoded = ks->encode_key(art::value_type(arg.data(), arg.size()));
            const auto v = encoded.get_value();
            return tx_locks::key_of(v.chars(), v.size);
        }

        /*
         * A wait that would close a ring - TODO 724: this one gives way. It can only be
         * ended now, like one that timed out, but its locks go at once, so the others in
         * the ring go on without waiting for its ABORTTX.
         */
        std::string tx_deadlocked(tx_session& t, const std::string& arg, const tx_locks::taken& got) {
            ++statistics::tx_deadlocks;
            constexpr size_t shown = 8;
            std::string ring = "transaction " + got.ring.front();
            for (size_t i = 1; i < got.ring.size() && i < shown; ++i)
                ring += ", which waits on " + got.ring[i];
            if (got.ring.size() > shown)
                ring += ", which waits on " + std::to_string(got.ring.size() - shown) + " more";
            t.failed = "TXDEADLOCK " + arg + " is locked by " + ring + ", which waits on this one. Its locks are let go of";
            t.release_locks();
            if (t.status) {
                std::lock_guard l(t.status->m);
                t.status->holds = 0;
                t.status->held.clear();
            }
            return t.failed;
        }

        std::string lock_wait_said() {
            return "tx_lock_wait_ms (" + std::to_string(get_tx_lock_wait_ms()) + " ms)";
        }

        /*
         * One key's lock, `arg` as sent: waited for up to `until`, or with `busy` only
         * tried, the one in the way said there. Empty when it's taken, or why not.
         */
        std::string lock_stored(tx_session& t, const std::string& key, const std::string& arg, bool exclusive,
                                std::chrono::steady_clock::time_point until, tx_lock_busy* busy, bool listed = true);

        std::string lock_one(tx_session& t, const std::string& arg, bool exclusive,
                             std::chrono::steady_clock::time_point until, tx_lock_busy* busy) {
            return lock_stored(t, lock_key(t.space, arg), arg, exclusive, until, busy);
        }

        // lock_one for a key already in tx_locks::key_of form; `arg` is how it's named
        std::string lock_stored(tx_session& t, const std::string& key, const std::string& arg, bool exclusive,
                                std::chrono::steady_clock::time_point until, tx_lock_busy* busy, bool listed) {
            const bool had = t.locked.count(key) > 0;
            tx_locks::taken got;
            if (busy) {
                got = t.locks->take(t.id, key, exclusive, until);
            } else {
                // shown with the one holding it now, while it waits - TODO 720
                const auto in_way = t.locks->held_by_other(t.id, key);
                tx_waiting_scope waiting(in_way.empty() ? nullptr : t.status.get(), "a lock on " + arg, in_way);
                got = t.locks->take(t.id, key, exclusive, until);
            }
            if (got.deadlock)
                return tx_deadlocked(t, arg, got);
            if (!got.ok) {
                tx_lock_busy b{key, arg, got.holder};
                if (busy) {
                    *busy = std::move(b);
                    return {};
                }
                return tx_lock_timed_out(t, b);
            }
            if (had)
                return {};
            t.locked.insert(key);
            if (t.status && listed) {
                std::lock_guard l(t.status->m);
                ++t.status->holds;
                if (t.status->held.size() < tx_status::held_shown)
                    t.status->held.push_back(arg);
            }
            return {};
        }
    }

    namespace {
        /*
         * The spans a walk reads, in tx_locks::key_of form - TODO 749. RANGE and COUNT
         * walk [lo, hi) in both regions (text_bounds); KEYS and SCAN walk everything,
         * as their phantom check counts it for an optimistic transaction. None for a
         * command that will only answer an error.
         */
        std::vector<tx_locks::span> spans_of(const key_space_ptr& ks, const std::string& lo, const std::string& hi,
                                             bool whole) {
            std::vector<tx_locks::span> out;
            if (whole) {
                out.push_back(tx_locks::span{{}, {}, true});
                return out;
            }
            try {
                for (const auto& [l, h] : text_bounds(ks, art::value_type(lo.data(), lo.size()),
                                                      art::value_type(hi.data(), hi.size())))
                    out.push_back(tx_locks::span{tx_locks::key_of(l.data(), l.size()),
                                                 tx_locks::key_of(h.data(), h.size()), false});
            } catch (const std::exception&) {
                out.clear();    // a bound the walk refuses, which it says itself
            }
            return out;
        }

        std::string span_said(const std::string& lo, const std::string& hi, bool whole) {
            return whole ? std::string("every key") : "the range " + lo + " to " + hi;
        }

        // one span's lock, as lock_one takes a key's
        std::string lock_span(tx_session& t, const tx_locks::span& sp, const std::string& said,
                              std::chrono::steady_clock::time_point until, tx_lock_busy* busy) {
            // tried first, then waited for, shown with the one in the way - TODO 720
            auto got = t.locks->take_span(t.id, sp, busy ? until : std::chrono::steady_clock::now());
            if (!busy && !got.ok && !got.deadlock) {
                tx_waiting_scope waiting(t.status.get(), "a lock on " + said, got.holder);
                got = t.locks->take_span(t.id, sp, until);
            }
            if (got.deadlock)
                return tx_deadlocked(t, "a key in " + said, got);
            if (!got.ok) {
                tx_lock_busy b{got.key, "a key in " + said, got.holder};
                if (busy) {
                    *busy = std::move(b);
                    return {};
                }
                return tx_lock_timed_out(t, b);
            }
            return {};
        }

        std::string lock_spans(tx_session& t, const std::string& lo, const std::string& hi, bool whole,
                               std::chrono::steady_clock::time_point until, tx_lock_busy* busy) {
            const auto said = span_said(lo, hi, whole);
            for (const auto& sp : spans_of(t.space, lo, hi, whole)) {
                auto why = lock_span(t, sp, said, until, busy);
                if (!why.empty() || (busy && *busy))
                    return why;
            }
            if (t.status) {
                std::lock_guard l(t.status->m);
                if (t.status->held.size() < tx_status::held_shown)
                    t.status->held.push_back(said);
            }
            return {};
        }
    }

    namespace {
        // the graph's layout meta key, which every graph commit writes, as tx_locks has it
        std::string graph_lock_key() {
            composite q;
            const auto k = meta::key(q, graph::layout_meta_name());
            return tx_locks::key_of(k.chars(), k.size);
        }

        std::string upper(std::string s) {
            for (auto& c : s) c = (char) std::toupper((unsigned char) c);
            return s;
        }

        /*
         * GRAPH in a pessimistic transaction - TODO 750. Which keys a subcommand reads
         * isn't known until it walks the path, so every one locks the whole graph: the
         * regions its keys live in, and the layout key, shared. A write takes the layout
         * key exclusive too, so it waits for every other transaction reading the graph,
         * and they wait for it. A writer outside a transaction waits on that key as well
         * (tx_plain_wait), and any other write into the regions meets the range lock.
         */
        std::string lock_graph(tx_session& t, const std::vector<std::string>& args,
                               std::chrono::steady_clock::time_point until, tx_lock_busy* busy) {
            if (args.size() < 2)
                return {};          // the command says what's wrong
            static const std::string said = "the graph";
            const auto sentinel = graph_lock_key();
            /*
             * The layout key first: a writer holds it exclusive, so a reader waits there
             * holding nothing yet. Taking the regions first, it would hold them while it
             * waited, and the writer's COMMITTX would meet them.
             */
            std::vector<tx_locks::span> spans{tx_locks::span{sentinel, sentinel, false}};
            for (const auto& [lo, hi] : graph::lock_regions())
                for (auto& sp : spans_of(t.space, lo, hi, false))
                    spans.push_back(std::move(sp));
            for (const auto& sp : spans) {
                auto why = lock_span(t, sp, said, until, busy);
                if (!why.empty() || (busy && *busy))
                    return why;
            }
            if (graph::writes(upper(args[1]))) {
                auto why = lock_stored(t, sentinel, said, true, until, busy, false);
                if (!why.empty() || (busy && *busy))
                    return why;
            }
            if (t.status) {
                std::lock_guard l(t.status->m);
                if (t.status->held.size() < tx_status::held_shown
                    && std::find(t.status->held.begin(), t.status->held.end(), said) == t.status->held.end())
                    t.status->held.push_back(said);
            }
            return {};
        }
    }

    std::string tx_lock(tx_session& t, const std::string& name, const std::vector<std::string>& args,
                        tx_lock_busy* busy) {
        if (!t.pessimistic || !t.locks || !t.space)
            return {};
        if (!t.failed.empty())
            return t.failed;    // it can only be ended now
        const auto now = std::chrono::steady_clock::now();
        // not waited for when the session parks instead - TODO 721
        const auto until = busy ? now : now + std::chrono::milliseconds(get_tx_lock_wait_ms());
        if (name == "GRAPH")
            return lock_graph(t, args, until, busy);
        if (tx_walks(name)) {
            if (name == "KEYS" || name == "SCAN")
                return lock_spans(t, {}, {}, true, until, busy);
            if (args.size() >= 3)
                return lock_spans(t, args[1], args[2], false, until, busy);
            return {};
        }
        for (const auto& [i, exclusive] : lock_args(name, args.size())) {
            auto why = lock_one(t, args[i], exclusive, until, busy);
            if (!why.empty() || (busy && *busy))
                return why;
        }
        return {};
    }

    std::string tx_lock_timed_out(tx_session& t, const tx_lock_busy& busy) {
        if (t.locks)
            t.locks->stop_waiting(t.id);    // given up: no longer in anyone's ring - TODO 724
        t.failed = "TXLOCKTIMEOUT " + busy.arg + " is locked by transaction " + busy.holder + ", waited " +
                   lock_wait_said();
        return t.failed;
    }

    std::string tx_plain_wait(const key_space_ptr& ks, const std::string& name, const std::vector<std::string>& args,
                              tx_lock_busy* busy) {
        if (!ks)
            return {};
        std::shared_ptr<tx_locks> locks;
        {
            std::lock_guard l(ks->versions_mu);
            locks = ks->locks;
        }
        if (!locks || !locks->any())
            return {};
        // a GRAPH write waits for the transactions using the graph - TODO 750
        const bool graph_write = name == "GRAPH" && args.size() >= 2 && graph::writes(upper(args[1]));
        if (graph_write && busy) {
            const auto key = graph_lock_key();
            auto other = locks->held_by_other(tx_locks::owner_now(), key);
            if (!other.empty())
                *busy = tx_lock_busy{key, "the graph", std::move(other)};
            return {};
        }
        if (busy) {
            // one look at each, no wait - TODO 721
            for (const auto& [i, exclusive] : lock_args(name, args.size())) {
                if (!exclusive)
                    continue;
                const auto key = lock_key(ks, args[i]);
                auto other = locks->held_by_other(tx_locks::owner_now(), key);
                if (!other.empty()) {
                    *busy = tx_lock_busy{key, args[i], std::move(other)};
                    return {};
                }
            }
            return {};
        }
        std::set<std::string> keys;
        if (graph_write)
            keys.insert(graph_lock_key());
        for (const auto& [i, exclusive] : lock_args(name, args.size()))
            if (exclusive)
                keys.insert(lock_key(ks, args[i]));
        if (keys.empty())
            return {};
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(get_tx_lock_wait_ms());
        const auto other = locks->wait_free(tx_locks::owner_now(), keys, until);
        if (other.empty())
            return {};
        return "TXLOCKTIMEOUT a key is locked by transaction " + other + ", waited " + lock_wait_said();
    }

    std::string tx_plain_timed_out(const tx_lock_busy& busy) {
        return "TXLOCKTIMEOUT " + busy.arg + " is locked by transaction " + busy.holder + ", waited " +
               lock_wait_said();
    }

    std::string tx_lock_wake_key(const std::string& key) {
        return std::string("\x01txlock:") + key;
    }

    void tx_lock_wake(const key_space_ptr& ks, const std::set<std::string>& keys) {
        if (!ks)
            return;
        for (const auto& k : keys) {
            auto t = ks->get(ks->get_shard_index(art::value_type(k.data(), k.size())));
            if (!t)
                continue;
            std::unique_lock lck(t->get_latch());
            t->call_unblock(tx_lock_wake_key(k));
        }
    }

    void tx_lock_recheck(const key_space_ptr& ks, const std::string& owner, const std::string& key) {
        if (!ks)
            return;
        std::shared_ptr<tx_locks> locks;
        {
            std::lock_guard l(ks->versions_mu);
            locks = ks->locks;
        }
        if (locks && !locks->held_by_other(owner, key).empty())
            return;                 // still held: its release wakes it
        tx_lock_wake(ks, {key});
    }

    void tx_lock_waiting(tx_status* s, const std::string& command, const tx_lock_busy* busy) {
        if (!s)
            return;
        std::lock_guard l(s->m);
        s->command = busy ? command : std::string();
        s->waiting = busy ? "a lock on " + busy->arg : std::string();
        s->waiting_on = busy ? busy->holder : std::string();
        s->waiting_gate = false;
        s->since = std::chrono::steady_clock::now();
    }

    uint64_t tx_hash(bool present, art::value_type value, int64_t expiry) {
        // FNV-1a over presence, deadline and value: the same bytes give the same hash anywhere
        uint64_t h = 1469598103934665603ull;
        auto mix = [&](const void* p, size_t n) {
            const auto* b = (const uint8_t*) p;
            for (size_t i = 0; i < n; ++i) {
                h ^= b[i];
                h *= 1099511628211ull;
            }
        };
        const uint8_t there = present ? 1 : 0;
        mix(&there, 1);
        if (present) {
            for (int i = 0; i < 8; ++i) {
                const uint8_t b = (uint8_t) (((uint64_t) expiry >> (8 * i)) & 0xff);
                mix(&b, 1);
            }
            mix(value.bytes, value.size);
        }
        return h;
    }

    uint64_t tx_hash_now(const key_space_ptr& ks, const std::string& key) {
        if (!ks)
            return tx_hash(false, {}, 0);
        uint64_t h = tx_hash(false, {}, 0);
        barch::sharded_store store(ks);
        store.search(art::value_type(key.data(), key.size()), [&](const art::node_ptr& n) {
            auto cl = n.const_leaf();
            if (cl->expired())
                return;
            auto vt = cl->get_value();
            if (cl->is_compressed())
                vt = dictionary::decompress(art::space_of(n), vt);
            h = tx_hash(true, vt, (int64_t) cl->expiry_ms());
        });
        return h;
    }

    std::string tx_range_key(art::value_type lo, art::value_type hi) {
        std::string out;
        put_u32(out, (uint32_t) lo.size);
        out.append(lo.chars(), lo.size);
        out.append(hi.chars(), hi.size);
        return out;
    }

    bool tx_range_from(const std::string& packed, std::string& lo, std::string& hi) {
        size_t at = 0;
        uint32_t n = 0;
        if (!get_u32(packed, at, n) || packed.size() - at < n)
            return false;
        lo.assign(packed, at, n);
        hi.assign(packed, at + n, std::string::npos);
        return true;
    }

    uint64_t tx_range_hash(const heap::vector<shard_ptr>& levels, art::value_type lo, art::value_type hi) {
        uint64_t h = 1469598103934665603ull;
        auto mix = [&](const void* p, size_t n) {
            const auto* b = (const uint8_t*) p;
            for (size_t i = 0; i < n; ++i) {
                h ^= b[i];
                h *= 1099511628211ull;
            }
        };
        // no lower bound: from the smallest key of any level, as merged_count starts
        std::string first;
        if (lo.empty()) {
            bool any = false;
            for (const auto& s : levels) {
                auto m = s->tree_minimum();
                if (!m.is_leaf)
                    continue;
                const auto k = m.const_leaf()->get_key();
                if (!any || k < art::value_type{first.data(), first.size()}) {
                    first.assign(k.chars(), k.size);
                    any = true;
                }
            }
            if (!any)
                return h;
            lo = art::value_type{first.data(), first.size()};
        }
        heap::vector<art::iterator> each;
        for (const auto& s : levels)
            each.emplace_back(s, lo);
        for (art::merge_iterator m(each, hi); m.ok(); m.next()) {
            const auto k = m.key();
            if (k < lo)
                continue;
            const auto n = m.current();
            const art::leaf* l = n.const_leaf();
            if (l->is_tomb() || l->expired())
                continue;
            auto v = l->get_value();
            if (l->is_compressed())
                v = dictionary::decompress(art::space_of(n), v);
            // little end first, so any node makes the same bytes
            uint8_t b[8];
            for (int i = 0; i < 4; ++i) b[i] = (uint8_t) ((k.size >> (8 * i)) & 0xff);
            mix(b, 4);
            mix(k.bytes, k.size);
            const uint64_t one = tx_hash(true, v, (int64_t) l->expiry_ms());
            for (int i = 0; i < 8; ++i) b[i] = (uint8_t) ((one >> (8 * i)) & 0xff);
            mix(b, 8);
        }
        return h;
    }

    uint64_t tx_range_hash_now(const key_space_ptr& ks, uint32_t shard, art::value_type lo, art::value_type hi) {
        if (!ks || shard >= ks->get_shard_count())
            return tx_range_hash({}, lo, hi);
        auto t = ks->get(shard);
        read_lock hold(t);
        heap::vector<shard_ptr> levels;
        for (auto s = t; s; s = s->sources())
            levels.push_back(s);
        return tx_range_hash(levels, lo, hi);
    }

    std::string tx_read_word(uint32_t shard, const std::string& space, const std::string& key, uint64_t hash) {
        std::string out;
        put_u32(out, shard);
        put_u32(out, (uint32_t) space.size());
        out += space;
        put_u32(out, (uint32_t) key.size());
        out += key;
        put_u32(out, (uint32_t) (hash & 0xffffffffu));
        put_u32(out, (uint32_t) (hash >> 32));
        return out;
    }

    bool tx_read_from(const std::string& word, uint32_t& shard, std::string& space, std::string& key,
                      uint64_t& hash) {
        size_t at = 0;
        uint32_t n = 0, lo = 0, hi = 0;
        if (!get_u32(word, at, shard) || !get_u32(word, at, n) || word.size() - at < n)
            return false;
        space.assign(word, at, n);
        at += n;
        if (!get_u32(word, at, n) || word.size() - at < n)
            return false;
        key.assign(word, at, n);
        at += n;
        if (!get_u32(word, at, lo) || !get_u32(word, at, hi) || at != word.size())
            return false;
        hash = ((uint64_t) hi << 32) | lo;
        return true;
    }
}
