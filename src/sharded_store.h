//
// Created by teejip on 8/1/26.
//

#ifndef BARCH_SHARDED_STORE_H
#define BARCH_SHARDED_STORE_H

#include <functional>

#include "art/art.h"
#include "art/iterator.h"
#include "key_space.h"
#include "keyspace_locks.h"

namespace barch {

    /**
     * A view over the shards of one key space, and the only place that should know a
     * key space is sharded at all.
     *
     * Three things used to be spread across every command that touched storage, and
     * live here instead:
     *
     *  - routing. which shard owns a key. commands used to call kspace()->get(key)
     *    themselves.
     *  - fan out and cross shard ordering. a minimum is per shard, so the global
     *    minimum is a reduction over all of them; a range has to merge N ordered
     *    streams. every command that needed this open coded the loop.
     *  - locking. each operation takes the lock it needs, for exactly as long as it
     *    needs it. this is per operation and deliberately not uniform: a point read
     *    locks one shard, an ordered scan locks the whole space because it walks all
     *    of them, and glob locks nothing because it copies pages to a working buffer
     *    before matching.
     *
     * Lifetime rule for callbacks: a key or leaf handed to a callback is borrowed from
     * a leaf inside a shard, and stays valid only for the duration of the callback.
     * The lock that keeps it alive is released when the operation returns. Copy it if
     * it needs to outlive the call. This is why the ordered operations below take a
     * callback rather than returning a value_type - returning one would hand back a
     * pointer to memory the caller no longer holds a lock on.
     *
     * The class is a value, cheap to construct, and meant to be made per call. It is
     * left open on purpose: shard_for() and shards() are the two routing primitives
     * every operation here is composed from, so a later stateful implementation -
     * one that keeps a routing table for remote shards, or carries a transaction or a
     * scan cursor across calls - can override those two and inherit the rest.
     */
    /**
     * Where a scan has got to.
     *
     * Ownership is split on purpose, and this is the split:
     *
     *  - the *lifetime* is the connection's. A scan spans commands, so something has to
     *    hold this between them, and how many a connection may keep open, what they
     *    cost, and when they are dropped is connection level accounting. caller keeps a
     *    collection of these and answers CLIENT INFO and CLIENT CLEAR_ITERS from it.
     *  - the *content* is sharding state - which shards are left to walk, which page of
     *    the current one, and the copy of that page being read - so only sharded_store
     *    touches these fields. caller stores it and never looks inside.
     */
    struct scan_cursor {
        size_t shard{};
        size_t page{};
        size_t pos{};
        size_t bytes{};
        size_t id{};
        key_space_ptr space{};
        heap::vector<shard_ptr> shards{};
        heap::vector<uint8_t> buffer{};

        /** what this cursor costs: the page copy dominates, the shard list is pointers */
        [[nodiscard]] size_t memory() const {
            return sizeof(scan_cursor)
                 + buffer.capacity() * sizeof(uint8_t)
                 + shards.capacity() * sizeof(shard_ptr);
        }
    };
    typedef std::shared_ptr<scan_cursor> scan_cursor_ptr;

    class sharded_store {
    public:
        // borrowed for the duration of the call - see the lifetime rule above
        typedef std::function<void(art::value_type key)> key_cb;
        typedef std::function<bool(const art::leaf& l)> leaf_cb;
        typedef std::function<void(const art::node_ptr& found)> node_cb;
        typedef std::function<art::node_ptr(const art::node_ptr& leaf)> updater_fn;
        typedef std::function<void(const shard_ptr& shard)> shard_fn;

        explicit sharded_store(key_space_ptr space);
        virtual ~sharded_store() = default;

        sharded_store(const sharded_store&) = default;
        sharded_store& operator=(const sharded_store&) = delete;

        [[nodiscard]] const key_space_ptr& space() const { return spc; }
        [[nodiscard]] size_t shard_count() const;
        [[nodiscard]] size_t size() const;

        // ---- routing primitives: everything below is built from these two ----

        /** the shard that owns key */
        [[nodiscard]] virtual shard_ptr shard_for(art::value_type key) const;
        /** every shard in the space, in shard number order */
        [[nodiscard]] virtual const heap::vector<shard_ptr>& shards() const;

        // ---- single key ----

        /**
         * find key and hand the leaf to cb under a read lock on its shard. cb is not
         * called if the key is absent, is a tombstone, or the shard's bloom filter
         * rules it out.
         * @return true if cb was called
         */
        bool search(art::value_type key, const node_cb& cb) const;

        /** true if key is present. does not read the value */
        [[nodiscard]] bool exists(art::value_type key) const;

        /** insert or replace, under a write lock on the owning shard */
        bool insert(const art::key_options& opts, art::value_type key, art::value_type value,
                    bool update, const art::NodeResult& fc);

        /** insert only if absent, under a write lock on the owning shard */
        bool add(const art::key_options& opts, art::value_type key, art::value_type value,
                 const art::NodeResult& fc);

        /** remove key, under a write lock on the owning shard */
        bool remove(art::value_type key, const art::NodeResult& fc);

        /** replace the leaf for key with whatever updater returns */
        bool update(art::value_type key, const updater_fn& updater);

        // ---- multi step escape hatch ----
        //
        // route once, take the lock once, and run fn with the owning shard. for
        // sequences that must not drop the lock in between - a read followed by a
        // conditional write, say. prefer the single operations above when one step
        // is enough: these hand out a shard, and a caller holding one is back to
        // knowing about sharding.

        void with_key_write(art::value_type key, const shard_fn& fn);
        void with_key_read(art::value_type key, const shard_fn& fn) const;

        // ---- held locks ----
        //
        // when the lock has to span a whole command - one with several exits, or one
        // that registers a blocking callback before it returns - a callback scope will
        // not do. these hand back a guard to hold instead. routing and the choice of
        // lock still happen here; only the extent is the caller's.

        typedef ordered_lock<storage_release> write_guard;
        typedef ordered_lock<read_lock> read_guard;

        /**
         * the shard that owns a key, with a lock held on it for as long as this lives.
         * dereferences to the shard, so a body written against a routed shard needs no
         * change - only the two lines that used to route and lock by hand.
         */
        template<typename Guard>
        struct locked_shard {
            shard_ptr shard{};
            Guard guard{};
            locked_shard() = default;
            locked_shard(shard_ptr s, Guard g) : shard(std::move(s)), guard(std::move(g)) {}
            locked_shard(locked_shard&&) = default;
            locked_shard& operator=(locked_shard&&) = default;
            locked_shard(const locked_shard&) = delete;
            locked_shard& operator=(const locked_shard&) = delete;
            const shard_ptr& operator->() const { return shard; }
            const shard_ptr& ptr() const { return shard; }
            // implicit, so it passes anywhere the routed shard used to
            operator const shard_ptr&() const { return shard; }
            bool null() const { return !shard; }
        };
        typedef locked_shard<write_guard> write_locked_shard;
        typedef locked_shard<read_guard> read_locked_shard;

        /** route to key's shard and write lock it, handing back both */
        [[nodiscard]] write_locked_shard write_locked(art::value_type key) const;
        /** route to key's shard and read lock it, handing back both */
        [[nodiscard]] read_locked_shard read_locked(art::value_type key) const;

        /** write lock the shard that owns key */
        [[nodiscard]] write_guard lock_key_write(art::value_type key) const;
        /** write lock every shard in the space, in shard order */
        [[nodiscard]] write_guard lock_space_write() const;
        /** read lock every shard in the space, in shard order */
        [[nodiscard]] read_guard lock_space_read() const;

        // ---- containers ----
        //
        // a hash, ordered set or list keeps every member on the shard that owns the
        // container key, because members are composite keys built from it. so a
        // command that touches many members of one container routes once and takes one
        // lock. these are with_key_* named for that intent, and routing must be done
        // on the same bytes every time or members would scatter across shards.

        void with_container_write(art::value_type container, const shard_fn& fn);
        void with_container_read(art::value_type container, const shard_fn& fn) const;

        // ---- whole space ----
        //
        // per shard lifecycle operations - load, save, clear, transaction boundaries.
        // the lock is named in the call so the choice is visible at the call site.

        /** fn for every shard, unlocked: for operations that lock internally or need none */
        void each_shard(const shard_fn& fn) const;
        /** fn for every shard, each under its own write lock */
        void each_shard_write(const shard_fn& fn) const;
        /** fn for every shard, each under its own read lock */
        void each_shard_read(const shard_fn& fn) const;
        /** fn for every shard, one thread per shard, unlocked. for bulk load and save */
        void each_shard_parallel(const shard_fn& fn) const;

        // ---- ordered fan out ----
        //
        // these reduce a per shard answer to a global one, and hold a shared lock
        // over the whole space while they do, because they read every shard.

        /** smallest key in the space */
        bool minimum(const key_cb& cb) const;
        /** largest key in the space */
        bool maximum(const key_cb& cb) const;
        /** first key not less than key */
        bool lower_bound(art::value_type key, const key_cb& cb) const;
        /** first key greater than key */
        bool upper_bound(art::value_type key, const key_cb& cb) const;

        /** number of keys in [lo, hi) */
        [[nodiscard]] int64_t count(art::value_type lo, art::value_type hi) const;

        /**
         * keys in [lo, hi) in ascending order, at most limit of them, or all of them
         * when limit is negative. cb is called under a shared lock on the space.
         */
        void range(art::value_type lo, art::value_type hi, int64_t limit, const key_cb& cb) const;

        /**
         * glob match over keys, or over values when by_value is set, calling cb for
         * each match. takes no lock: the shards copy each page to a working buffer
         * before matching, so a leaf handed to cb is valid only inside the callback.
         * cb runs on worker threads and must serialise itself.
         */
        void glob(const art::keys_spec& spec, art::value_type pattern, bool by_value,
                  const leaf_cb& cb) const;

        // ---- scan ----

        /** false to stop the scan here; the cursor keeps its place for the next call */
        typedef std::function<bool(art::value_type key)> scan_cb;

        /**
         * point a fresh cursor at every shard of the space, and at each shard's pull
         * sources, which are walked as separate shards and de-duplicated against the
         * shard that shadows them.
         * @return false if the space and its source disagree on shard count
         */
        bool open_scan(scan_cursor& cursor) const;

        /**
         * walk on from wherever the cursor is, handing each key to cb, and stopping
         * either when cb says so or when the shards run out.
         * @return true when the scan is finished and the cursor can be dropped
         */
        bool scan(scan_cursor& cursor, const art::scan_spec& spec, const scan_cb& cb) const;

    protected:
        key_space_ptr spc;
    };

    /**
     * merge a shard with its pull sources into one ordered stream from lower
     */
    art::merge_iterator make_merged(const shard_ptr& shard, art::value_type lower);
}

#endif //BARCH_SHARDED_STORE_H
