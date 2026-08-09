//
// Created by teejip on 8/9/26.
//
// What kind of thing a name already holds, so a command can refuse to work on the wrong
// one. See TODO 49.
//
#ifndef BARCH_KEY_TYPE_H
#define BARCH_KEY_TYPE_H

#include "value_type.h"
#include "composite.h"
#include "conversion.h"
#include "sharded_store.h"
#include "art/iterator.h"

namespace barch {

    /**
     * What a name is being used as.
     *
     * barch does not tag values with a type the way redis does. A string lives at the
     * key's own encoding, while a list, hash or ordered set lives at a set of composite
     * keys that all begin with the same container prefix - which is a different byte
     * sequence from the plain key. Nothing prevented one name holding a string *and* a
     * list *and* a hash at the same time, because those are simply different keys, and
     * `SET k v` followed by `RPUSH k v` did exactly that.
     *
     * So the type is not stored, it is observed: a plain key that exists means a string,
     * and anything under the container prefix means a collection.
     *
     * What this deliberately does not do is tell one collection from another. A hash and
     * an ordered set under one name are indistinguishable here, and worse, they already
     * miscount each other - HLEN over a name that also holds an ordered set counts its
     * entries too, because both walk the same prefix. Telling them apart needs a type
     * tag written when the container is created, which is a stored format change and a
     * migration for anything already saved. That is TODO 53; this covers the string
     * against collection case, which is the one redis's tests exercise and the one a
     * caller actually hits.
     */
    enum class key_kind {
        none,        ///< the name is free
        string,      ///< a plain value
        container    ///< a list, hash or ordered set - which of the three is not known
    };

    /**
     * What `name` currently holds.
     *
     * This has to go through the store rather than through a shard, and that is the whole
     * subtlety of it: a plain key and its container prefix are different byte sequences,
     * so they route to *different shards*. An earlier version took the shard the caller
     * had already locked and probed both on it, which quietly never found anything -
     * every check passed and no wrong type was ever reported.
     *
     * Call it before taking the command's own lock. Doing it inside would mean asking the
     * store for a second shard while holding the first, which is the self deadlock that
     * cross space COPY hit in DONE 42. The gap between the check and the write is a race
     * in principle - a name could become a list in between - but it is the same race
     * redis has, and the loser writes a value nobody asked for rather than corrupting
     * anything.
     *
     * Two probes at worst, and the second only runs when the first misses, so the
     * ordinary path costs one extra lookup on a key that is not there.
     */
    inline key_kind kind_of(sharded_store& store, art::value_type name) {
        auto plain = conversion::as_composite(name);
        if (store.exists(plain.get_value())) {
            return key_kind::string;
        }
        // A collection is a run of composite keys sharing the container prefix. This is
        // a probe rather than a lookup of one key, and it cannot tell a hash from an
        // ordered set - see TODO 53 for what that costs and why the marker that would
        // fix it was reverted.
        bool container = false;
        composite probe;
        art::value_type prefix = probe.create({conversion::convert(name)});
        store.with_container_read(name, [&](const shard_ptr& t) {
            art::node_ptr lb = t->lower_bound(prefix);
            if (lb.null() || !lb.is_leaf) return;
            if (lb.const_leaf()->prefix(prefix) != 0) return;
            // a removed key lingers as a tombstone, which lower_bound finds and search
            // does not. Walk past them, or a deleted collection reports itself as one
            // forever and GETRANGE on the name answers wrong type after a DEL
            for (art::iterator i(t, lb.const_leaf()->get_key()); i.ok(); i.next()) {
                if (!i.key().starts_with(prefix)) return;
                const art::leaf *l = i.l();
                if (l && !l->is_tomb() && !l->deleted()) {
                    container = true;
                    return;
                }
            }
        });
        return container ? key_kind::container : key_kind::none;
    }

    /**
     * Remove everything stored under `name` as a list, hash or ordered set.
     *
     * A collection is a run of composite keys sharing a container prefix, so deleting the
     * name means deleting that run - which nothing did. `DEL k` removed only the plain
     * key, so a list survived its own deletion and the name went on reporting itself as a
     * container long after the caller thought it was gone.
     *
     * The keys are collected before any are removed. Removing while walking invalidates
     * the iterator, and the run is bounded by the size of the collection rather than by
     * the space.
     *
     * Answers with how many keys went, so a caller can tell whether there was anything.
     */
    inline size_t remove_container(sharded_store& store, art::value_type name) {
        size_t removed = 0;
        composite probe;
        art::value_type prefix = probe.create({conversion::convert(name)});
        store.with_container_write(name, [&](const shard_ptr& t) {
            heap::std_vector<std::string> doomed;
            art::node_ptr lb = t->lower_bound(prefix);
            if (lb.null()) return;
            if (lb.is_leaf && lb.const_leaf()->prefix(prefix) != 0) return;
            for (art::iterator i(t, lb.const_leaf()->get_key()); i.ok(); i.next()) {
                auto k = i.key();
                if (!k.starts_with(prefix)) break;
                doomed.emplace_back(k.chars(), k.size);
            }
            for (const auto& k : doomed) {
                if (t->remove(art::value_type{k})) {
                    ++removed;
                }
            }
        });
        return removed;
    }

    /** the message redis answers with, and the code clients branch on */
    inline const char *wrong_type_message() {
        return "WRONGTYPE Operation against a key holding the wrong kind of value";
    }
}

#endif //BARCH_KEY_TYPE_H
