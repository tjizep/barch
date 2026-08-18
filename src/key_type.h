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
     * One collection is told from another by the lead byte its keys carry - see
     * tcomposite_list in nodes.h and container_kind below. Before that, all three shared a
     * prefix and so miscounted each other: HLEN over a name that also held an ordered set
     * counted its members as fields. They are separate key ranges now, so neither can see
     * the other.
     */
    enum class key_kind {
        none,        ///< the name is free
        string,      ///< a plain value
        container    ///< a list, hash or ordered set - see container_kind for which
    };

    /** which sort of collection a name holds, now that the lead byte says so */
    enum class container_kind {
        none,
        list,
        hash,
        ordered_map
    };

    /** the lead byte a container of this kind stores its keys under */
    inline art::composite_type lead_of(container_kind k) {
        switch (k) {
            case container_kind::list: return art::ts_list;
            case container_kind::hash: return art::ts_hash;
            case container_kind::ordered_map: return art::ts_ordered_map;
            default: return art::ts_composite;
        }
    }

    // defined below, and used by kind_of
    inline container_kind kind_of_container(sharded_store& store, art::value_type name);

    inline const char *name_of(container_kind k) {
        switch (k) {
            case container_kind::list: return "list";
            case container_kind::hash: return "hash";
            case container_kind::ordered_map: return "ordered set";
            default: return "none";
        }
    }

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
     * Four probes at worst - the plain key, then one per container kind - and they only
     * run until one answers, so a name that holds a string costs a single lookup.
     */
    inline key_kind kind_of(sharded_store& store, art::value_type name) {
        auto plain = store.space()->encode_key(name);
        if (store.exists(plain.get_value())) {
            return key_kind::string;
        }
        // A collection is a run of composite keys sharing a container prefix - a probe
        // rather than a lookup of one key, since the name itself holds nothing.
        return kind_of_container(store, name) != container_kind::none
                   ? key_kind::container
                   : key_kind::none;
    }

    /**
     * Does a collection of this kind live under `name`?
     *
     * One probe against one lead byte. Since the kind is part of the address there is
     * nothing stored to read back and nothing that can disagree with the keys themselves -
     * either there is a live key under this prefix or there is not.
     */
    inline bool has_container_of(sharded_store& store, art::value_type name, container_kind kind) {
        bool found = false;
        composite probe;
        // zt=false: create() zero terminates by default, and a prefix that ends in the
        // terminator is not a prefix of any key that carries components after the name -
        // it never matched, so every container probe answered none
        art::value_type prefix = probe.create(lead_of(kind), {conversion::convert(name)}, false);
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
                    found = true;
                    return;
                }
            }
        });
        return found;
    }

    /**
     * Which kind of collection `name` holds, if any.
     *
     * Three probes rather than one, because the kinds are separate key ranges now and
     * nothing about one of them says whether the others exist. That is the price of
     * putting the kind in the address, and it is why the check that matters -
     * claim_container_kind - runs where a collection is created rather than on every
     * command that touches one.
     *
     * The same caution as kind_of applies, and more sharply: each lead routes to its own
     * shard, so this must be called before the command takes its own lock.
     */
    inline container_kind kind_of_container(sharded_store& store, art::value_type name) {
        if (has_container_of(store, name, container_kind::list)) return container_kind::list;
        if (has_container_of(store, name, container_kind::hash)) return container_kind::hash;
        if (has_container_of(store, name, container_kind::ordered_map)) return container_kind::ordered_map;
        return container_kind::none;
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
    inline size_t remove_prefix(sharded_store& store, art::value_type name, art::value_type prefix) {
        size_t removed = 0;
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

    inline size_t remove_container(sharded_store& store, art::value_type name) {
        size_t removed = 0;
        auto field = conversion::convert(name);
        for (auto kind : {container_kind::list, container_kind::hash, container_kind::ordered_map}) {
            composite probe;
            removed += remove_prefix(store, name, probe.create(lead_of(kind), {field}, false));
        }
        // an ordered set keeps a second range, member to score, and it does not begin with
        // the name - the index marker comes first, so the sweep above walks straight past
        // it. Left behind, a deleted ordered set still answers ZSCORE for its members
        composite ix;
        removed += remove_prefix(store, name,
            ix.create(art::ts_ordered_map, {conversion::empty_component(), field}, false));
        return removed;
    }

    /**
     * Take `name` for a collection of this kind, or say who already has it.
     *
     * The kinds are separate key ranges, so nothing stops HSET and ZADD both succeeding on
     * one name - they would simply not see each other, which is two objects wearing the
     * same name rather than the miscount it used to be. This is where that is refused.
     *
     * It runs where a collection is created rather than in every command that touches one,
     * because that is where the case that matters arises and it keeps three cross shard
     * probes off the hot path. Call it before taking the command's own lock, for the
     * reasons in kind_of.
     *
     * Answers container_kind::none when the name is free or already this kind.
     */
    inline container_kind claim_container_kind(sharded_store& store, art::value_type name,
                                               container_kind want) {
        for (auto kind : {container_kind::list, container_kind::hash, container_kind::ordered_map}) {
            if (kind == want) continue;
            if (has_container_of(store, name, kind)) return kind;
        }
        return container_kind::none;
    }

    /**
     * May `name` be written as a container of this kind?
     *
     * The one call a creating command needs: false when a plain value already lives there,
     * and false when another kind of collection does. Both answer the same WRONGTYPE, which
     * is what redis says in either case.
     *
     * Three probes at worst and they route to shards of their own, so this goes before the
     * command takes its lock - see kind_of. Commands that only add to a collection which
     * must already exist do not need it; the ones that bring a collection into being do.
     */
    inline bool container_writable(sharded_store& store, art::value_type name, container_kind want) {
        auto plain = store.space()->encode_key(name);
        if (store.exists(plain.get_value())) {
            return false;
        }
        return claim_container_kind(store, name, want) == container_kind::none;
    }

    /** the message redis answers with, and the code clients branch on */
    inline const char *wrong_type_message() {
        return "WRONGTYPE Operation against a key holding the wrong kind of value";
    }
}

#endif //BARCH_KEY_TYPE_H
