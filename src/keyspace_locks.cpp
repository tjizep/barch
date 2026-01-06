//
// Created by teejip on 10/30/25.
//
#include "keyspace_locks.h"
#include "abstract_shard.h"
void lock_shared(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:ks->get_shards()) {
        shard->get_latch().lock_shared();
    }
}
void lock_unique(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:ks->get_shards()) {
        shard->get_latch().lock();
    }
}
void unlock(barch::key_space_ptr ks) {
    if (!ks) return;
    ;
    for (size_t shard = ks->get_shard_count(); shard > 0; --shard) {
        auto t = ks->get(shard - 1);
        t->get_latch().unlock();
    }
}
void unlock_shared(barch::key_space_ptr ks) {
    if (!ks) return;
    for (size_t shard = ks->get_shard_count(); shard > 0; --shard) {
        auto t = ks->get(shard - 1);
        t->get_latch().unlock_shared();
    }
}