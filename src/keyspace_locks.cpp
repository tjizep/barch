//
// Created by teejip on 10/30/25.
//
#include "keyspace_locks.h"
#include "abstract_shard.h"
#include "queue_server.h"
void lock_shared(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:barch::get_shard_count()) {
        auto t = ks->get(shard);
        queue_consume(t);
        t->get_latch().lock_shared();
    }
}
void lock_unique(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:barch::get_shard_count()) {
        auto t = ks->get(shard);
        queue_consume(t);
        t->get_latch().lock();
    }
}
void unlock(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:barch::get_shard_count()) {
        auto t = ks->get(shard);
        t->get_latch().unlock();
    }
}
void unlock_shared(barch::key_space_ptr ks) {
    if (!ks) return;
    for (auto shard:barch::get_shard_count()) {
        auto t = ks->get(shard);
        t->get_latch().unlock_shared();
    }
}