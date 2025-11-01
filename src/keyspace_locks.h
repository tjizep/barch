//
// Created by teejip on 10/30/25.
//

#ifndef BARCH_KEYSPACE_LOCKS_H
#define BARCH_KEYSPACE_LOCKS_H
#include "key_space.h"
extern void lock_shared(barch::key_space_ptr ks) ;
extern void lock_unique(barch::key_space_ptr ks) ;
extern void unlock(barch::key_space_ptr ks) ;
extern void unlock_shared(barch::key_space_ptr ks);
struct ks_shared {
    barch::key_space_ptr spce;
    explicit ks_shared(barch::key_space_ptr s) : spce(std::move(s)) {
        lock_shared(spce);
    }
    ~ks_shared() {
        unlock(spce);
    }
};
struct ks_unique {
    barch::key_space_ptr spce;
    explicit ks_unique(barch::key_space_ptr s) : spce(std::move(s)) {
        lock_unique(spce);
    }
    ~ks_unique() {
        unlock(spce);
    }
};
#endif //BARCH_KEYSPACE_LOCKS_H