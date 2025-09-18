//
// Created by teejip on 9/4/25.
//

#ifndef BARCH_QUEUE_SERVER_H
#define BARCH_QUEUE_SERVER_H
#include "value_type.h"
#include "key_options.h"
extern void hash_queue_insert(size_t shard, art::key_options options,art::value_type k, art::value_type v);
extern void queue_insert(size_t shard,art::key_options options,art::value_type k, art::value_type v) ;
extern void queue_consume(size_t shard) ;
extern void queue_consume_all();
extern void start_queue_server();
extern void stop_queue_server();
extern bool is_queue_server_running();
#endif //BARCH_QUEUE_SERVER_H