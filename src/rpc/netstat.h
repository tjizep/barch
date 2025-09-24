//
// Created by teejip on 9/24/25.
//

#ifndef BARCH_NETSTAT_H
#define BARCH_NETSTAT_H
#include "statistics.h"
#include "ioutil.h"
struct net_stat {
    uint64_t saved_writes = stream_write_ctr;
    uint64_t saved_reads = stream_read_ctr;
    net_stat() = default;
    ~net_stat() {
        statistics::repl::bytes_recv += stream_read_ctr - saved_reads;
        statistics::repl::bytes_sent += stream_write_ctr - saved_writes;
    }
};

#endif //BARCH_NETSTAT_H