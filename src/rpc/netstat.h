//
// Created by teejip on 9/24/25.
//

#ifndef BARCH_NETSTAT_H
#define BARCH_NETSTAT_H
#include "statistics.h"
#include "ioutil.h"
struct net_stat {
    net_stat() = default;
    ~net_stat() {
        statistics::repl::bytes_recv += stream_read_ctr ;
        statistics::repl::bytes_sent += stream_write_ctr;
        stream_read_ctr = 0;
        stream_write_ctr = 0;
    }
};

#endif //BARCH_NETSTAT_H