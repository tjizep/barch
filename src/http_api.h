#ifndef HTTP_API_H
#define HTTP_API_H

#include "barch_apis.h"

extern "C" {
    int HTTP(caller& call, const arg_t& argv);
}

namespace barch {
    void stop_http_servers();
    void stop_http_server(const std::string& space);
    /**
     * Have this space serve HTTP the way `HTTP START key port bind` would - TODO 582.
     * A server already running from the same request is left alone; a different one
     * is stopped and started again. Empty is success, and `started` says whether a
     * server was (re)started.
     */
    std::string ensure_http_server(const key_space_ptr& space, const std::string& key,
                                   uint16_t port, const std::string& bind, bool& started);
}

void register_http_api(function_map& r);

#endif
