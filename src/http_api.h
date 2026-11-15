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
    /**
     * Push a frame to one of this space's open websockets, by the id its open event
     * gave - TODO 909. 1 is sent, 0 is no such socket open here, -1 is an error in
     * `err`. Not tied to a transaction or the Raft commit: it goes out at once.
     */
    int ws_send(const key_space_ptr& space, const std::string& id, const std::string& data,
                bool binary, std::string& err);
    /** close one of this space's websockets with a code and reason; answers as ws_send */
    int ws_close(const key_space_ptr& space, const std::string& id, int code,
                 const std::string& reason, std::string& err);
}

void register_http_api(function_map& r);

#endif
