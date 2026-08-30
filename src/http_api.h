#ifndef HTTP_API_H
#define HTTP_API_H

#include "barch_apis.h"

extern "C" {
    int HTTP(caller& call, const arg_t& argv);
}

namespace barch {
    void stop_http_servers();
    void stop_http_server(const std::string& space);
}

void register_http_api(function_map& r);

#endif
