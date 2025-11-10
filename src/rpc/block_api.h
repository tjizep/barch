//
// Created by teejip on 11/9/25.
//

#ifndef BARCH_BLOCK_API_H
#define BARCH_BLOCK_API_H
#include "asio_resp_session.h"
#include "sastam.h"

namespace barch {
    void add_rpc_blocks(const heap::vector<std::string>& keys, const barch::resp_session_ptr& ptr);
    void erase_rpc_blocks(const heap::vector<std::string>& keys, const barch::resp_session_ptr& ptr);
    void call_unblock(const std::string& key);
}
#endif //BARCH_BLOCK_API_H