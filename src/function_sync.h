#pragma once
#include <string>

namespace barch {
    /** apply the checkout. empty string is success; anything else is the reason.
     *  `pin` is a git rev for this call; empty uses functions_git_commit. */
    std::string sync_functions(const std::string& pin = {});
    std::string functions_sync_status();
    void start_function_sync();
    void stop_function_sync();
    /** wake a waiting poller, used by FUNCTIONS SYNC */
    void request_function_sync();
}
