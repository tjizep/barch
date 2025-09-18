//
// Created by teejip on 9/6/25.
//

#ifndef BARCH_BARCH_FUNCTIONS_H
#define BARCH_BARCH_FUNCTIONS_H
#include "barch_apis.h"

namespace barch {
    // users of barch functions should just save the pointer before using
    // i.e.
    // auto bf = barch_functions // take snapshot
    // ... = bf->find(...) // use
    // ... // auto release snapshot
    //
    // Then, runtime changes to barch_functions should be by CoW (copy on write)
    // i.e.
    // new_barch_functions = std::make_shared<...>(*barch_functions)
    // new_barch_functions->insert(...)
    // (*new_barch_functions)[...] = ...
    // ...
    // barch_functions = new_barch_functions
    //
    extern std::shared_ptr<function_map> barch_functions;
}
#endif //BARCH_BARCH_FUNCTIONS_H