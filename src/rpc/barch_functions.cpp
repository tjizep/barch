//
// Created by teejip on 9/6/25.
//

#include "barch_apis.h"

namespace barch {
    std::shared_ptr<function_map> barch_functions = std::make_shared<function_map>(functions_by_name());
}
