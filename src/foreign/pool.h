#ifndef BARCH_FOREIGN_POOL_H
#define BARCH_FOREIGN_POOL_H

#include <functional>

namespace barch {
namespace foreign {

void enqueue(std::function<void()> job);

}
}

#endif
