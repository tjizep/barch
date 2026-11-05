#ifndef BARCH_FOREIGN_POOL_H
#define BARCH_FOREIGN_POOL_H

#include <functional>
#include <memory>

namespace barch {
namespace foreign {

void enqueue(std::function<void()> job);

/*
 * Function overtime - TODO 836. A stored function call past its deadline may go on
 * in a pool of its own, function_overtime_threads wide, so it doesn't hold the
 * workers that calls inside their deadline run on. At most function_overtime_max
 * calls are in overtime at once.
 */
void enqueue_overtime(std::function<void()> job);

/** a call's place in overtime; the place is given back when the last copy goes */
struct overtime_slot {
    overtime_slot() = default;
    overtime_slot(const overtime_slot&) = delete;
    overtime_slot& operator=(const overtime_slot&) = delete;
    ~overtime_slot();
};
using overtime_ptr = std::shared_ptr<overtime_slot>;

/** a place in overtime, or null when function_overtime_max is 0 or all are taken */
overtime_ptr enter_overtime();

}
}

#endif
