#ifndef BARCH_FOREIGN_WASM_DRIVER_H
#define BARCH_FOREIGN_WASM_DRIVER_H

//
// Stored functions written in WebAssembly - TODO 820, WASM-PLAN.md.
//
// The Luau driver owns the call path and hands a call over here once it knows the
// source is a module, so CALLF, the function key, the ACL, the limits and Raft are the
// same for both. Nothing outside wasm_driver.cpp includes a WAMR header.
//

#include "driver.h"

#include <memory>
#include <string>

namespace barch::foreign {

/**
 * The WASM instances one session holds, one per function it has called, kept between
 * calls the way its Luau state keeps compiled functions. Lives in function_states.
 */
struct wasm_sessions;
typedef std::shared_ptr<wasm_sessions> wasm_sessions_ptr;
wasm_sessions_ptr make_wasm_sessions();

/**
 * SETF's check: the module loads, imports only `barch` functions that exist, exports
 * `call() -> i32`, has an `i32` `arity` if it has one, and runs nothing while it's
 * instantiated. False with `err` otherwise.
 *
 * The transport in its barch section goes in whichever specs aren't null - TODO 823.
 * A cron or queue one excuses `call`, as it does for Luau, and every export a resp
 * method names has to be a `() -> i32` function as well.
 */
bool check_wasm(const std::string& name, const std::string& source, std::string& err,
                resp_spec* resp = nullptr, cron_spec* cron = nullptr, queue_spec* queue = nullptr);

/**
 * A call to a function this session already has an instance for, still current.
 * False when it hasn't one, or the one it had went stale and was dropped; the caller
 * then loads the source and comes back through `start_wasm`.
 */
bool start_wasm_warm(const wasm_sessions_ptr& sessions, const std::string& key, const std::string& name,
                     const call_interface_ptr& iface, const heap::vector<std::string>& args,
                     const call_limits& limits, const function_done& done,
                     const std::string& entry);

/** a call with the module's source in hand: an instance is built for it */
void start_wasm(const wasm_sessions_ptr& sessions, const std::string& key, const std::string& name,
                const std::string& source, const call_interface_ptr& iface,
                const heap::vector<std::string>& args, const call_limits& limits,
                const function_done& done, const std::string& entry);

}

#endif
