#include "luau_internal.h"
#include "key_space.h"
#include "lzr_log.h"

namespace barch {
namespace foreign {

#ifdef BARCH_HAS_LUAU

struct luau_driver_t : driver {
    result fetch(std::string_view space, std::string_view key, uint64_t deadline_ms) override {
        result out;
        detail::start_resolve(space, key, deadline_ms, false, [&](result r) { out = std::move(r); });
        return out;
    }

    void fetch_async(std::string_view space, std::string_view key, uint64_t deadline_ms,
                     std::function<void(result)> done) override {
        detail::start_resolve(space, key, deadline_ms, true, std::move(done));
    }
};


/*
 * Nothing is compiled here any more.
 *
 * This runs from the key_space constructor, before the space has any shards, so the
 * function the script now lives in cannot be read yet. The first fill compiles it and
 * the space keeps the bytecode after that - which also means a script stored *after*
 * its space was built starts working, where before the space had to be rebuilt.
 *
 * All that is left to check is that a name was given at all. See TODO 139.
 */
bool prepare_luau(key_space& ks) {
    if (ks.foreign_script.empty()) {
        barch::err({"luau foreign source needs a function name - ignoring it for space",
                    ks.get_name()});
        return false;
    }
    return true;
}



bool luau_available() {
    return true;
}

driver& luau_driver() {
    static luau_driver_t d;
    return d;
}


#else

lock_wait*& script_lock_wait() {
    thread_local lock_wait* w = nullptr;
    return w;
}

bool lock_wait::can_park() const {
    return on_fiber;
}

bool package_setup(const std::string&, std::string&, std::string& err) {
    err = "luau not built";
    return false;
}

bool prepare_luau(key_space& ks) {
    barch::err({"luau not built - ignoring it for space", ks.get_name()});
    return false;
}

bool compile_function(const std::string&, const std::string&, const std::string&,
                      const source_loader&, std::string& err, resp_spec*, cron_spec*) {
    err = "luau not built";
    return false;
}

struct function_states {};

function_states_ptr make_function_states() {
    return nullptr;
}

function_states_ptr make_function_states(std::shared_ptr<std::atomic<uint64_t>>) {
    return nullptr;
}

parked_call_ptr park_call(lua_State*) {
    return nullptr;
}

int park_yield(lua_State*) {
    return 0;   // park_call never parks here
}

void complete_call(const parked_call_ptr&, push_results) {
}

uint64_t blocking_wait_cap(lua_State*) {
    return 0;
}

uint64_t park_wait_cap(lua_State*) {
    return 0;
}

int64_t blocking_wait_start(lua_State*) {
    return 0;
}

void blocking_wait_end(lua_State*, int64_t) {
}

void start_function(const std::string&, const std::string&, const call_interface_ptr&,
                    const heap::vector<std::string>&, const call_limits&,
                    const function_states_ptr&, const function_done& done,
                    const std::string&) {
    done(false, Variable(nullptr), "luau not built");
}

bool http_vm_load(http_vm&, const std::string&, const std::string&, http_route&,
                  std::string& err) {
    err = "luau not built";
    return false;
}

void http_vm_start(http_vm&, int, const void*, void*, const std::vector<http_binding>*,
                   uint64_t, http_call call) {
    call.done("luau not built");
}


bool luau_available() {
    return false;
}

struct luau_stub : driver {
    result fetch(std::string_view, std::string_view, uint64_t) override {
        return {result::status::error, "FOREIGN luau not built"};
    }
};

driver& luau_driver() {
    static luau_stub d;
    return d;
}

#endif

const char* const call_stopped_error =
    "FUNCTION timeout: its caller stopped waiting (foreign_timeout_ms)";

}
}
