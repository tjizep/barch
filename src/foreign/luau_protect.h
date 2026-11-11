#pragma once
/*
 * Reading what a script handed back, in protected mode - TODO 871.
 *
 * Reading a table runs script code. lua_getfield and lua_getglobal honour
 * __index, so a field read can raise. Outside a pcall that raise is a
 * lua_exception thrown through C++, and its what() reads the message off the
 * raising thread's stack. If the unwinding closes that thread's state first -
 * SETF's scratch state, HTTP START's vm - the catch reads a closed state and
 * the process dies on signal 4.
 *
 * protected_read runs `read` in a C function under lua_pcall on L. A raise
 * inside it stops there, and its text is copied into `err` while L is open.
 *
 *   - The top `nargs` values on L are its arguments, at 1..nargs in its own
 *     frame. They are popped either way.
 *   - `read` answers false, with `err` set, for a refusal of its own.
 *   - On true it leaves exactly `nresults` values on top of its frame, and
 *     those are on L afterwards. On false or a raise, L has nothing more.
 */
#include <string>

#include "lua.h"

namespace barch::foreign {

template <typename F>
bool protected_read(lua_State* L, int nargs, int nresults, std::string& err, F&& read) {
    struct frame {
        F* read;
        int nresults;
        bool ok;
    } f{&read, nresults, false};
    // a variable, not the lambda inline: lua_pushcclosure is a macro, and the
    // commas in the body would split its arguments
    lua_CFunction run = [](lua_State* T) -> int {
        auto* f = static_cast<frame*>(lua_touserdata(T, lua_upvalueindex(1)));
        f->ok = (*f->read)(T);
        return f->ok ? f->nresults : 0;
    };
    lua_pushlightuserdata(L, &f);
    lua_pushcclosure(L, run, "protected_read", 1);
    lua_insert(L, -(nargs + 1));
    if (lua_pcall(L, nargs, nresults, 0) != 0) {
        const char* s = lua_tostring(L, -1);
        err = s ? s : "luau error while reading a table";
        lua_pop(L, 1);
        return false;
    }
    if (!f.ok) {
        // the call returned nothing, and lua_pcall made it nresults nils
        lua_pop(L, nresults);
        return false;
    }
    return true;
}

} // namespace barch::foreign
