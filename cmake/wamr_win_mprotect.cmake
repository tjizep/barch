# Fix os_mprotect in WAMR's Windows platform - TODO 832.
#
# WAMR 2.4.5's os_mprotect calls VirtualProtect with NULL for the old protection and
# returns VirtualProtect's BOOL. Windows wants a real pointer there, and WAMR's callers
# read 0 as success. On Windows the call fails (NULL pointer), returns 0, and the
# callers take that as success; wine reads through the NULL and crashes barchd on the
# first instance. The pages are already committed by then, so the fix is to ask
# properly and say 0 when it worked. A zero size changes nothing and succeeds, as
# mprotect does.
#
# Applied the same way as wamr_budget_hook.cmake: idempotent, run on every configure,
# and the configure fails if the anchor isn't there.

if (NOT DEFINED WAMR_WIN_MEMMAP_C)
    message(FATAL_ERROR "wamr_win_mprotect: WAMR_WIN_MEMMAP_C is not set")
endif ()
if (NOT EXISTS "${WAMR_WIN_MEMMAP_C}")
    message(FATAL_ERROR "wamr_win_mprotect: no such file ${WAMR_WIN_MEMMAP_C}")
endif ()

file(READ "${WAMR_WIN_MEMMAP_C}" _src)
if (_src MATCHES "barch_old_protect")
    return()                                    # already patched
endif ()

set(_anchor "    return VirtualProtect((LPVOID)addr, request_size, protect, NULL);")
string(FIND "${_src}" "${_anchor}" _at)
if (_at EQUAL -1)
    message(FATAL_ERROR "wamr_win_mprotect: os_mprotect in ${WAMR_WIN_MEMMAP_C} has changed")
endif ()
string(REPLACE "${_anchor}" "    DWORD barch_old_protect;
    if (request_size == 0)
        return 0;
    return VirtualProtect((LPVOID)addr, request_size, protect, &barch_old_protect)
               ? 0
               : -1;" _src "${_src}")
file(WRITE "${WAMR_WIN_MEMMAP_C}" "${_src}")
message(STATUS "Patched WAMR's os_mprotect for Windows (TODO 832)")
