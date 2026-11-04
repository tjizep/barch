# Let barch top up a WASM call's instruction budget instead of WAMR stopping it.
#
# WAMR's fast interpreter counts instructions down from exec_env->instructions_to_execute
# and traps with "instruction limit exceeded" at zero. That's all it offers: no callback,
# no way to pause. barch needs to come back every few thousand instructions to check the
# CPU deadline and, when a slice is spent, switch the call's fiber back to the worker so
# the call gets requeued like a Luau one. See TODO 821 and WASM-PLAN.md.
#
# So at zero the patched interpreter calls barch_wasm_budget_hook, if it's set, and
# carries on with whatever count it returns. A zero from the hook means stop, and the
# trap is the same one WAMR raised before. The hook is a pointer rather than a function
# barch defines, so WAMR still links on its own and an unset hook changes nothing.
#
# Applied as a script for the same reasons as crow_tcp_nodelay.cmake: idempotent, run
# on every configure, and it fails the configure if the anchor isn't there rather than
# building a server whose WASM calls can't be sliced or timed out.

if (NOT DEFINED WAMR_INTERP_FAST_C)
    message(FATAL_ERROR "wamr_budget_hook: WAMR_INTERP_FAST_C is not set")
endif ()
if (NOT EXISTS "${WAMR_INTERP_FAST_C}")
    message(FATAL_ERROR "wamr_budget_hook: no such file ${WAMR_INTERP_FAST_C}")
endif ()

file(READ "${WAMR_INTERP_FAST_C}" _wamr_src)

# The second part, TODO 823: a host function that calls a guest back through its
# function table (store_locked) has to know what the table slot holds before it calls
# it. WAMR's own call_indirect from native code checks nothing about the type, and the
# only public way to a table is through an export, which a C module doesn't have. So
# this hands back the function in table 0 at `elem`, or NULL when the slot is empty or
# holds an import. Appended, so it needs no anchor; it has its own already-patched
# check so a tree patched before it existed still gets it.
if (NOT _wamr_src MATCHES "barch_wasm_table_func")
    string(APPEND _wamr_src "
/* barch_wasm_table_func: the module's own function in table 0 at `elem`, or NULL.
   See barch's cmake/wamr_budget_hook.cmake. */
WASMFunctionInstance *
barch_wasm_table_func(WASMModuleInstance *module_inst, uint32 elem)
{
    WASMTableInstance *table;
    table_elem_type_t value;
    uint32 func_idx;
    WASMFunctionInstance *func;
    if (!module_inst || module_inst->table_count == 0)
        return NULL;
    table = module_inst->tables[0];
    if (!table || elem >= table->cur_size)
        return NULL;
    value = ((table_elem_type_t *)table->elems)[elem];
    if (value == NULL_REF)
        return NULL;
#if WASM_ENABLE_GC == 0
    func_idx = (uint32)value;
#else
    func_idx = wasm_func_obj_get_func_idx_bound((WASMFuncObjectRef)value);
#endif
    if (func_idx >= module_inst->e->function_count)
        return NULL;
    func = module_inst->e->functions + func_idx;
    return func->is_import_func ? NULL : func;
}
")
    file(WRITE "${WAMR_INTERP_FAST_C}" "${_wamr_src}")
    message(STATUS "Patched WAMR's interpreter for barch's table lookup (TODO 823)")
endif ()

if (_wamr_src MATCHES "barch_wasm_budget_hook")
    return()                                    # already patched
endif ()

set(_anchor "#if WASM_ENABLE_INSTRUCTION_METERING != 0
#define CHECK_INSTRUCTION_LIMIT()                                 \\
    if (instructions_left == 0) {                                 \\
        wasm_set_exception(module, \"instruction limit exceeded\"); \\
        goto got_exception;                                       \\
    }                                                             \\
    else if (instructions_left > 0)                               \\
        instructions_left--;
")
set(_patched "#if WASM_ENABLE_INSTRUCTION_METERING != 0
/* barch_wasm_budget_hook: called when the count runs out. It returns the next count,
   or 0 to stop the call. It may switch the calling fiber away and back, so nothing
   here may hold state across it except the locals the interpreter already keeps.
   See barch's cmake/wamr_budget_hook.cmake. */
int (*barch_wasm_budget_hook)(WASMExecEnv *exec_env) = NULL;
#define CHECK_INSTRUCTION_LIMIT()                                     \\
    if (instructions_left == 0) {                                     \\
        if (barch_wasm_budget_hook)                                   \\
            instructions_left = barch_wasm_budget_hook(exec_env);     \\
        if (instructions_left == 0) {                                 \\
            wasm_set_exception(module, \"instruction limit exceeded\"); \\
            goto got_exception;                                       \\
        }                                                             \\
    }                                                                 \\
    if (instructions_left > 0)                                        \\
        instructions_left--;
")

string(FIND "${_wamr_src}" "${_anchor}" _at)
if (_at EQUAL -1)
    message(FATAL_ERROR
        "wamr_budget_hook: CHECK_INSTRUCTION_LIMIT in ${WAMR_INTERP_FAST_C} is not the "
        "shape this patch expects. WAMR has changed - look at how it meters instructions "
        "now and update the anchor. See TODO 821.")
endif ()

string(REPLACE "${_anchor}" "${_patched}" _wamr_src "${_wamr_src}")
file(WRITE "${WAMR_INTERP_FAST_C}" "${_wamr_src}")
message(STATUS "Patched WAMR's interpreter for barch's budget hook (TODO 821)")
