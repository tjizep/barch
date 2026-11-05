# WAMR, the WebAssembly runtime stored WASM functions run in - TODO 820, WASM-PLAN.md.
#
# A directory of its own because WAMR's build scripts set everything with
# add_definitions and include_directories, which are directory-wide. Included from
# the top level they'd put BUILD_TARGET_X86_64, WASM_ENABLE_* and WAMR's internal
# include paths on every barch target. Here they stop at this directory, and
# barch_wamr hands out only the public include dir.
#
# WAMR_ROOT_DIR comes from the top level, which fetched the source.

# fast interpreter only. No AOT or JIT (nothing compiled to native code at run
# time), no WASI and no libc builtins (a module reaches only the `barch` imports),
# no threads, no SIMD.
if (WIN32)
    set(WAMR_BUILD_PLATFORM windows)
else ()
    set(WAMR_BUILD_PLATFORM linux)
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
    set(WAMR_BUILD_TARGET X86_64)
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    set(WAMR_BUILD_TARGET AARCH64)
else ()
    message(FATAL_ERROR "ADD_WASM: no WAMR target for ${CMAKE_SYSTEM_PROCESSOR}")
endif ()
set(WAMR_BUILD_INTERP 1)
set(WAMR_BUILD_FAST_INTERP 1)
set(WAMR_BUILD_AOT 0)
set(WAMR_BUILD_JIT 0)
set(WAMR_BUILD_FAST_JIT 0)
set(WAMR_BUILD_LIBC_BUILTIN 0)
set(WAMR_BUILD_LIBC_WASI 0)
set(WAMR_BUILD_LIB_PTHREAD 0)
set(WAMR_BUILD_THREAD_MGR 0)
set(WAMR_BUILD_MULTI_MODULE 0)
set(WAMR_BUILD_SIMD 0)
set(WAMR_BUILD_REF_TYPES 1)
set(WAMR_BUILD_BULK_MEMORY 1)
# the instruction cap that stops a runaway, and the custom section the
# function's settings live in
set(WAMR_BUILD_INSTRUCTION_METERING 1)
set(WAMR_BUILD_LOAD_CUSTOM_SECTION 1)
# Bounds checks in software. The hardware way reserves 8 GB of address space per
# instance and catches faults with a SIGSEGV handler, which would sit on top of
# barch's own handlers.
set(WAMR_DISABLE_HW_BOUND_CHECK 1)
# WAMR writes the GS base by default, to find linear memory faster. barch's
# threads aren't WAMR's, and nothing else expects GS to change under it.
set(WAMR_DISABLE_WRITE_GS_BASE 1)

enable_language(ASM)
include(${WAMR_ROOT_DIR}/build-scripts/runtime_lib.cmake)

add_library(barch_wamr STATIC ${WAMR_RUNTIME_LIB_SOURCE})
set_target_properties(barch_wamr PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(barch_wamr INTERFACE ${WAMR_ROOT_DIR}/core/iwasm/include)
# invokeNative_em64.s has no .note.GNU-stack, so the linker would mark the whole
# barchd or _barch.so as needing an executable stack. PE has no such note.
if (NOT WIN32)
    target_compile_options(barch_wamr PRIVATE $<$<COMPILE_LANGUAGE:ASM>:-Wa,--noexecstack>)
    target_link_libraries(barch_wamr PUBLIC m)
endif ()
# WAMR checks for native stack overflow by comparing a local's address with the
# stack boundary. ASan's fake stacks (use-after-return) move that local to the heap,
# so every call on a fiber looked like an overflow. Keep WAMR's locals on the real
# stack; the rest of barch keeps the use-after-return checks. TODO 821
if (SANITIZE STREQUAL "address" AND CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(barch_wamr PRIVATE $<$<COMPILE_LANGUAGE:C>:--param=asan-use-after-return=0>)
elseif (SANITIZE STREQUAL "address")
    target_compile_options(barch_wamr PRIVATE $<$<COMPILE_LANGUAGE:C>:-fsanitize-address-use-after-return=never>)
endif ()
