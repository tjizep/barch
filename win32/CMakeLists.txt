# barchd for Windows, built with MinGW-w64 (gcc). See TODO 598 and win32/README.md.
#
# This is a project of its own rather than a branch inside the top level
# CMakeLists.txt. That file builds three targets for Linux - the python
# extension, the valkey module and barchd - plus the tests, and most of what it
# does (liburing, SWIG, the sanitizer runs, the test harness that shells out to
# make) has no meaning on Windows. Keeping Windows here means the Linux build
# can't be broken by it, and the other way round.
#
# Only barchd is built. lbarch is a valkey module and valkey doesn't run on
# Windows; the python binding can come later.
#
# The dependencies are the same ones at the same versions as the Linux build,
# fetched the same way, except OpenSSL, which comes from the system - MSYS2's
# mingw-w64-x86_64-openssl on GitHub. If you bump a version in the top level
# file, bump it here too.
#
# On Windows, from an MSYS2 MINGW64 shell:
#
#   cmake -S win32 -B build-win -G Ninja
#   cmake --build build-win --target barchd
#
# Cross compiling from Linux:
#
#   cmake -S win32 -B build-win -G Ninja \
#         -DCMAKE_TOOLCHAIN_FILE=win32/mingw-cross.cmake \
#         -DOPENSSL_ROOT_DIR=<an unpacked mingw-w64-x86_64-openssl>/mingw64
#
cmake_minimum_required(VERSION 3.22)
project(barch_win32 VERSION 0.5.8 LANGUAGES C CXX)

if (NOT WIN32)
    message(FATAL_ERROR "win32/CMakeLists.txt builds for Windows. Use a MinGW-w64 "
            "compiler, or -DCMAKE_TOOLCHAIN_FILE=win32/mingw-cross.cmake from Linux. "
            "The Linux build is the CMakeLists.txt one directory up.")
endif ()
if (MSVC)
    message(FATAL_ERROR "barch builds with gcc. Use MinGW-w64 (MSYS2), not MSVC.")
endif ()

set(BARCH_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
cmake_path(NORMAL_PATH BARCH_ROOT)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

# The Linux build uses -march=native, which is fine for a binary that runs where
# it was built. A Windows binary off GitHub runs on whatever the user has, so the
# default is x86-64-v3 (AVX2, Haswell and later). Set it to x86-64-v2 for older
# machines, or native for a local build.
set(BARCH_MARCH "x86-64-v3" CACHE STRING "-march for the Windows build")

execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY ${BARCH_ROOT}
        OUTPUT_VARIABLE GIT_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
if (NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH "unknown")
endif ()
set(PROJECT_VERSION ${barch_win32_VERSION})
configure_file(${BARCH_ROOT}/src/version.h.in ${CMAKE_CURRENT_BINARY_DIR}/generated/version.h)
message("barch version: ${PROJECT_VERSION} (commit: ${GIT_COMMIT_HASH}) for Windows")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=${BARCH_MARCH} -Wall -Wextra -Wsign-compare -Wno-format-zero-length -Wpointer-arith -Wno-missing-braces -Wno-missing-field-initializers -Wno-missing-attributes")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=${BARCH_MARCH}")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -funroll-all-loops -frename-registers")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -fno-omit-frame-pointer")

# Windows 10 1803 and later: AF_UNIX sockets, which asio's local::stream_protocol
# needs, and the condition variable / SRW lock apis winpthreads uses
add_compile_definitions(_WIN32_WINNT=0x0A00 WINVER=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)

include(FetchContent)

# ---- dependencies, matching the top level CMakeLists.txt ---------------------

set(OPENSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL REQUIRED COMPONENTS Crypto SSL)

FetchContent_Declare(
        asio
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        URL "https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-36-0.tar.gz")
FetchContent_MakeAvailable(asio)

set(ZSTD_BUILD_STATIC ON)
set(ZSTD_BUILD_SHARED OFF)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        zstd
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SOURCE_SUBDIR build/cmake
        URL "https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz")

FetchContent_Declare(
        fast_float
        GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
        GIT_TAG main)
FetchContent_MakeAvailable(fast_float)

FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt
        GIT_TAG 123913715afeb8a437e6388b4473fcc4753e1c9a) # 11.4
set(BUILD_SHARED_LIBS OFF)
FetchContent_MakeAvailable(fmt)
FetchContent_MakeAvailable(zstd)

# Luau gets none of our warning flags - same as on Linux
set(LUAU_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(LUAU_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LUAU_BUILD_WEB OFF CACHE BOOL "" FORCE)
set(LUAU_WERROR OFF CACHE BOOL "" FORCE)
set(_barch_cxx_flags_saved "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-march=${BARCH_MARCH}")
FetchContent_Declare(
        luau
        GIT_REPOSITORY https://github.com/luau-lang/luau.git
        GIT_TAG 0.734
        GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(luau)
set(CMAKE_CXX_FLAGS "${_barch_cxx_flags_saved}")
foreach (_luau_tgt IN ITEMS Luau.Common Luau.Ast Luau.Bytecode Luau.Compiler Luau.VM
        Luau.CodeGen Luau.Config Luau.Analysis Luau.Inliner Luau.Require)
    if (TARGET ${_luau_tgt})
        target_compile_options(${_luau_tgt} PRIVATE -w)
    endif ()
endforeach ()

if (POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif ()

FetchContent_Declare(
        numkong
        GIT_REPOSITORY https://github.com/ashvardanian/NumKong.git
        GIT_TAG main
        GIT_SHALLOW TRUE)
FetchContent_GetProperties(numkong)
if (NOT numkong_POPULATED)
    FetchContent_Populate(numkong)
endif ()
# NumKong's allocator picks _aligned_malloc on _MSC_VER and std::aligned_alloc
# everywhere else, and MinGW's crt has no aligned_alloc. _WIN32 is the test it
# means. Rewritten every configure, like the Crow patch, and idempotent
file(READ ${numkong_SOURCE_DIR}/include/numkong/vector.hpp _nk_vector)
string(REPLACE "#if defined(_MSC_VER)\n        return static_cast<value_type *>(::_aligned_malloc"
        "#if defined(_WIN32)\n        return static_cast<value_type *>(::_aligned_malloc" _nk_patched "${_nk_vector}")
string(REPLACE "#if defined(_MSC_VER)\n        if (p) ::_aligned_free(p);"
        "#if defined(_WIN32)\n        if (p) ::_aligned_free(p);" _nk_patched "${_nk_patched}")
if (NOT _nk_patched STREQUAL _nk_vector)
    file(WRITE ${numkong_SOURCE_DIR}/include/numkong/vector.hpp "${_nk_patched}")
    message(STATUS "numkong: aligned allocation switched to _aligned_malloc for MinGW")
elseif (NOT _nk_vector MATCHES "#if defined\\(_WIN32\\)\n        return static_cast<value_type \\*>\\(::_aligned_malloc")
    message(FATAL_ERROR "numkong's vector.hpp has moved; the MinGW aligned_alloc patch no longer applies")
endif ()
add_library(numkong INTERFACE)
target_include_directories(numkong INTERFACE ${numkong_SOURCE_DIR}/include)
file(GLOB _numkong_dispatch_srcs "${numkong_SOURCE_DIR}/c/numkong.c"
        "${numkong_SOURCE_DIR}/c/dispatch_*.c")
add_library(numkong_dispatch STATIC ${_numkong_dispatch_srcs})
target_include_directories(numkong_dispatch PUBLIC ${numkong_SOURCE_DIR}/include)
target_compile_definitions(numkong_dispatch PUBLIC NK_DYNAMIC_DISPATCH=1)
target_compile_options(numkong_dispatch PRIVATE -w)

FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG f9c973a359ea7de2a4ae02ffec95bae85f68a6d7)
FetchContent_GetProperties(simdjson)
if (NOT simdjson_POPULATED)
    FetchContent_Populate(simdjson)
endif ()
add_library(simdjson_amalg OBJECT ${simdjson_SOURCE_DIR}/singleheader/simdjson.cpp)
target_include_directories(simdjson_amalg PUBLIC ${simdjson_SOURCE_DIR}/singleheader)
target_compile_options(simdjson_amalg PRIVATE -w)

FetchContent_Declare(
        crow
        GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
        GIT_TAG v1.2.1.2
        GIT_SHALLOW TRUE)
FetchContent_GetProperties(crow)
if (NOT crow_POPULATED)
    FetchContent_Populate(crow)
endif ()
# Crow's acceptor takes asio's default, SO_REUSEADDR, which on Windows lets a
# second process bind a port another is already listening on - two barchds would
# split an HTTP transport's connections between them. Ask for no reuse instead,
# so the second one's bind fails. Rewritten every configure, idempotent - TODO 599
file(READ ${crow_SOURCE_DIR}/include/crow/http_server.h _crow_server)
string(REPLACE "acceptor_(io_context_,endpoint),"
        "acceptor_(io_context_,endpoint,false)," _crow_patched "${_crow_server}")
if (NOT _crow_patched STREQUAL _crow_server)
    file(WRITE ${crow_SOURCE_DIR}/include/crow/http_server.h "${_crow_patched}")
    message(STATUS "crow: acceptor without SO_REUSEADDR for Windows")
elseif (NOT _crow_server MATCHES "acceptor_\\(io_context_,endpoint,false\\),")
    message(FATAL_ERROR "crow's http_server.h has moved; the SO_REUSEADDR patch no longer applies")
endif ()
execute_process(
        COMMAND ${CMAKE_COMMAND}
                -DCROW_HTTP_SERVER_H=${crow_SOURCE_DIR}/include/crow/http_server.h
                -P ${BARCH_ROOT}/cmake/crow_tcp_nodelay.cmake
        RESULT_VARIABLE _crow_patch_rc
        ERROR_VARIABLE _crow_patch_err)
if (NOT _crow_patch_rc EQUAL 0)
    message(FATAL_ERROR "could not patch Crow: ${_crow_patch_err}")
endif ()

# curl, cut down the same way as on Linux: HTTP(S) and SMTP(S) over OpenSSL
set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_LIBCURL_DOCS OFF CACHE BOOL "" FORCE)
set(BUILD_MISC_DOCS OFF CACHE BOOL "" FORCE)
set(ENABLE_CURL_MANUAL OFF CACHE BOOL "" FORCE)
set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
set(HTTP_ONLY OFF CACHE BOOL "" FORCE)
foreach (_curl_proto DICT FILE FTP GOPHER IMAP IPFS MQTT POP3 RTSP SMB TELNET TFTP WEBSOCKETS)
    set(CURL_DISABLE_${_curl_proto} ON CACHE BOOL "" FORCE)
endforeach ()
set(CURL_DISABLE_SMTP OFF CACHE BOOL "" FORCE)
set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
set(CURL_USE_SCHANNEL OFF CACHE BOOL "" FORCE)
set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
set(CURL_DISABLE_LDAP ON CACHE BOOL "" FORCE)
set(CURL_DISABLE_LDAPS ON CACHE BOOL "" FORCE)
set(CURL_BROTLI OFF CACHE BOOL "" FORCE)
set(CURL_ZSTD OFF CACHE BOOL "" FORCE)
set(CURL_ZLIB OFF CACHE BOOL "" FORCE)
# no HTTP/2, as on Linux. Said outright rather than left to detection: MSYS2
# has nghttp2 installed, curl finds it, and the static link then fails on
# __imp_nghttp2_* because only the dll import library is there
set(USE_NGHTTP2 OFF CACHE BOOL "" FORCE)
set(USE_NGTCP2 OFF CACHE BOOL "" FORCE)
set(USE_QUICHE OFF CACHE BOOL "" FORCE)
set(USE_MSH3 OFF CACHE BOOL "" FORCE)
set(USE_LIBRTMP OFF CACHE BOOL "" FORCE)
set(CURL_USE_GSSAPI OFF CACHE BOOL "" FORCE)
set(USE_LIBIDN2 OFF CACHE BOOL "" FORCE)
set(USE_WIN32_IDN OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        curl
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        URL "https://github.com/curl/curl/releases/download/curl-8_19_0/curl-8.19.0.tar.gz")
FetchContent_MakeAvailable(curl)

# ---- the barch sources, compiled once for barchd and the python module -------

file(GLOB SOURCES
        "${BARCH_ROOT}/src/*.c" "${BARCH_ROOT}/src/*.cpp" "${BARCH_ROOT}/src/*.cxx"
        "${BARCH_ROOT}/src/rpc/*.cpp" "${BARCH_ROOT}/src/art/*.cpp" "${BARCH_ROOT}/src/io/*.cpp"
        "${BARCH_ROOT}/src/keys/*.cpp" "${BARCH_ROOT}/src/shard/*.cpp"
        "${BARCH_ROOT}/src/parsers/*.cpp" "${BARCH_ROOT}/src/foreign/*.cpp")

# what everything compiled against barch's headers needs: the sources here, the
# SWIG wrapper and barchd's main
function(barch_win32_settings tgt)
    target_include_directories(${tgt} BEFORE PRIVATE
            ${BARCH_ROOT}/win32/include
            ${luau_SOURCE_DIR}/VM/include
            ${luau_SOURCE_DIR}/Compiler/include
            ${luau_SOURCE_DIR}/CodeGen/include)
    target_include_directories(${tgt} PRIVATE
            ${CMAKE_CURRENT_BINARY_DIR}/generated
            ${BARCH_ROOT}/src
            ${BARCH_ROOT}/external/include
            ${asio_SOURCE_DIR}/asio/include
            ${fast_float_SOURCE_DIR}/include
            ${zstd_SOURCE_DIR}/lib
            ${crow_SOURCE_DIR}/include)
    target_compile_definitions(${tgt} PRIVATE
            BARCH_HAS_LUAU BARCH_HAS_SIMDJSON BARCH_HAS_CROW BARCH_HAS_COFETCH
            ASIO_STANDALONE CROW_DISABLE_STATIC_DIR CROW_ENABLE_SSL CURL_STATICLIB)
    # on the object library this only carries the headers and definitions along;
    # the libraries themselves go into whatever links barch_core
    target_link_libraries(${tgt} PRIVATE
            Luau.Compiler Luau.VM Luau.CodeGen
            numkong numkong_dispatch simdjson_amalg
            libcurl_static libzstd_static fmt::fmt
            OpenSSL::SSL OpenSSL::Crypto
            ws2_32 mswsock crypt32 bcrypt iphlpapi secur32 psapi dbghelp)
    target_compile_options(${tgt} PRIVATE -Wno-undef -Wno-unused-parameter -Wno-class-memaccess
            "SHELL:-include ${BARCH_ROOT}/win32/include/barch_win32.h")
endfunction()

# and what links them in: -static so there are no libstdc++, libgcc or
# winpthread dlls to carry around beside the exe or the pyd
function(barch_win32_link tgt)
    target_link_libraries(${tgt} PRIVATE barch_core)
    target_link_options(${tgt} PRIVATE -static)
endfunction()

add_library(barch_core OBJECT ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/posix_compat.cpp)
barch_win32_settings(barch_core)
# the compat layer has file local names (mapping, release) that a unity batch
# would share with barch's, and it wants none of barch's headers
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/src/posix_compat.cpp PROPERTIES
        SKIP_UNITY_BUILD_INCLUSION ON SKIP_PRECOMPILE_HEADERS ON)
target_precompile_headers(barch_core PRIVATE ${BARCH_ROOT}/src/value_type.h ${BARCH_ROOT}/src/variable.h)
set_target_properties(barch_core PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 8)

# ---- barchd ------------------------------------------------------------------

add_executable(barchd ${BARCH_ROOT}/src/main/barchd.cpp)
barch_win32_settings(barchd)
barch_win32_link(barchd)

# ---- the python module -------------------------------------------------------
#
# _barch.pyd for python.org's CPython, not MSYS2's: that's the python people on
# Windows pip install into. It's built against the include/ and libs/ of a
# python.org install, which is what actions/setup-python puts in pythonLocation;
# locally, point BARCH_PYTHON_ROOT at one (the `python` NuGet package's tools/
# folder unpacks to the same layout). The SWIG wrapper is generated on the build
# machine, so swig only needs to run there.

set(BARCH_PYTHON_ROOT "$ENV{pythonLocation}" CACHE PATH
        "A python.org Python (the folder holding include/ and libs/) to build _barch.pyd for")
find_program(SWIG_EXECUTABLE NAMES swig swig4.0)
if (BARCH_PYTHON_ROOT AND SWIG_EXECUTABLE)
    file(GLOB _py_libs "${BARCH_PYTHON_ROOT}/libs/python3[0-9]*.lib")
    list(FILTER _py_libs EXCLUDE REGEX "_d\\.lib$")
    if (NOT _py_libs OR NOT EXISTS "${BARCH_PYTHON_ROOT}/include/Python.h")
        message(FATAL_ERROR "BARCH_PYTHON_ROOT=${BARCH_PYTHON_ROOT} has no include/Python.h "
                "and libs/python3XY.lib - point it at a python.org Python")
    endif ()
    list(GET _py_libs 0 BARCH_PYTHON_LIB)
    message("Python module for ${BARCH_PYTHON_LIB}")

    find_package(SWIG REQUIRED)
    include(UseSWIG)
    set(SWIG_USE_SWIG_DEPENDENCIES TRUE)
    set_property(SOURCE ${BARCH_ROOT}/src/barch.i PROPERTY CPLUSPLUS ON)
    swig_add_library(barch TYPE MODULE LANGUAGE python
            OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
            OUTFILE_DIR ${CMAKE_CURRENT_BINARY_DIR}/swig
            SOURCES ${BARCH_ROOT}/src/barch.i)
    set_property(TARGET barch PROPERTY SWIG_USE_TARGET_INCLUDE_DIRECTORIES TRUE)
    barch_win32_settings(barch)
    target_include_directories(barch PRIVATE ${BARCH_PYTHON_ROOT}/include)
    barch_win32_link(barch)
    target_link_libraries(barch PRIVATE ${BARCH_PYTHON_LIB})
    # the generated wrapper is one huge unit and warns about plenty it got from SWIG
    target_compile_options(barch PRIVATE -w)
    set_target_properties(barch PROPERTIES PREFIX "_" SUFFIX ".pyd")
else ()
    message("Python module not built: set BARCH_PYTHON_ROOT to a python.org Python, "
            "and have swig on the PATH")
endif ()
