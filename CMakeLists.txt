cmake_minimum_required(VERSION 3.22)
project(barch VERSION 0.5.8 LANGUAGES CXX)
enable_testing()
set(TARGETS barch lbarch)
# Get the current git commit hash for versioning purposes
execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
)

# Fallback if git is not available
if(NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH "unknown")
endif()

# Generate the barch version header file
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
        ${CMAKE_CURRENT_BINARY_DIR}/generated/version.h
)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)

message("barch version: ${PROJECT_VERSION} (commit: ${GIT_COMMIT_HASH})")

if (NOT ADD_ZSTD_COMPRESSION)
    set(ADD_ZSTD_COMPRESSION ON)
endif ()
if (NOT ADD_FAST_FLOAT)
    set(ADD_FAST_FLOAT ON)
endif ()
if (NOT USE_ASIO)
    set(USE_ASIO ON)
endif ()
if (NOT ADD_SWIG)
    set(ADD_SWIG ON)
endif ()

if (NOT ADD_FMT)
    set(ADD_FMT ON)
endif ()

if (NOT TEST_OD)
    set(TEST_OD ON)
endif ()
if (NOT USE_LIBURING)
    set(USE_LIBURING ON)
endif ()
if (NOT USE_OPENSSL)
    set(USE_OPENSSL ON)
endif ()

# These were `if (NOT X) set(X ON) endif()`, which reads like "default to on"
# and is not: an explicit -DGEN_JNI=OFF is precisely the case that sets it back
# on, so the switch could not be switched. option() defaults the same way and
# leaves a -D alone. See TODO 211.
# Both bindings are off by default. They need a jdk or an openresty luajit that
# most machines building barch do not have, and until now the build looked for
# them on every configure and quietly produced nothing - the CI has been
# printing "Did not find Java and Jni libraries" on every run for as long as
# there has been a CI. Ask for them:
#
#   cmake -B build -DGEN_JNI=ON      # barchjni, barchj.jar, the mvn install
#   cmake -B build -DGEN_LUA=ON      # barchlua for luajit
#
# ADD_LUAU is a different thing and stays on: that is the embedded interpreter
# the stored functions run in, not an optional binding.
option(GEN_LUA "Generate the luajit binding (needs openresty luajit)" OFF)
option(GEN_JNI "Generate the java/jni binding and jar (needs a jdk)" OFF)
option(ADD_LUAU "Build the embedded luau interpreter" ON)
option(COVERAGE "Enable code coverage reporting" OFF)

set(CMAKE_CPP_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(USE_AVX_INSTRUCTIONS ON)

#remove -Werror because fmtlib
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -march=native -Wanalyzer-malloc-leak -Wanalyzer-double-free -Wall -Wextra -Wsign-compare -Wundef -Wno-format-zero-length -Wpointer-arith -Wno-missing-braces -Wno-missing-field-initializers -Wno-missing-attributes")

# A sanitized build, for running the short test set against - see TODO 206 and
# ci/README.md. -DSANITIZE=thread is the one that has earned its keep: it found
# DONE 186, 188, 190 and 192. -DSANITIZE=address is wired the same way but has
# not been run to a clean pass yet.
#
# Use RelWithDebInfo with it. Release turns on -flto, and a sanitizer wants
# frame pointers and readable stacks more than it wants the last of the inlining.
set(SANITIZE "" CACHE STRING "Build with a sanitizer: thread, address, or empty")
set_property(CACHE SANITIZE PROPERTY STRINGS "" thread address)
if (SANITIZE)
    if (NOT SANITIZE STREQUAL "thread" AND NOT SANITIZE STREQUAL "address")
        message(FATAL_ERROR "SANITIZE must be 'thread' or 'address', got '${SANITIZE}'")
    endif ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=${SANITIZE} -fno-omit-frame-pointer -g")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=${SANITIZE} -fno-omit-frame-pointer -g")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=${SANITIZE}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=${SANITIZE}")
    message("Building with the ${SANITIZE} sanitizer")
endif ()

if (COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
    message("Code coverage enabled")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    #set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
endif ()

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_DEBUG
        "-g3 -fno-omit-frame-pointer -mno-avx512f")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
        "-O2 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE
        "-O3 -funroll-all-loops -frename-registers -flto")

if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    message("Using debuggable optimizations")
endif ()

# timeout dumps and writer backtraces on the shard latch. the backtrace
# on every unique acquire is the expensive part, so Release leaves it
# off. Debug / RelWithDebInfo keep it. -DBARCH_LOCK_DEBUG=ON for a
# Release build that still dumps (slow CI).
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    option(BARCH_LOCK_DEBUG "Latch timeout dumps and writer backtraces" OFF)
else ()
    option(BARCH_LOCK_DEBUG "Latch timeout dumps and writer backtraces" ON)
endif ()
if (BARCH_LOCK_DEBUG)
    add_compile_definitions(BARCH_LOCK_DEBUG)
    message("Latch timeout dumps enabled")
endif ()
# maybe one day we may want boost
#find_package(Boost REQUIRED)
# include boost for all targets
#include_directories(${Boost_INCLUDE_DIRS})

set(PYTHON3_EXEC ${CMAKE_BINARY_DIR}/venv/bin/python3)
if (SANITIZE)
    # _barch.so is dlopened by python, so the sanitizer runtime is not in the
    # process when it starts and its interceptors never install - it has to be
    # preloaded. And the runtimes lay out their own shadow memory, which ASLR
    # can land on top of: without setarch -R, TSan dies with "unexpected memory
    # mapping" before the first test runs. See ci/README.md.
    # 66 means a sanitizer finding fails the test that produced it. That started at
    # 0, because the tail of known races would have failed every test in the set and
    # taught everyone to ignore it; the tail is empty now - the short set runs clean
    # under TSan - so a new report is a regression and should fail. Set
    # -DSANITIZE_EXITCODE=0 to go back to reporting without failing, which is what
    # ASan still needs.
    set(SANITIZE_EXITCODE 66 CACHE STRING "Exit code a sanitizer finding produces: 0 reports, 66 fails")
    if (SANITIZE STREQUAL "thread")
        set(_san_libs libtsan.so.2 libtsan.so.0 libtsan.so)
        set(_san_env "TSAN_OPTIONS=halt_on_error=0 exitcode=${SANITIZE_EXITCODE} history_size=4 suppressions=${CMAKE_SOURCE_DIR}/ci/tsan.supp")
    else ()
        set(_san_libs libasan.so.8 libasan.so.6 libasan.so)
        # python allocates plenty it never frees by design; leak checking here
        # reports the interpreter, not barch
        set(_san_env "ASAN_OPTIONS=halt_on_error=0 exitcode=${SANITIZE_EXITCODE} detect_leaks=0")
    endif ()
    set(SANITIZER_RUNTIME "")
    foreach (_lib ${_san_libs})
        execute_process(COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=${_lib}
                OUTPUT_VARIABLE _found OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (EXISTS "${_found}")
            set(SANITIZER_RUNTIME "${_found}")
            break()
        endif ()
    endforeach ()
    if (NOT SANITIZER_RUNTIME)
        message(FATAL_ERROR "no ${SANITIZE} runtime found to preload - looked for ${_san_libs}")
    endif ()
    message("Sanitizer runtime: ${SANITIZER_RUNTIME}")
    set(PYTHON3_EXEC setarch -R env "LD_PRELOAD=${SANITIZER_RUNTIME}" "${_san_env}" ${CMAKE_BINARY_DIR}/venv/bin/python3)
endif ()

set(TEST_BUILD_DIR build)

if (CMAKE_BUILD_TYPE STREQUAL "Release")

else ()
    set(TEST_BUILD_DIR ${CMAKE_BUILD_TYPE})

endif ()

message("The TEST_BUILD_DIR is ${TEST_BUILD_DIR}")

include(FetchContent)
include(ExternalProject)

if (USE_OPENSSL)
    message("Using OpenSSL")
    if (BUILD_OPENSSL)
        FetchContent_Declare(
            openssl
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/openssl/openssl/releases/download/openssl-3.6.0/openssl-3.6.0.tar.gz"
        )

        FetchContent_MakeAvailable(openssl)

        execute_process(COMMAND ./Configure
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
        execute_process(COMMAND make -j4
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
        include_directories(${openssl_SOURCE_DIR}/src/include)
        set(OPENSSL_LIB_PATH ${openssl_SOURCE_DIR}/libssl.a ${openssl_SOURCE_DIR}/libcrypto.a)
        message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
    else()
        set(OPENSSL_USE_STATIC_LIBS on)
        find_package(OpenSSL COMPONENTS Crypto SSL)
        # Avoid a rare circumstance of not finding all components but the end-user did their
        # own call for OpenSSL, which might trick us into thinking we'd otherwise have what we wanted
        if(OPENSSL_FOUND)
            include_directories(${OPENSSL_INCLUDE_DIR})
            message(STATUS "OpenSSL libs: " ${OPENSSL_LIBRARIES})
            set(OPENSSL_LIB_PATH ${OPENSSL_LIBRARIES})
            message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
        else()
            message(STATUS "Did not find OpenSSL libs use -DBUILD_OPENSSL to build it")
        endif()


    endif ()

endif ()

if (USE_LIBURING)
    message("Using liburing")
    FetchContent_Declare(
            liburing
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/axboe/liburing/archive/refs/tags/liburing-2.11.tar.gz"
    )
    FetchContent_MakeAvailable(liburing)
    execute_process(COMMAND ./configure --cc=gcc --cxx=g++
            WORKING_DIRECTORY ${liburing_SOURCE_DIR})
    execute_process(COMMAND make -j4
            WORKING_DIRECTORY ${liburing_SOURCE_DIR})
    include_directories(${liburing_SOURCE_DIR}/src/include)
    set(LIBURING_LIB_PATH ${liburing_SOURCE_DIR}/src/liburing.a)
else ()
    message("Liburing not used")
endif()

if (USE_ASIO)
    message("Using std alone ASIO")
    FetchContent_Declare(
            asio
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-36-0.tar.gz"
    )
    FetchContent_MakeAvailable(asio)
    include_directories(${asio_SOURCE_DIR}/asio/include)
else ()
    message("asio not used use -USE_ASIO=ON")
endif()
if (ADD_ZSTD_COMPRESSION)
    message("Using ZSTD compression")
    set(ZSTD_BUILD_STATIC ON)
    set(ZSTD_BUILD_SHARED OFF)

    FetchContent_Declare(
            zstd
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SOURCE_SUBDIR build/cmake
            URL "https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz"
    )
else ()
    message("ZSTD compression not used use -DADD_ZSTD_COMPRESSION=ON")
endif()
if (ADD_FAST_FLOAT)
    FetchContent_Declare(
            fast_float
            GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
            GIT_TAG main
    )
    FetchContent_MakeAvailable(fast_float)
    include_directories(${fast_float_SOURCE_DIR}/include)
endif ()
if (ADD_FMT)

    FetchContent_Declare(
            fmt
            GIT_REPOSITORY https://github.com/fmtlib/fmt
            GIT_TAG        123913715afeb8a437e6388b4473fcc4753e1c9a) # 11.4
    SET(BUILD_SHARED_LIBS FALSE)
    SET(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
    FetchContent_MakeAvailable(fmt)
    include_directories(${fmt_SOURCE_DIR}/include)
endif ()

if (ADD_ZSTD_COMPRESSION)
    FetchContent_MakeAvailable(zstd)
    include_directories(${zstd_SOURCE_DIR}/lib)
endif ()

if (ADD_LUAU)
    message("Using Luau for foreign miss scripts")
    set(LUAU_BUILD_CLI OFF CACHE BOOL "" FORCE)
    set(LUAU_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(LUAU_BUILD_WEB OFF CACHE BOOL "" FORCE)
    set(LUAU_WERROR OFF CACHE BOOL "" FORCE)
    set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
    # our -Wextra / -Werror must not leak into Luau. Compiler and Bytecode
    # never get Luau's own -Wno-unused, so unused parameters there have
    # failed CI.
    set(_barch_cxx_flags_saved "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "")
    FetchContent_Declare(
            luau
            GIT_REPOSITORY https://github.com/luau-lang/luau.git
            GIT_TAG 0.734
            GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(luau)
    set(CMAKE_CXX_FLAGS "${_barch_cxx_flags_saved}")
    unset(_barch_cxx_flags_saved)
    foreach (_luau_tgt IN ITEMS
            Luau.Common Luau.Ast Luau.Bytecode Luau.Compiler Luau.VM
            Luau.Config Luau.Analysis Luau.CodeGen Luau.Inliner
            Luau.Require Luau.CLI.lib isocline)
        if (TARGET ${_luau_tgt})
            target_compile_options(${_luau_tgt} PRIVATE
                    -Wno-unused -Wno-unused-parameter -Wno-unused-variable
                    -Wno-error=unused-parameter -Wno-error=unused-variable)
        endif ()
    endforeach ()
    set(BARCH_HAS_LUAU ON)

    # NumKong: header-only C++ types and nk::vector. Scalar kernels stay for
    # a later cut; this is f64/f32/f16/bf16 and their vectors in Luau.
    # Populate the source and add our own INTERFACE target. Their CMake
    # defaults GIT to `master` (the repo is `main`), runs ISA probes at
    # configure time, and puts Clang `-fmodules` on the INTERFACE target.
    FetchContent_Declare(
            numkong
            GIT_REPOSITORY https://github.com/ashvardanian/NumKong.git
            GIT_TAG main
            GIT_SHALLOW TRUE
    )
    # CMake 3.30+ wants MakeAvailable; that would run NumKong's CMake.
    if (POLICY CMP0169)
        cmake_policy(SET CMP0169 OLD)
    endif ()
    FetchContent_GetProperties(numkong)
    if (NOT numkong_POPULATED)
        FetchContent_Populate(numkong)
    endif ()
    if (NOT TARGET numkong)
        add_library(numkong INTERFACE)
        target_include_directories(numkong INTERFACE ${numkong_SOURCE_DIR}/include)
        # NumKong picks the native `__bf16` when the compiler is gcc or clang and
        # the target has bf16, which it tests with `__AVX512BF16__`. gcc only grew
        # the `__bf16` scalar type in 13; 11 and 12 define `__AVX512BF16__` for the
        # intrinsics but have no such type, so `-march=native` on a host that has
        # AVX512-BF16 gives 600 errors starting at `typedef __bf16 nk_bf16_t`.
        # Both halves are needed to see it, which is why it only shows up on the
        # 22.04 job and only on the runners with a new enough CPU.
        # 0 falls back to their `unsigned short`, same two bytes, same layout.
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 13)
            target_compile_definitions(numkong INTERFACE NK_NATIVE_BF16=0)
        endif ()
    endif ()
    set(BARCH_HAS_NUMKONG ON)
    # Their vector::try_zeros memsets the wrapper structs. Harmless, noisy.
    set_source_files_properties(src/foreign/nk_luau.cpp PROPERTIES
            COMPILE_OPTIONS "-Wno-class-memaccess")
    message("Using NumKong for Luau nk.f32 / nkf32vector")

    # simdjson amalgamated single header. Their CMake is not run.
    FetchContent_Declare(
            simdjson
            GIT_REPOSITORY https://github.com/simdjson/simdjson.git
            GIT_TAG master
            GIT_SHALLOW TRUE
    )
    if (POLICY CMP0169)
        cmake_policy(SET CMP0169 OLD)
    endif ()
    FetchContent_GetProperties(simdjson)
    if (NOT simdjson_POPULATED)
        FetchContent_Populate(simdjson)
    endif ()
    add_library(simdjson_amalg OBJECT ${simdjson_SOURCE_DIR}/singleheader/simdjson.cpp)
    set_target_properties(simdjson_amalg PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_include_directories(simdjson_amalg PUBLIC ${simdjson_SOURCE_DIR}/singleheader)
    target_compile_options(simdjson_amalg PRIVATE -w)
    set_source_files_properties(src/foreign/simdjson_luau.cpp PROPERTIES
            COMPILE_OPTIONS "-Wno-undef")
    set(BARCH_HAS_SIMDJSON ON)
    message("Using simdjson for Luau simdjson.parse")

    # Crow is header-only. Their CMake wants find_package(asio); we already have it.
    if (USE_ASIO)
        FetchContent_Declare(
                crow
                GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
                GIT_TAG v1.2.1.2
                GIT_SHALLOW TRUE
        )
        if (POLICY CMP0169)
            cmake_policy(SET CMP0169 OLD)
        endif ()
        FetchContent_GetProperties(crow)
        if (NOT crow_POPULATED)
            FetchContent_Populate(crow)
        endif ()
        set(BARCH_HAS_CROW ON)
        set_property(SOURCE src/http_api.cpp src/foreign/crow_luau.cpp
                APPEND PROPERTY COMPILE_OPTIONS -Wno-undef -Wno-unused-parameter)
        message("Using Crow for Luau HTTP transport()")

        # cofetch is an asio-native async HTTP client, header-only over libcurl,
        # and it is what `http.request(url):get()` inside a stored function runs
        # on. Ubuntu ships no libcurl.a, so static means building curl here.
        #
        # Kept as small as it can usefully be: HTTP and HTTPS only, no ldap, psl,
        # ssh or brotli, no exe and no tests. TLS reuses the OpenSSL this build
        # already links. That leaves HTTP/1.1 - HTTP/2 would want nghttp2, which
        # is another source build for something no stored function has asked for.
        set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
        set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
        set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
        set(BUILD_LIBCURL_DOCS OFF CACHE BOOL "" FORCE)
        set(BUILD_MISC_DOCS OFF CACHE BOOL "" FORCE)
        set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
        set(HTTP_ONLY ON CACHE BOOL "" FORCE)
        set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
        set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
        set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
        set(CURL_DISABLE_LDAP ON CACHE BOOL "" FORCE)
        set(CURL_DISABLE_LDAPS ON CACHE BOOL "" FORCE)
        set(CURL_BROTLI OFF CACHE BOOL "" FORCE)
        set(CURL_ZSTD OFF CACHE BOOL "" FORCE)
        # curl's CMake reads this to decide shared vs static; it is restored below
        # so nothing else in the build inherits it
        set(BARCH_SAVED_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
        set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
        FetchContent_Declare(
                curl
                DOWNLOAD_EXTRACT_TIMESTAMP TRUE
                URL "https://github.com/curl/curl/releases/download/curl-8_11_1/curl-8.11.1.tar.gz"
        )
        FetchContent_MakeAvailable(curl)
        set(BUILD_SHARED_LIBS ${BARCH_SAVED_BUILD_SHARED_LIBS} CACHE BOOL "" FORCE)
        # everything barch builds ends up in a shared module, so curl's objects
        # have to be position independent even though the library itself is static
        set_property(TARGET libcurl_static PROPERTY POSITION_INDEPENDENT_CODE ON)

        FetchContent_Declare(
                cofetch
                GIT_REPOSITORY https://github.com/SSARCandy/cofetch.git
                GIT_TAG main
                GIT_SHALLOW TRUE
                # only include/cofetch.h is wanted. Their submodules are asio,
                # googletest and a doxygen theme - we have our own asio and none
                # of the rest is built here
                GIT_SUBMODULES ""
                GIT_SUBMODULES_RECURSE OFF
        )
        if (POLICY CMP0169)
            cmake_policy(SET CMP0169 OLD)
        endif ()
        FetchContent_GetProperties(cofetch)
        if (NOT cofetch_POPULATED)
            FetchContent_Populate(cofetch)
        endif ()
        if (NOT TARGET cofetch)
            add_library(cofetch INTERFACE)
            target_include_directories(cofetch INTERFACE ${cofetch_SOURCE_DIR}/include)
            # consumers of a static curl must say so, or the headers declare the
            # entry points dllimport-style and the link fails on some platforms
            target_compile_definitions(cofetch INTERFACE CURL_STATICLIB)
            target_link_libraries(cofetch INTERFACE libcurl_static)
        endif ()
        set(BARCH_HAS_COFETCH ON)
        set_property(SOURCE src/foreign/fetch_luau.cpp
                APPEND PROPERTY COMPILE_OPTIONS -Wno-undef -Wno-unused-parameter)
        message("Using cofetch for Luau http.request()")
    else ()
        set(BARCH_HAS_CROW OFF)
        set(BARCH_HAS_COFETCH OFF)
    endif ()
else ()
    message("Luau not used; foreign=luau spaces stay off")
    set(BARCH_HAS_LUAU OFF)
    set(BARCH_HAS_NUMKONG OFF)
    set(BARCH_HAS_SIMDJSON OFF)
    set(BARCH_HAS_CROW OFF)
endif ()

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
#-Weffc++ is removed else fmtlib does not compile - because it's a little ...s

if (NOT DEFINED ADD_MYSQL)
    set(ADD_MYSQL ON)
endif ()
if (NOT DEFINED ADD_POSTGRES)
    set(ADD_POSTGRES ON)
endif ()

if (ADD_MYSQL)
    find_package(MySQL)
    if (MySQL_FOUND)
        message("Using MySQL client for foreign=mysql")
        set(BARCH_HAS_MYSQL ON)
    else ()
        message("MySQL client not found; foreign=mysql spaces stay off")
        set(BARCH_HAS_MYSQL OFF)
    endif ()
else ()
    message("MySQL client not used; foreign=mysql spaces stay off")
    set(BARCH_HAS_MYSQL OFF)
endif ()

if (ADD_POSTGRES)
    find_package(PostgreSQL)
    if (PostgreSQL_FOUND)
        message("Using PostgreSQL client for foreign=postgres")
        set(BARCH_HAS_POSTGRES ON)
    else ()
        message("PostgreSQL client not found; foreign=postgres spaces stay off")
        set(BARCH_HAS_POSTGRES OFF)
    endif ()
else ()
    message("PostgreSQL client not used; foreign=postgres spaces stay off")
    set(BARCH_HAS_POSTGRES OFF)
endif ()


include_directories(src)
include_directories(external/include)

if (NOT TEST_OD)
    message("Testing not enabled Use -DTEST_OD=ON")
else()
    set(EXTRA_TEST_ARGS "")
    if (COVERAGE)
        set(EXTRA_TEST_ARGS "-DCOVERAGE=ON")
    endif ()
    ## CMake sub project executables only works if you pretend they are not subprojects
    execute_process(COMMAND rm -rf ${TEST_BUILD_DIR} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND mkdir ${TEST_BUILD_DIR} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ".."  RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${TEST_BUILD_DIR})
    execute_process(COMMAND make RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${TEST_BUILD_DIR})

endif ()

file(GLOB SOURCES
    "src/*.c" "src/*.cpp" "src/*.cxx" "src/rpc/*.cpp" "src/art/*.cpp" "src/io/*.cpp" "src/keys/*.cpp" "src/shard/*.cpp" "src/parsers/*.cpp" "src/foreign/*.cpp"
)
if (ADD_SWIG)
    find_package (Python3 COMPONENTS Interpreter Development)
    find_package (SWIG)
    include (UseSWIG)
    set_property(SOURCE src/barch.i PROPERTY CPLUSPLUS ON)
    if (${GEN_LUA})
        find_package(LuaJIT)
        # for luajit, lua and openresty
        if (${LuaJIT_FOUND})
            message("Found LuaJIT - generating a binding")
            message("LuaJIT barchlua - will be installed to ${CMAKE_INSTALL_PREFIX}/lib/lua/5.1")
            include_directories(${LUAJIT_INCLUDE_DIR})
            # luau_driver.cpp includes Luau's lua.h. The SWIG wrap includes
            # LuaJIT's. They cannot share an include path, so the driver is
            # compiled as an object library below and linked in.
            set(BARCHLUA_SOURCES ${SOURCES})
            list(FILTER BARCHLUA_SOURCES EXCLUDE REGEX "luau_driver\\.cpp$")
            list(FILTER BARCHLUA_SOURCES EXCLUDE REGEX "nk_luau\\.cpp$")
            list(FILTER BARCHLUA_SOURCES EXCLUDE REGEX "simdjson_luau\\.cpp$")
            list(FILTER BARCHLUA_SOURCES EXCLUDE REGEX "crow_luau\\.cpp$")
            list(FILTER BARCHLUA_SOURCES EXCLUDE REGEX "fetch_luau\\.cpp$")
            swig_add_library(barchlua LANGUAGE lua SOURCES src/barch.i ${BARCHLUA_SOURCES})
            target_link_libraries(barchlua PRIVATE ${LUAJIT_LIBRARIES})
            if (USE_LIBURING)
                target_link_libraries(barchlua PRIVATE ${LIBURING_LIB_PATH})
            endif ()
            if (USE_OPENSSL)
                target_link_libraries(barchlua PRIVATE ${OPENSSL_LIB_PATH})
            endif ()
            target_link_libraries(barchlua PRIVATE fmt::fmt)
            target_link_libraries(barchlua PRIVATE libzstd_static)
            set_target_properties(barchlua PROPERTIES
                    OUTPUT_NAME "barch"
            )
            install(TARGETS barchlua DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/lua/5.1 COMPONENT barchlua)

        else()
            message("The Lua language was not found - perhaps install Lua?")
        endif ()
    endif ()
    if (${GEN_JNI})
        find_package(Java)
        find_package(JNI)

        include(UseJava)
        set_property(SOURCE src/barch.i PROPERTY CPLUSPLUS ON)
        if (${JNI_FOUND})
            message("Found Java - generating a binding")
            message("Java includes:${JNI_INCLUDE_DIRS}")
            message("Java libraries:${JNI_LIBRARIES} '${Java_LIBRARIES}'")
            include_directories(${JNI_INCLUDE_DIRS})
            swig_add_library(barchjni LANGUAGE java OUTPUT_DIR org/barch SOURCES src/barch.i ${SOURCES})
            set_property(TARGET barchjni PROPERTY SWIG_COMPILE_OPTIONS -package org.barch)
            target_link_libraries(barchjni PRIVATE ${Java_LIBRARIES})
            set(BARCHJNI_TEST ON)
            if (USE_LIBURING)
                target_link_libraries(barchjni PRIVATE ${LIBURING_LIB_PATH})
            endif ()
            if (USE_OPENSSL)
                target_link_libraries(barchjni PRIVATE ${OPENSSL_LIB_PATH})
            endif ()
            target_link_libraries(barchjni PRIVATE fmt::fmt)
            target_link_libraries(barchjni PRIVATE libzstd_static)
            # after the installation we should run below if maven is available
            #
            find_program(MAVEN_EXE NAMES mvn)
            if (MAVEN_EXE)
                add_jar(barchj
                        SOURCES
                        ${CMAKE_BINARY_DIR}/org/barch/barchJNI.java
                        ${CMAKE_BINARY_DIR}/org/barch/barch.java
                )

                add_custom_command(
                        TARGET barchjni POST_BUILD
                        COMMAND ${CMAKE_COMMAND} --build . --target barchj
                        COMMENT "Triggering JAR build after JNI generation"
                )

                add_custom_command(
                        TARGET barchjni
                        POST_BUILD
                        COMMAND mvn install:install-file -Dfile=barchj.jar -DgroupId=org.barch -DartifactId=barch -Dversion=${PROJECT_VERSION} -Dpackaging=jar
                        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
                        COMMENT "installing barchj.jar using mvn at org.barch:barch:${PROJECT_VERSION}"
                        VERBATIM
                )

            endif ()
        else ()
            message("Did not find Java and Jni libraries - please set the JAVA_HOME environment variable ")
        endif ()
    else ()
        message("Java JNI off")
    endif ()
    if (${Python3_FOUND})
        message("Found Python - generating a binding")
        message("Python includes:${Python3_INCLUDE_DIRS}")
        message("Python libraries:${Python3_LIBRARIES} '${PYTHON_LIBRARIES}'")

        include_directories(${Python3_INCLUDE_DIRS})
        #add_library(barch SHARED ${SOURCES})
        swig_add_library(barch LANGUAGE python SOURCES src/barch.i ${SOURCES})
        target_link_libraries(barch PRIVATE ${Python3_LIBRARIES})
        set(PYTHON_INSTALL_FILES
                /home/barch/setup/${PROJECT_NAME}.py
                /home/barch/setup/_${PROJECT_NAME}.so)

        set(SETUP_PY_IN ${CMAKE_CURRENT_SOURCE_DIR}/src/python/setup.py.in)
        set(SETUP_PY_OUT ${CMAKE_BINARY_DIR}/setup.py)
        configure_file(${SETUP_PY_IN} ${SETUP_PY_OUT})

        add_custom_command(TARGET barch POST_BUILD
            COMMAND mkdir -p ${CMAKE_BINARY_DIR}/b
            BYPRODUCTS ${CMAKE_BINARY_DIR}/b
        )
        set(PYBARCH_TEST ON)
    else ()
        message("SWIG python interface generation skipped - python not found")
        message("building non SWIG library")
    endif ()
else ()
    message("SWIG local interface not used use -DADD_SWIG=ON")
    message("building non SWIG library")

endif ()

add_library(lbarch SHARED ${SOURCES}
        src/rpc/restarter.h
        external/include/a5hash.h
        src/shared_mutex.cpp
        src/shared_mutex.h)

set(TEST_SOURCE_PATH ${CMAKE_SOURCE_DIR}/test)
set(TEST_BUILD_PATH ${TEST_SOURCE_PATH}/${TEST_BUILD_DIR})

if (COVERAGE)
    add_custom_target(coverage_report
            COMMAND lcov --capture --directory . --output-file coverage.info
            COMMAND lcov --remove coverage.info '${CMAKE_BINARY_DIR}/_deps/*' '/usr/*' 'external/*' '*/CMakeFiles/*' --output-file coverage.info
            COMMAND genhtml coverage.info --output-directory coverage_html
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Generating HTML coverage report"
    )
    set(LCOV_REPORT "${CMAKE_BINARY_DIR}/coverage.info")
    set(BADGE_OUTPUT "${CMAKE_SOURCE_DIR}/coverage.svg")

    add_custom_target(coverage_badge
            DEPENDS coverage_report
            COMMAND bash "${CMAKE_SOURCE_DIR}/generate_badge.sh" "${LCOV_REPORT}" "${BADGE_OUTPUT}"
            COMMENT "Generating coverage badge from ${LCOV_REPORT} into ${BADGE_OUTPUT}"
            VERBATIM
    )
endif ()

if (ADD_ZSTD_COMPRESSION)
    target_link_libraries(barch PRIVATE libzstd_static)
    target_link_libraries(lbarch PRIVATE libzstd_static)
endif ()

if (USE_LIBURING)
    target_link_libraries(barch PRIVATE ${LIBURING_LIB_PATH})
    target_link_libraries(lbarch PRIVATE ${LIBURING_LIB_PATH})
endif ()

if (USE_OPENSSL)
    message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
    target_link_libraries(barch PRIVATE ${OPENSSL_LIB_PATH})
    target_link_libraries(lbarch PRIVATE ${OPENSSL_LIB_PATH})
endif ()

if (ADD_FMT)
    target_link_libraries(barch PRIVATE fmt::fmt)
    target_link_libraries(lbarch PRIVATE fmt::fmt)
endif ()

if (BARCH_HAS_LUAU)
    foreach (tgt barch lbarch barchjni)
        if (TARGET ${tgt})
            target_compile_definitions(${tgt} PRIVATE BARCH_HAS_LUAU)
            target_include_directories(${tgt} BEFORE PRIVATE
                ${luau_SOURCE_DIR}/VM/include
                ${luau_SOURCE_DIR}/Compiler/include)
            target_link_libraries(${tgt} PRIVATE Luau.Compiler Luau.VM)
            if (BARCH_HAS_NUMKONG)
                target_link_libraries(${tgt} PRIVATE numkong)
            endif ()
            if (BARCH_HAS_SIMDJSON)
                target_link_libraries(${tgt} PRIVATE simdjson_amalg)
            endif ()
            if (BARCH_HAS_CROW)
                # CROW_DISABLE_STATIC_DIR: without it Crow adds a route of its own
                # at run() - /static/<path>, serving files out of `static/` next
                # to the process cwd, which for barch is wherever the shards are
                # written. Nothing here asked for that, it is not in HTTP STATUS
                # beside the routes a transport() declared, and it takes the
                # /static/* prefix away from anyone who wants it. See TODO 222.
                target_compile_definitions(${tgt} PRIVATE BARCH_HAS_CROW ASIO_STANDALONE
                        CROW_DISABLE_STATIC_DIR)
                target_include_directories(${tgt} PRIVATE ${crow_SOURCE_DIR}/include)
                if (USE_OPENSSL)
                    target_compile_definitions(${tgt} PRIVATE CROW_ENABLE_SSL)
                endif ()
            endif ()
            if (BARCH_HAS_COFETCH)
                target_compile_definitions(${tgt} PRIVATE BARCH_HAS_COFETCH)
                target_link_libraries(${tgt} PRIVATE cofetch)
            endif ()
        endif ()
    endforeach ()
    if (TARGET barchlua)
        target_compile_definitions(barchlua PRIVATE BARCH_HAS_LUAU)
        target_link_libraries(barchlua PRIVATE Luau.Compiler Luau.VM)
        if (BARCH_HAS_NUMKONG)
            target_link_libraries(barchlua PRIVATE numkong)
        endif ()
        if (BARCH_HAS_SIMDJSON)
            target_link_libraries(barchlua PRIVATE simdjson_amalg)
        endif ()
        if (BARCH_HAS_CROW)
            target_compile_definitions(barchlua PRIVATE BARCH_HAS_CROW ASIO_STANDALONE CROW_DISABLE_STATIC_DIR)
            target_include_directories(barchlua PRIVATE ${crow_SOURCE_DIR}/include)
            if (USE_OPENSSL)
                target_compile_definitions(barchlua PRIVATE CROW_ENABLE_SSL)
            endif ()
        endif ()
        if (BARCH_HAS_COFETCH)
            target_compile_definitions(barchlua PRIVATE BARCH_HAS_COFETCH)
            target_link_libraries(barchlua PRIVATE cofetch)
        endif ()
    endif ()
endif ()

if (TARGET barchlua)
    add_library(barch_luau_driver OBJECT src/foreign/luau_driver.cpp src/foreign/nk_luau.cpp src/foreign/simdjson_luau.cpp src/foreign/crow_luau.cpp src/foreign/fetch_luau.cpp)
    set_target_properties(barch_luau_driver PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if (BARCH_HAS_LUAU)
        target_compile_definitions(barch_luau_driver PRIVATE BARCH_HAS_LUAU)
        target_include_directories(barch_luau_driver BEFORE PRIVATE
            ${luau_SOURCE_DIR}/VM/include
            ${luau_SOURCE_DIR}/Compiler/include)
        target_link_libraries(barch_luau_driver PRIVATE Luau.Compiler Luau.VM)
        if (BARCH_HAS_NUMKONG)
            target_link_libraries(barch_luau_driver PRIVATE numkong)
        endif ()
        if (BARCH_HAS_SIMDJSON)
            target_link_libraries(barch_luau_driver PRIVATE simdjson_amalg)
        endif ()
        if (BARCH_HAS_CROW)
            target_compile_definitions(barch_luau_driver PRIVATE BARCH_HAS_CROW ASIO_STANDALONE CROW_DISABLE_STATIC_DIR)
            target_include_directories(barch_luau_driver PRIVATE ${crow_SOURCE_DIR}/include)
            if (USE_OPENSSL)
                target_compile_definitions(barch_luau_driver PRIVATE CROW_ENABLE_SSL)
            endif ()
        endif ()
        if (BARCH_HAS_COFETCH)
            target_compile_definitions(barch_luau_driver PRIVATE BARCH_HAS_COFETCH)
            target_link_libraries(barch_luau_driver PRIVATE cofetch)
        endif ()
    endif ()
    if (BARCH_HAS_MYSQL)
        target_compile_definitions(barch_luau_driver PRIVATE BARCH_HAS_MYSQL)
    endif ()
    if (BARCH_HAS_POSTGRES)
        target_compile_definitions(barch_luau_driver PRIVATE BARCH_HAS_POSTGRES)
    endif ()
    target_sources(barchlua PRIVATE $<TARGET_OBJECTS:barch_luau_driver>)
endif ()

if (BARCH_HAS_MYSQL)
    foreach (tgt barch lbarch barchlua barchjni)
        if (TARGET ${tgt})
            target_compile_definitions(${tgt} PRIVATE BARCH_HAS_MYSQL)
            target_include_directories(${tgt} PRIVATE ${MySQL_INCLUDE_DIR})
            target_link_libraries(${tgt} PRIVATE ${MySQL_LIBRARIES})
        endif ()
    endforeach ()
endif ()

if (BARCH_HAS_POSTGRES)
    foreach (tgt barch lbarch barchlua barchjni)
        if (TARGET ${tgt})
            target_compile_definitions(${tgt} PRIVATE BARCH_HAS_POSTGRES)
            target_include_directories(${tgt} PRIVATE ${PostgreSQL_INCLUDE_DIRS})
            target_link_libraries(${tgt} PRIVATE ${PostgreSQL_LIBRARIES})
        endif ()
    endforeach ()
endif ()


if (NOT TEST_OD)
    message("Testing not enabled tests not generated :Use -DTEST_OD=ON")
else()


    add_test(NAME TestIntegers
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testintegers.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestDoubles
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testdouble.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME BicycleTest
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testbicycle.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestAbc
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testabc.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME Test123
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/test123.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestRandom
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testrnd.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestBounds
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testbounds.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestKeys
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testkeys.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME LargeKeys
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testgloblarge.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME Test1111111
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/test111111.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestMSG
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testmulti.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestIntArithmetic
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testintarith.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestComposites
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testcomposites.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestZRank
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testzrank.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestBenchy
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/zbenchy.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestHashBenchy
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/zwbenchy.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestClean
            COMMAND sh -c "rm -f *.dat t/*/*.dat"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    if (PYBARCH_TEST)
        message("Python test: WORKING_DIRECTORY ${CMAKE_BINARY_DIR}")

        add_test(NAME TestInstallVENV
                COMMAND "python3" -m venv ./venv
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestInstallRedisPy
                COMMAND "venv/bin/pip" install redis
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestInstallRequestsPy
                COMMAND "venv/bin/pip" install requests
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchInstallPy
                COMMAND "venv/bin/pip" install .
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchPy
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/testbarch.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy2
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchLru
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/lrutest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchExpireMany
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/expiretest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchList
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/listtest.py ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}
                WORKING_DIRECTORY ${TEST_BUILD_PATH})

        add_test(NAME TestBarchRPC
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/remotetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchSimpleClusterRPC
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/routetest.py ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}
                WORKING_DIRECTORY ${TEST_BUILD_PATH})

        add_test(NAME TestRespClientLocal
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/redispytest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the same command surface again over RESP3, so the protocol the writer picks
        # is exercised both ways rather than only on the RESP2 path
        add_test(NAME TestRespClientLocalRESP3
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/redispytest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
        set_tests_properties(TestRespClientLocalRESP3 PROPERTIES
                ENVIRONMENT "BARCH_TEST_RESP=3")

        # the glob matcher only needs glob.cpp, so it gets a plain executable
        add_executable(globdifftest test/globdifftest.cpp src/glob.cpp)
        add_test(NAME TestGlobDifferential
                COMMAND globdifftest
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_executable(locktest test/locktest.cpp)
        set(THREADS_PREFER_PTHREAD_FLAG ON)
        find_package(Threads REQUIRED)
        target_link_libraries(locktest PRIVATE Threads::Threads)
        # snapshot tests need the dump path even when the server is Release
        target_compile_definitions(locktest PRIVATE BARCH_LOCK_DEBUG)
        # CI builds --target barch and used to skip this binary, so ctest
        # reported TestDebuggableServerLock as Not Run. Pull it in with barch.
        if (TARGET barch)
            add_dependencies(barch locktest)
        endif()
        add_test(NAME TestDebuggableServerLock
                COMMAND locktest
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestAsyncPipeline
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/asyncpipelinetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestConfig
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/configtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestHybrid
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/hybridtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # rights that differ per key space
        add_test(NAME TestSpaceAcl
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/spaceacltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # a stored function survives eviction. Its own process: it drops maxmemory
        # far enough that the sweeper takes almost everything else
        add_test(NAME TestFunctionEviction
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/functionevicttest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # stored Luau functions: the key range, SETF/GETF/REMF/KEYSF
        add_test(NAME TestFunctions
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/functiontest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestFunctionSync
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/functionsynctest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        if (ADD_LUAU)
            add_test(NAME TestNkLuau
                    COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/nkluautest.py
                    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
            add_test(NAME TestSimdjsonLuau
                    COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/simdjsontest.py
                    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
            add_test(NAME TestHttpLuau
                    COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/httptest.py
                    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
            if (BARCH_HAS_COFETCH)
                add_test(NAME TestFetchLuau
                        COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/fetchluautest.py
                        WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
            endif ()
            add_test(NAME TestRespTransport
                    COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/resptransporttest.py
                    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
        endif ()

        # ACL key patterns are parsed and refused rather than silently dropped
        add_test(NAME TestAclFilter
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/aclfiltertest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestKeysStream
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/keysstreamtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestChaos
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/chaostest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the content gauges are per shard, so clearing one key space must not zero them
        add_test(NAME TestShardStats
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/shardstatstest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # configuration taken from the environment. Reads it at import, so the test
        # drives child processes with the environment set
        add_test(NAME TestEnvConfig
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/envconfigtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the per key space range sharding option and its plumbing
        add_test(NAME TestRangeShardConfig
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/rangeshardtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestForeign
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/foreigntest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestKeySplit
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/keysplittest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestForeignLuau
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/foreign_luau.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestForeignMysql
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/foreign_mysql.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestForeignPostgres
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/foreign_postgres.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the ordered routing the option selects, and the rebalancer behind it
        add_test(NAME TestRangeShardRouting
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/rangeroutetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # turning the option on for a space that already has hash sharded data in it.
        # Runs in two processes, because a key space reads its options once
        add_test(NAME TestRangeShardConvert
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/rangeconverttest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the exact bytes each command answers with. Everything else drives barch through
        # the embedded interface or through redis-py, and neither can see a reply shape -
        # redis-py parses a bulk string and a one element array holding one into values
        # that compare equal. Every defect in DONE 33 was invisible to the suite without it
        # valkey's own tests, translated and run against valkey first and then barch -
        # see TODO 40. The valkey run is what decides which translations are faithful, so
        # this needs the valkey-server that test/CMakeLists.txt fetches; without it the
        # test reports SKIP and passes rather than failing on a missing dependency
        # the same command through a local handle and a remote one must agree. Guards the
        # reply decoder on the binary protocol, which got every element after the first
        # wrong for as long as nothing asked it for more than one - see DONE 40
        # SCAN's contract: a full iteration reports every key present at its start and
        # still present at its end, whatever is written while it runs. Hand written from
        # valkey's unit/scan.tcl, which cannot be translated mechanically - see TODO 40
        add_test(NAME TestScanGuarantees
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/scantest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestRemoteArrays
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/remotearraytest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # one name holds one kind of collection, and the kinds count only their own
        # entries. Asserted against literals rather than against each other, because the
        # miscount this replaced was two walks of one prefix agreeing with each other while
        # both were wrong - see DONE 48
        # a logical export is the migration path a storage version bump leaves people
        # needing, so the test is a round trip through an emptied store - see DONE 61
        add_test(NAME TestExportRoundTrip
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/exporttest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestContainerKinds
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/containerkindtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestValkeyDifferential
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/valkeytrans/differential.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestRespShapes
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/respshapetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBindings
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/bindingtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestGlobPerformance
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/globperftest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # the same measurement over JSON shaped values, where a query starts with a
        # quote or a colon and the shortcuts have nothing to skip. Run separately
        # because loading a second corpus into a space that has been globbed over is
        # presently far too slow - see TODO 13
        add_test(NAME TestGlobPerformanceJson
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/globperftest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
        set_tests_properties(TestGlobPerformanceJson PROPERTIES
                ENVIRONMENT "BARCH_PERF_CORPUS=json")

        add_test(NAME TestReplyShape
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/replyshapetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestScanGlob
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/scanglobtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestRespInfoMemory
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/redisinfotest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestMerge
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/mergetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestSpaceThread
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/spacethreadtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBpopThread
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/bstartest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestLargeDataSave
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/largetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestLargeDataLoad
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/largetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestCompression
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/compresstest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestScan
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/scantest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        # The set worth running under a sanitizer: the ones that put several
        # threads through the server at once, which is where TSan has actually
        # found things. Pair it with BARCH_TEST_SCALE so they finish - see
        # test/scale.py and TODO 206:
        #
        #   cmake -B build-tsan -DTEST_OD=ON -DSANITIZE=thread \
        #         -DCMAKE_BUILD_TYPE=RelWithDebInfo
        #   cmake --build build-tsan --target barch --parallel 2
        #   ( cd build-tsan ; BARCH_TEST_SCALE=0.05 ctest -L short --output-on-failure )
        #
        # TestBarchLru is deliberately not here. Its assertions are about
        # absolute memory pressure - it fills past a cap and checks that inserts
        # start being refused - so a smaller key count just means eviction keeps
        # up and oom_avoided_inserts stays zero. Scaling it would mean scaling
        # the cap and the overshoot together against a memory model nobody has
        # written down, so it stays full size and out of the short set.
        #
        # TestClean and the four install tests carry the label as well, not
        # because they are worth sanitizing but because the set has to stand on
        # its own: the install tests build the venv everything else runs in, and
        # TestClean removes the .dat files a previous run left in the build
        # directory - without it TestBarchPy loads stale data and fails on a key
        # count. They are defined first, so they still run first.
        set_tests_properties(
                TestClean
                TestInstallVENV TestInstallRedisPy TestInstallRequestsPy TestBarchInstallPy
                TestBarchPy TestBarchRPC
                TestRespClientLocal TestRespClientLocalRESP3
                TestAsyncPipeline TestHybrid
                TestFunctions TestFunctionSync TestFetchLuau
                TestKeysStream TestChaos TestScanGuarantees
                TestMerge TestSpaceThread TestBpopThread
                TestCompression TestScan
                PROPERTIES LABELS short)

    endif ()
endif ()

# --- one directory and one port block per test -------------------------------
#
# ctest -j needs both. Every test used to run in the build directory and write
# its shards there - 3535 .dat files in one place - and 34 of them had a port
# literal, eighteen of those the same 14000. Two at once fought over both.
#
# The directory half is not here: a python test calls scale.workdir() and moves
# into <build>/t/<testname> itself, so it is isolated however it was started -
# through ctest or by hand. Doing it from here would only have isolated the
# tests ctest launches. See TODO 209.
#
# What is left is the port, which the test cannot pick for itself without
# knowing about the others. Tests read it with scale.port(), which falls back to
# the old literal when BARCH_TEST_PORT is unset, so running one by hand is
# unchanged. See TODO 207.
#
# Ports go up in twenties because a few tests need more than one - fetchluautest
# binds a web server and a crow port beside its own.
if (BUILD_TESTING OR TEST_OD)
    get_property(_barch_tests DIRECTORY PROPERTY TESTS)
    # These four build the venv every other test runs in, and TestClean sweeps
    # the directories, so they belong in the build directory and own no port.
    set(_barch_shared_dir_tests
            TestClean TestInstallVENV TestInstallRedisPy TestInstallRequestsPy TestBarchInstallPy)
    # The short list is the one that cannot run beside anything, so it is the one
    # kept here. Everything else takes its port from BARCH_TEST_PORT and its
    # directory from scale.workdir(), which is all it needs to run in parallel.
    #
    #   - the lua and C++ tests under TestStarter bind their ports from lua and
    #     have no way to read the environment yet
    #   - TestBarchList and TestBarchSimpleClusterRPC share TEST_BUILD_PATH, where
    #     the valkey they start lives, and routetest.py publishes to 13000 from
    #     lua as well as from python so both sides have to move together
    #
    # See TODO 208.
    set(_barch_serial_tests
            TestIntegers TestDoubles BicycleTest TestAbc Test123 TestRandom
            TestBounds TestKeys LargeKeys Test1111111 TestMSG TestIntArithmetic
            TestComposites TestZRank TestBenchy TestHashBenchy
            TestDebuggableServerLock TestGlobDifferential
            TestBarchList TestBarchSimpleClusterRPC)
    set(_barch_port 20000)
    # TestClean deletes t/*/*.dat and the install tests build the venv, so with -j
    # they cannot simply run beside everything else - TestClean wiping a directory
    # underneath a running test is what five of them failed on the first time this
    # was tried. As a ctest fixture they are ordered before anything that needs
    # them instead. The chain between them is explicit because -j does not keep
    # definition order: the venv has to exist before pip runs in it.
    set_tests_properties(TestInstallRedisPy PROPERTIES DEPENDS TestInstallVENV)
    set_tests_properties(TestInstallRequestsPy PROPERTIES DEPENDS TestInstallRedisPy)
    set_tests_properties(TestBarchInstallPy PROPERTIES DEPENDS TestInstallRequestsPy)
    foreach (_s IN LISTS _barch_shared_dir_tests)
        set_tests_properties(${_s} PROPERTIES FIXTURES_SETUP barch_env)
    endforeach ()

    foreach (_t IN LISTS _barch_tests)
        list(FIND _barch_shared_dir_tests ${_t} _shared)
        if (_shared EQUAL -1)
            # a test that chose its own directory keeps it - TestBarchList and
            # TestBarchSimpleClusterRPC run in TEST_BUILD_PATH because that is
            # where the valkey they start lives. They share it, so they get a
            # resource lock instead of a directory and ctest will not run them
            # at the same time.
            get_test_property(${_t} WORKING_DIRECTORY _existing_dir)
            if (NOT _existing_dir STREQUAL "NOTFOUND" AND NOT _existing_dir STREQUAL "${CMAKE_BINARY_DIR}")
                set_tests_properties(${_t} PROPERTIES RESOURCE_LOCK shared_test_build_dir)
            endif ()
            # append, do not replace: TestRespClientLocalRESP3 and
            # TestGlobPerformanceJson already carry an ENVIRONMENT
            get_test_property(${_t} ENVIRONMENT _existing_env)
            if (_existing_env STREQUAL "NOTFOUND")
                set(_env "BARCH_TEST_PORT=${_barch_port}" "BARCH_TEST_NAME=${_t}")
            else ()
                set(_env ${_existing_env} "BARCH_TEST_PORT=${_barch_port}" "BARCH_TEST_NAME=${_t}")
            endif ()
            set_tests_properties(${_t} PROPERTIES
                    ENVIRONMENT "${_env}"
                    FIXTURES_REQUIRED barch_env
                    # a hung test should fail, not hold the run open for the
                    # 1500s ctest allows by default
                    TIMEOUT 600)
            list(FIND _barch_serial_tests ${_t} _serial)
            if (NOT _serial EQUAL -1)
                set_tests_properties(${_t} PROPERTIES RESOURCE_LOCK legacy_ports)
            endif ()
            math(EXPR _barch_port "${_barch_port} + 20")
        endif ()
    endforeach ()
endif ()
