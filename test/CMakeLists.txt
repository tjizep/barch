cmake_minimum_required(VERSION 3.9)
project(TestStarter VERSION 0.0.1 LANGUAGES CXX)
enable_testing()
include(FetchContent)
# Pinned to 8.1.12 - TODO 39. It was the branch `8.1`, which moves. Not shallow,
# for the same reason NumKong isn't: a shallow clone can't reliably fetch a commit.
set(valkey_commit 38316e6c481cc6d265ab52482223c68f63f0ac30)
FetchContent_Declare(
        valkey
        GIT_REPOSITORY https://github.com/valkey-io/valkey.git
        GIT_TAG ${valkey_commit}
)
# Populated only when the pin isn't checked out. FetchContent records the path
# of the cmake that cloned, and another one deletes the checkout and clones again.
set(valkey_SOURCE_DIR ${FETCHCONTENT_BASE_DIR}/valkey-src)
set(valkey_head "")
execute_process(COMMAND git --git-dir=${valkey_SOURCE_DIR}/.git rev-parse HEAD
        OUTPUT_VARIABLE valkey_head
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
if (NOT "${valkey_head}" STREQUAL "${valkey_commit}")
    FetchContent_Populate(valkey)
endif()

message(${valkey_SOURCE_DIR})

# The directory outlives a configure, so make runs only when the pin isn't built.
# make alone would not do: it keeps old deps/ libraries after a change of commit.
# The server names its own commit. One that can't run here names none, and gets
# built again.
string(SUBSTRING ${valkey_commit} 0 8 valkey_sha)
set(valkey_built "")
execute_process(COMMAND ${valkey_SOURCE_DIR}/src/valkey-server --version
        OUTPUT_VARIABLE valkey_built
        ERROR_QUIET)
if (NOT valkey_built MATCHES "sha=${valkey_sha}"
        OR NOT EXISTS ${valkey_SOURCE_DIR}/src/valkey-cli
        OR NOT EXISTS ${valkey_SOURCE_DIR}/src/valkey-benchmark)
    execute_process(COMMAND make distclean RESULT_VARIABLE result
            WORKING_DIRECTORY ${valkey_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "could not clean valkey ${result}")
    endif()
    execute_process(COMMAND make RESULT_VARIABLE result
            WORKING_DIRECTORY ${valkey_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "could not build valkey ${result}")
    endif()
endif()
option(COVERAGE "Enable coverage" OFF)
if (COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Weffc++")
set(CMAKE_CPP_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(TestStarter test_starter.cpp)
