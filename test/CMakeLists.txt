cmake_minimum_required(VERSION 3.9)
project(TestStarter VERSION 0.0.1 LANGUAGES CXX)
enable_testing()
include(FetchContent)
FetchContent_Declare(
        valkey
        GIT_REPOSITORY https://github.com/valkey-io/valkey.git
        GIT_TAG 8.1.3
)
FetchContent_Populate(valkey)

message(${valkey_SOURCE_DIR})

execute_process(COMMAND make RESULT_VARIABLE result
        WORKING_DIRECTORY ${valkey_SOURCE_DIR})

if(result)
    message(FATAL_ERROR "could not build valkey ${result}")
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
