cmake_minimum_required(VERSION 3.28)
project(TestStarter)
enable_testing()
include(FetchContent)
FetchContent_Declare(
        valkey
        GIT_REPOSITORY https://github.com/valkey-io/valkey.git
        GIT_TAG 8.0
)
FetchContent_Populate(valkey)

message(${valkey_SOURCE_DIR})

execute_process(COMMAND make RESULT_VARIABLE result
        WORKING_DIRECTORY ${valkey_SOURCE_DIR})

if(result)
    message(FATAL_ERROR "could not build valkey ${result}")
endif()

set(CMAKE_CPP_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Weffc++")

add_executable(TestStarter test_starter.cpp)
