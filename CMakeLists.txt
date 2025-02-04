cmake_minimum_required(VERSION 3.28)
project(cdict)
enable_testing()

set(CMAKE_CPP_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Weffc++")
include_directories("module/fast_float/include")

## CMake sub project executables only works if you pretend they are not subprojects
execute_process(COMMAND mkdir build RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
execute_process(COMMAND cmake ".." RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)
execute_process(COMMAND make RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

file(GLOB SOURCES
    "src/*.c" "src/*.cpp"
)

add_library(cdict SHARED ${SOURCES})
add_test(NAME TestStarter
        COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

#add_test(
#        NAME start_valkey_server
#        COMMAND valkey-server --loadmodule ${CMAKE_BINARY_DIR}/libcdict.so
#        WORKING_DIRECTORY ${valkey_SOURCE_DIR}/src/)

#add_test(
#        NAME run_lua
#        COMMAND valkey-cli --eval "${CMAKE_SOURCE_DIR}/test/test.lua"
#        WORKING_DIRECTORY)