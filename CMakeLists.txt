cmake_minimum_required(VERSION 3.22)
project(barch VERSION 0.1.5 LANGUAGES CXX)
enable_testing()
message("barch version: ${PROJECT_VERSION}")

if (NOT ADD_ZSTD_COMPRESSION)
    set(ADD_ZSTD_COMPRESSION OFF)
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

set(CMAKE_CPP_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(USE_AVX_INSTRUCTIONS ON)
#remove -Werror because fmtlib
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Weffc++")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

include(FetchContent)
if (USE_ASIO)
    message("Using msgpack")
    FetchContent_Declare(
            msgpack
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SOURCE_SUBDIR build/cmake
            URL "https://github.com/msgpack/msgpack-c/archive/refs/tags/cpp-7.0.0.tar.gz"
    )
    FetchContent_MakeAvailable(msgpack)
    include_directories(${msgpack_SOURCE_DIR}/include)
else ()
    message("Boost.Asio not used use -USE_ASIO=ON")
endif()
if (USE_ASIO)
    message("Using std alone ASIO")
    FetchContent_Declare(
            asio
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SOURCE_SUBDIR build/cmake
            URL "https://sourceforge.net/projects/asio/files/asio/1.30.2%20%28Stable%29/asio-1.30.2.tar.gz/download"
    )
    FetchContent_MakeAvailable(asio)
    include_directories(${asio_SOURCE_DIR}/include)
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
            URL "https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz"
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
endif ()
if (ADD_FMT)

    FetchContent_Declare(
            fmt
            GIT_REPOSITORY https://github.com/fmtlib/fmt
            GIT_TAG        123913715afeb8a437e6388b4473fcc4753e1c9a) # 11.4
    SET(BUILD_SHARED_LIBS FALSE)
    SET(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
    FetchContent_MakeAvailable(fmt)
endif ()

if (ADD_ZSTD_COMPRESSION)
    FetchContent_MakeAvailable(zstd)
endif ()

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
#-Weffc++ is removed else fmtlib does not compile - because it's a little ...s

include_directories(src)
include_directories(external/include)

include_directories(${fast_float_SOURCE_DIR}/include)
include_directories(${zstd_SOURCE_DIR}/lib)


if (NOT TEST_OD)
    message("Testing not enabled Use -DTEST_OD=ON")
else()
    ## CMake sub project executables only works if you pretend they are not subprojects
    execute_process(COMMAND rm -rf build RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND mkdir build RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ".."  RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)
    execute_process(COMMAND make RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

endif ()

file(GLOB SOURCES
    "src/*.c" "src/*.cpp" "src/*.cxx"
)
if (ADD_SWIG)
    find_package (Python3 COMPONENTS Interpreter Development)
    find_package (SWIG)
    include (UseSWIG)
    set_property(SOURCE src/barch.i PROPERTY CPLUSPLUS ON)
    if (${Python3_FOUND})
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
        message("COMMAND cp ${CMAKE_BINARY_DIR}/_${PROJECT_NAME}.so ${CMAKE_BINARY_DIR}/b/_${PROJECT_NAME}.so")
        add_custom_command(TARGET barch POST_BUILD
            COMMAND cp ${CMAKE_BINARY_DIR}/_${PROJECT_NAME}.so ${CMAKE_BINARY_DIR}/b/_${PROJECT_NAME}.so
            BYPRODUCTS ${CMAKE_BINARY_DIR}/b/_${PROJECT_NAME}.so
        )
        message("COMMAND cp ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.py ${CMAKE_BINARY_DIR}/b/${PROJECT_NAME}.py")
        add_custom_command(TARGET barch POST_BUILD
            COMMAND cp ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.py ${CMAKE_BINARY_DIR}/b/${PROJECT_NAME}.py
            BYPRODUCTS ${CMAKE_BINARY_DIR}/b/${PROJECT_NAME}.py
        )
        message("COMMAND cp ${CMAKE_SOURCE_DIR}/test/testbarch.py ${CMAKE_BINARY_DIR}/testbarch.py")
        add_custom_command(TARGET barch POST_BUILD
            COMMAND cp ${CMAKE_SOURCE_DIR}/test/testbarch.py ${CMAKE_BINARY_DIR}/testbarch.py
            BYPRODUCTS ${CMAKE_BINARY_DIR}/testbarch.py
        )
        message("COMMAND cp ${CMAKE_SOURCE_DIR}/test/repltest.py ${CMAKE_BINARY_DIR}/repltest.py")
        add_custom_command(TARGET barch POST_BUILD
                COMMAND cp ${CMAKE_SOURCE_DIR}/test/repltest.py ${CMAKE_BINARY_DIR}/repltest.py
                BYPRODUCTS ${CMAKE_BINARY_DIR}/repltest.py
        )
        set(PYBARCH_TEST ON)
    else ()
        message("SWIG python interface generation skipped - python not found")
    endif ()
else ()
    message("SWIG local interface not used use -DADD_SWIG=ON")
    message("building non SWIG library")
    add_library(barch SHARED ${SOURCES})
endif ()

if (ADD_ZSTD_COMPRESSION)
    target_link_libraries(barch PRIVATE libzstd_static)
endif ()

if (ADD_FMT)
    target_link_libraries(barch PRIVATE fmt::fmt)
endif ()

if (NOT TEST_OD)
    message("Testing not enabled tests not generated :Use -DTEST_OD=ON")
else()

    add_test(NAME TestIntegers
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testintegers.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestDoubles
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testdouble.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME BicycleTest
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testbicycle.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestAbc
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testabc.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME Test123
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} test123.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestRandom
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testrnd.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestBounds
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testbounds.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestKeys
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testkeys.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME LargeKeys
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testgloblarge.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME Test1111111
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} test111111.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestMSG
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testmulti.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestIntArithmetic
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testintarith.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestComposites
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testcomposites.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)

    add_test(NAME TestZRank
            COMMAND ${CMAKE_SOURCE_DIR}/test/build/TestStarter ${CMAKE_BINARY_DIR} testzrank.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/build)
    if (PYBARCH_TEST)
        message("Python test: WORKING_DIRECTORY ${CMAKE_BINARY_DIR}")
        add_test(NAME TestBarchInstallPy
                COMMAND "pip" install .
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchPy
                COMMAND "python" ${CMAKE_BINARY_DIR}/testbarch.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy
                COMMAND "python" ${CMAKE_BINARY_DIR}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy
                COMMAND "python" ${CMAKE_BINARY_DIR}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    endif ()
endif ()

