# Toolchain file for building win32/CMakeLists.txt from Linux with MinGW-w64.
#
#   cmake -S win32 -B build-win -DCMAKE_TOOLCHAIN_FILE=win32/mingw-cross.cmake ...
#
# Ubuntu's g++-mingw-w64-x86-64-posix package gives the names below. The posix
# flavour matters: the win32 one has no std::thread. Point MINGW_PREFIX
# somewhere else if your compiler lives elsewhere or has another suffix.
set(CMAKE_SYSTEM_NAME Windows)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

set(MINGW_TRIPLE x86_64-w64-mingw32 CACHE STRING "MinGW-w64 target triple")
set(MINGW_PREFIX "" CACHE PATH "Directory holding the cross compilers, if not on PATH")
set(MINGW_SUFFIX "-posix" CACHE STRING "Suffix on the compiler names")

if (MINGW_PREFIX)
    set(_mingw_bin "${MINGW_PREFIX}/")
else ()
    set(_mingw_bin "")
endif ()

set(CMAKE_C_COMPILER   ${_mingw_bin}${MINGW_TRIPLE}-gcc${MINGW_SUFFIX})
set(CMAKE_CXX_COMPILER ${_mingw_bin}${MINGW_TRIPLE}-g++${MINGW_SUFFIX})
set(CMAKE_RC_COMPILER  ${_mingw_bin}${MINGW_TRIPLE}-windres)
set(CMAKE_AR           ${_mingw_bin}${MINGW_TRIPLE}-ar CACHE FILEPATH "")
set(CMAKE_RANLIB       ${_mingw_bin}${MINGW_TRIPLE}-ranlib CACHE FILEPATH "")

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
