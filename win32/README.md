# barchd for Windows

This folder builds `barchd.exe`, the standalone barch server, for 64-bit Windows.
It's the same server as on Linux: it speaks RESP, runs Luau functions, serves
HTTP transports and saves to disk. The Valkey module and the Python binding
aren't built for Windows.

## Running it

```
barchd.exe --port 14000 --dir C:\barch-data
```

`--dir` is where the shards are saved. Only one barchd can use a directory at
a time; a second one exits with a message saying the directory is held.

Any Redis client can connect, for example `redis-cli -p 14000` from WSL or a
Windows build of redis-cli. Stop it with Ctrl+C. Run `SAVE` before stopping it
some other way, such as closing the window or ending it in Task Manager.

The exe is self-contained. It needs Windows 10 version 1803 or later, and a CPU
with AVX2 (Intel Haswell or AMD Excavator, 2013 or later).

## What's different from Linux

- **The git function sync** works, but it needs `git.exe` on the `PATH`.
- **Thread pinning** uses Windows affinity masks and only covers the first 64
  logical CPUs.
- **Growing an in-memory arena** copies it. Linux can grow one in place, so
  very large arenas are slower to grow on Windows. Arenas backed by a file
  (`arena_map`) grow without a copy.
- **Memory limits from cgroups** don't exist on Windows. barch uses the
  machine's physical memory as the limit.
- **Backtraces** in lock timeout reports show raw addresses only.

Saved data uses the same layout as on Linux, but nobody has tried loading a
Linux data directory on Windows yet.

## Building on Windows

Install [MSYS2](https://www.msys2.org/), open the **MSYS2 MINGW64** shell, and
run:

```
pacman -S --needed git mingw-w64-x86_64-gcc mingw-w64-x86_64-cmake \
    mingw-w64-x86_64-ninja mingw-w64-x86_64-openssl
cmake -S win32 -B build-win -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-win --target barchd
```

The first configure takes a few minutes, because it downloads the same
dependencies the Linux build uses.

MSVC isn't supported; barch needs GCC.

## Cross-compiling from Linux

On Ubuntu, install `g++-mingw-w64-x86-64-posix`. Get OpenSSL for MinGW by
unpacking MSYS2's `mingw-w64-x86_64-openssl` package from
<https://repo.msys2.org/mingw/mingw64/>. Then run:

```
cmake -S win32 -B build-win -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=win32/mingw-cross.cmake \
    -DOPENSSL_ROOT_DIR=/path/to/openssl/mingw64 \
    -DCMAKE_FIND_ROOT_PATH=/path/to/openssl/mingw64
cmake --build build-win --target barchd
```

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `BARCH_MARCH` | `x86-64-v3` | The `-march` value. Use `x86-64-v2` for CPUs without AVX2, or `native` for a build that only runs on the machine that built it. |

## Testing

`smoke_test.py` starts a barchd and runs PING, SET, GET and a key with an
expiry. Then it saves, kills the server, starts it again and checks the keys
are still there. It does this twice: once with in-memory arenas, and once with
arenas backed by files (`arena_dir`), writing enough keys that the files have
to grow. The Windows CI job runs it on every push:

```
python win32/smoke_test.py build-win/barchd.exe
```

It works against a Linux build too.

## How the port is put together

`win32/CMakeLists.txt` is a separate CMake project. The top-level CMakeLists.txt
is still the Linux build, and nothing in it changed for Windows.

- `win32/include/` has stand-in headers for the POSIX headers MinGW doesn't
  have (`sys/mman.h`, `execinfo.h` and others). It also has `barch_win32.h`,
  which the build includes ahead of every source file.
- `win32/src/posix_compat.cpp` implements those calls with the Windows API:
  `mmap` and `mremap` on file mappings and `VirtualAlloc`, `pread`, `fsync`,
  and a `rename` that replaces an existing file.
- Where Windows needs different logic, the shared source has an
  `#ifdef _WIN32` branch. This covers the git command runner, the
  data-directory lock, the idle-connection check in the RESP client, thread
  affinity and the queue file's write-through mode.

When you bump a dependency version in the top-level CMakeLists.txt, bump it in
`win32/CMakeLists.txt` too.
