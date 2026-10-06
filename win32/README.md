# barch for Windows

This folder builds barch for 64-bit Windows:

- `barchd.exe`, the standalone server. It's the same server as on Linux: it
  speaks RESP, runs Luau functions, serves HTTP transports and saves to disk.
- `_barch.pyd` and `barch.py`, the Python module, for Python 3.12 from
  python.org.

The Valkey module isn't built, because Valkey doesn't run on Windows.

## Running it

```
barchd.exe --port 14000 --dir C:\barch-data
```

`--dir` is where the shards are saved. Only one barchd can use a directory at
a time; a second one exits with a message saying the directory is held.

Any Redis client can connect, for example `redis-cli -p 14000` from WSL or a
Windows build of redis-cli. Stop it with Ctrl+C. Run `SAVE` before stopping it
some other way, such as closing the window or ending it in Task Manager.

To stop barchd from another program, set the named event
`Local\barchd-stop-<pid>`. barchd saves and exits, the same as on SIGTERM on
Linux. The tests stop it this way (see `test/scale.py`).

The exe is self-contained. It needs Windows 10 version 1803 or later, and a CPU
with AVX2 (Intel Haswell or AMD Excavator, 2013 or later).

## Using the Python module

Put `_barch.pyd` and `barch.py` in the same folder, and put that folder on
`sys.path` (or `PYTHONPATH`). Then `import barch` works as it does on Linux.
The module only loads in python.org's Python 3.12 (64-bit), because it's built
against that Python's `python312.dll`.

## What's different from Linux

- **The git function sync** works, but it needs `git.exe` on the `PATH`.
- **Thread pinning** uses Windows affinity masks and only covers the first 64
  logical CPUs.
- **Growing an in-memory arena** copies it. Linux can grow one in place, so
  very large arenas are slower to grow on Windows. Arenas backed by a file
  (`arena_map`) grow without a copy.
- **Memory limits from cgroups** don't exist on Windows. barch uses the
  machine's physical memory as the limit.
- **Backtraces** in lock timeout reports and fatal errors show `module+offset`,
  not function names. To read one, build with `-DCMAKE_CXX_FLAGS=-g`, add the
  offset to the `ImageBase` that `objdump -p` reports for that module, and give
  the result to `addr2line -f -C -i -e <module>`.

Saved data uses the same layout as on Linux, but nobody has tried loading a
Linux data directory on Windows yet.

## Building on Windows

Install [MSYS2](https://www.msys2.org/) and Python 3.12 from python.org. Open
the **MSYS2 UCRT64** shell (UCRT64, because python.org's Python uses the UCRT C
runtime), and run:

```
pacman -S --needed git mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake \
    mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-openssl \
    mingw-w64-ucrt-x86_64-swig mingw-w64-ucrt-x86_64-libmariadbclient \
    mingw-w64-ucrt-x86_64-postgresql mingw-w64-ucrt-x86_64-gettext-runtime \
    mingw-w64-ucrt-x86_64-libiconv mingw-w64-ucrt-x86_64-zlib
cmake -S win32 -B build-win -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DBARCH_PYTHON_ROOT=C:/path/to/Python312
cmake --build build-win --target barchd barch
```

`BARCH_PYTHON_ROOT` is the folder with Python's `include` and `libs` folders in
it. Leave it out to build barchd only.

The MySQL and PostgreSQL clients (for `foreign=mysql` and `foreign=postgres`
spaces) come from the last five packages. Both are linked statically, so they
add no DLLs. If they aren't installed, the build still works without them and
says so when you configure.

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
    -DCMAKE_FIND_ROOT_PATH=/path/to/openssl/mingw64 \
    -DBARCH_PYTHON_ROOT=/path/to/python/tools
cmake --build build-win --target barchd barch
```

For the Python module, install `swig` and get python.org's Python from its
NuGet package (`https://www.nuget.org/packages/python`). A `.nupkg` is a zip,
and its `tools` folder is what `BARCH_PYTHON_ROOT` wants. Ubuntu's MinGW uses
the older msvcrt C runtime rather than the UCRT; a module built that way loads
and runs fine in python.org's Python, but CI builds the real one in UCRT64.
One difference shows: the module and Python then keep separate copies of the
environment, so a variable Python sets after it starts isn't seen by barch.

For the SQL clients, unpack MSYS2's MINGW64 (not UCRT64) packages of the same
names next to OpenSSL. Their current libintl also wants a few C runtime entry
points Ubuntu's older MinGW doesn't export, so a local link needs a small shim
for `mbrtowc`, `wcrtomb` and `mbrlen`; CI's toolchain doesn't.

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `BARCH_MARCH` | `x86-64-v3` | The `-march` value. Use `x86-64-v2` for CPUs without AVX2, or `native` for a build that only runs on the machine that built it. |
| `BARCH_PYTHON_ROOT` | `$pythonLocation` | A python.org Python to build `_barch.pyd` for. Empty means barchd only. |
| `BARCH_WIN_MYSQL` | `ON` | Build in the MySQL client (MariaDB's `libmariadbclient.a`) when it's found. |
| `BARCH_WIN_POSTGRES` | `ON` | Build in the PostgreSQL client (`libpq.a`) when it's found. |

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

`run_tests.py` runs barch's Python tests against the Windows build. It reads
the tests from the top-level CMakeLists.txt, so there's no second list to keep
up to date, and gives each one the same port, name and `BARCHD` setting that
ctest gives it on Linux. Tests that can't run on Windows are listed in
`test_skips.txt`, each with a reason.

```
python -m pip install redis requests
python win32/run_tests.py --build build-win -j 4
python win32/run_tests.py --build build-win -L short     # the short set
python win32/run_tests.py --build build-win -R TestBarchd
```

Each test's output goes to `build-win/testroot/logs/<test>.log`. The tests also
need `git` on the `PATH`.

From Linux, the same runner drives the tests under Wine:

```
python3 win32/run_tests.py --build build-win -j 4 --path-prefix Z: \
    --python "wine /path/to/python/tools/python.exe" --port-base 24000
```

`--port-base` moves the tests' ports away from the 20000 range ctest uses, so a
Linux ctest run and a Wine run on the same machine don't connect to each other's
servers. Keep it under 32768: from there up Linux hands ports out to outgoing
connections, and one of those can hold a test's port.

Wine is close enough to find most problems, but it isn't Windows. It doesn't
support the TCP keepalive settings that redis-py turns on, so the Wine
Python needs a `sitecustomize.py` that removes `socket.TCP_KEEPIDLE`,
`TCP_KEEPINTVL` and `TCP_KEEPCNT`.

## How the port is put together

`win32/CMakeLists.txt` is a separate CMake project. The top-level CMakeLists.txt
is still the Linux build, and nothing in it changed for Windows. barch's
sources are compiled once, into an object library, and linked into both
barchd.exe and `_barch.pyd`.

- `win32/include/` has stand-in headers for the POSIX headers MinGW doesn't
  have (`sys/mman.h`, `execinfo.h` and others). It also has `barch_win32.h`,
  which the build includes ahead of every source file.
- The SQL clients are static archives built for a DLL world, which takes two
  adjustments at link time. MariaDB's client calls curl through DLL import
  pointers, which `win32/src/mariadb_curl_imports.c` provides for barch's
  static curl. libpq carries its own Windows pthread emulation, with different
  types from winpthreads; the build links copies of the PostgreSQL archives
  with those functions renamed to `pq_pthread_*`, so the two never meet.
- `win32/src/posix_compat.cpp` implements those calls with the Windows API:
  `mmap` and `mremap` on file mappings and `VirtualAlloc`, `pread`, `fsync`,
  and a `rename` that replaces an existing file.
- Where Windows needs different logic, the shared source has an
  `#ifdef _WIN32` branch. This covers the git command runner, the
  data-directory lock, the idle-connection check in the RESP client, thread
  affinity, the queue file's write-through mode, and barchd's stop event.
- `test/scale.py`, which every Python test imports, makes two adjustments on
  Windows. A test's SIGTERM or SIGINT to barchd sets barchd's stop event
  instead of killing it. And pipes get 1 MB of buffer instead of Windows'
  4 KB, because many tests only read barchd's output after it exits, and
  Linux's 64 KB pipes are what that relies on.

When you bump a dependency version in the top-level CMakeLists.txt, bump it in
`win32/CMakeLists.txt` too.
