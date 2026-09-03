# Running the CI build locally

`local-ci.sh` runs the GitHub workflow's build inside a container, so a
compiler this machine does not have gets a look at the tree before a push
does.

```
ci/local-ci.sh 22                 configure and build, gcc 11
ci/local-ci.sh 24                 the same on gcc 13
ci/local-ci.sh 22 --configure     stop after cmake
ci/local-ci.sh 22 --target lbarch just one target
ci/local-ci.sh 22 --tests         build, then ctest
ci/local-ci.sh 22 --shell         a prompt in the container
```

The tree is bind mounted rather than copied and the container runs as you,
so nothing lands root-owned and there is no second checkout to drift. Each
image builds into its own `ci-build-ubuntu<n>`, which git ignores.

## What it is and is not

It reproduces the **compiler**, which is where the differences that matter
have been. It does not reproduce the hosted runner: the images here install
what the build reaches for rather than the hundred-odd packages a GitHub
image ships with, and a missing dependency will look like a build failure.
It also cannot reproduce the runner's *hardware* — see below, because that
turned out to matter.

## The failure this was written for

`-march=native` on a host with AVX512-BF16 makes gcc define
`__AVX512BF16__`. NumKong reads that as "this compiler has bf16" and does
`typedef __bf16 nk_bf16_t`. gcc only grew the `__bf16` scalar type in 13,
and 11 and 12 define the macro anyway, so the typedef fails and around 600
errors follow it.

Seeing it needs gcc under 13 *and* a CPU with bf16 at the same time. A
24.04 host never shows it whatever the CPU, and a 22.04 runner on an older
CPU does not either — which is why it arrived looking like a change in our
code rather than a change of runner. `CMakeLists.txt` now defines
`NK_NATIVE_BF16=0` for gcc under 13, which falls back to their
`unsigned short`: same two bytes, same layout.

Note the second half of that: this only reproduces here because this
machine's CPU has bf16. On a host without it, `ci/local-ci.sh 22` would
have compiled quite happily.

## Thread sanitizer

Not part of any CI job - it is far too slow for the whole suite - but useful on
one or two tests. `_barch.so` is dlopened by python, so the sanitizer runtime is
not in the process at start and its interceptors never install. Preloading it
fixes that, and then TSan needs ASLR out of the way or it dies on its own
address space check with `unexpected memory mapping`.

Configure:

    cmake -B build-tsan -DTEST_OD=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
      -DCMAKE_C_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
      -DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=thread" \
      -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
    cmake --build build-tsan --target barch --parallel 2

Run, from the build directory:

    setarch -R env LD_PRELOAD=$(gcc -print-file-name=libtsan.so.2) \
      TSAN_OPTIONS="halt_on_error=0 history_size=4 suppressions=../ci/tsan.supp" \
      PYTHONPATH=. python3 ../test/chaostest.py

`chaostest.py` and `fetchluautest.py` are the two that have earned their keep -
between them they found DONE 186 and DONE 188.

Use the suppressions file. Without it every write unlock is reported, because
TSan does not intercept `pthread_mutex_timedlock` and barch takes its write
latch with `try_lock_for`; that was a thousand reports against a hundred real
ones. See `ci/tsan.supp` and DONE 189.
