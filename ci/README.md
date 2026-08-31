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
