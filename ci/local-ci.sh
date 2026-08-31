#!/bin/sh
#
# Run the GitHub CI build in a container, so a compiler the host does not have
# gets a look at the tree before a push does.
#
# The failure this was written for: NumKong takes the native `__bf16` whenever
# `__AVX512BF16__` is defined, gcc only grew that type in 13, and gcc 11 defines
# the macro anyway. You need gcc 11 and a bf16-capable CPU together to see it,
# so it is invisible on a 24.04 host and lands as 600 errors on the 22.04 job.
#
#   ci/local-ci.sh 22                 configure and build, gcc 11
#   ci/local-ci.sh 24                 the same on gcc 13
#   ci/local-ci.sh 22 --target lbarch just one target
#   ci/local-ci.sh 22 --tests         build, then ctest
#   ci/local-ci.sh 22 --shell         a prompt in the container, nothing built
#   ci/local-ci.sh 22 --configure     stop after cmake, for a quick sanity check
#
# The tree is bind mounted, not copied, and the container runs as you, so
# nothing lands root-owned and there is no second checkout to get out of step.
# Each image builds into its own `ci-build-ubuntu<n>` directory, ignored by git.
#
# One thing to know about: TEST_OD=ON has the top level CMakeLists configure and
# build a TestStarter helper into `test/<TEST_BUILD_DIR>` inside the source tree.
# No build type is set here, the same as the workflows, so that is `test/build`,
# while a RelWithDebInfo host build uses `test/RelWithDebInfo`. They do not tread
# on each other unless you make the build types match. Both are ignored output.

set -e

ver=${1:-24}
shift 2>/dev/null || true

case "$ver" in
    22|24) ;;
    *) echo "usage: $0 [22|24] [--target NAME] [--tests] [--shell] [--configure]" >&2; exit 2 ;;
esac

target=""
tests=""
shell=""
configure_only=""
while [ $# -gt 0 ]; do
    case "$1" in
        --target)    target=$2; shift 2 ;;
        --tests)     tests=yes; shift ;;
        --shell)     shell=yes; shift ;;
        --configure) configure_only=yes; shift ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

root=$(cd "$(dirname "$0")/.." && pwd)
image=barch-ci-ubuntu$ver
builddir=ci-build-ubuntu$ver

echo "==> building image $image"
docker build -q -t "$image" -f "$root/ci/Dockerfile.ubuntu$ver" "$root/ci" >/dev/null

# a tty only when there is one to give, so this works from a script or a pipe too
tty_flags=""
if [ -t 0 ] && [ -t 1 ]; then
    tty_flags="-it"
fi

run() {
    docker run --rm $tty_flags \
        -u "$(id -u):$(id -g)" \
        -v "$root:/src" -w /src \
        -e HOME=/tmp \
        "$image" "$@"
}

if [ -n "$shell" ]; then
    run /bin/bash
    exit $?
fi

# the same flags the workflow uses, and an explicit -j: with no number cmake
# hands make a bare `-j`, which starts every source at once - see DONE 175
script="set -xe
cmake -B $builddir -DTEST_OD=ON -DCMAKE_CXX_FLAGS=' -Wall -Wextra '
"
if [ -n "$configure_only" ]; then
    :
elif [ -n "$target" ]; then
    script="$script
cmake --build $builddir --target $target --parallel \$(nproc)"
else
    script="$script
cmake --build $builddir --target barch --parallel \$(nproc)
cmake --build $builddir --target lbarch --parallel \$(nproc)
cmake --build $builddir --target globdifftest --parallel \$(nproc)
cmake --build $builddir --target locktest --parallel \$(nproc)"
fi
if [ -n "$tests" ]; then
    script="$script
cd $builddir && ctest --output-on-failure"
fi

echo "==> ubuntu $ver, building into $builddir"
run /bin/sh -c "$script"
