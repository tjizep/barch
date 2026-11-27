#!/bin/bash
# Installs barchd into /opt/barch on the Packer build instance. Runs as root.
#
# Two ways to get the binary:
#   BARCHD_TARBALL_URL and BARCHD_TARBALL_SHA256 set: download a prebuilt
#     tarball with barchd at its top level, and check its hash.
#   Otherwise: build barchd from source at BARCH_REF, and check that the tag
#     points at BARCH_COMMIT. The build tools are removed again afterwards.
#
# The v0.5.8b release has no barchd asset, so the source build is the default.
set -euo pipefail

BARCH_REPO=${BARCH_REPO:-https://github.com/tjizep/barch.git}
BARCH_REF=${BARCH_REF:-v0.5.8b}
BARCH_COMMIT=${BARCH_COMMIT:-d915a6ba3e9bb86f7600a2a6028edfb36426b6d8}
BARCHD_TARBALL_URL=${BARCHD_TARBALL_URL:-}
BARCHD_TARBALL_SHA256=${BARCHD_TARBALL_SHA256:-}
STAGE=${STAGE:-/tmp/barch-files}
# barch builds with -march=native. An AMI built that way on a new CPU dies with
# SIGILL on an older one, so the build targets a baseline every current EC2 type
# has: AVX2 on x86-64 (Haswell, Zen and later), Neoverse N1 on arm64 (Graviton2).
case "$(uname -m)" in
    x86_64) default_march=x86-64-v3 ;;
    aarch64) default_march=armv8.2-a+crypto+fp16+rcpc+dotprod ;;
    *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
BARCH_MARCH=${BARCH_MARCH:-$default_march}

export DEBIAN_FRONTEND=noninteractive

# Wait for cloud-init, which runs apt itself on first boot of the build instance.
cloud-init status --wait >/dev/null || true

apt-get update
apt-get -y -o Dpkg::Options::=--force-confold full-upgrade
# redis-cli for first boot and for the buyer. git for barchd's git sync, which
# runs the git binary; the Debezium sink is a package it loads that way. It is
# installed here, before the build, so the build's cleanup leaves it. The
# runtime libraries barchd links against (libssl3, libzstd1, zlib) are part of
# the Ubuntu base image.
apt-get -y install --no-install-recommends redis-tools ca-certificates curl git

install -d -m 755 /opt/barch/bin /opt/barch/libexec
work=$(mktemp -d)

if [ -n "$BARCHD_TARBALL_URL" ]; then
    [ -n "$BARCHD_TARBALL_SHA256" ] || { echo "BARCHD_TARBALL_SHA256 is not set" >&2; exit 1; }
    curl -fsSL -o "$work/barchd.tar.gz" "$BARCHD_TARBALL_URL"
    echo "$BARCHD_TARBALL_SHA256  $work/barchd.tar.gz" | sha256sum -c -
    tar -xzf "$work/barchd.tar.gz" -C "$work" barchd
    install -m 755 "$work/barchd" /opt/barch/bin/barchd
    source_note="tarball $BARCHD_TARBALL_URL sha256 $BARCHD_TARBALL_SHA256"
else
    # python3-dev and swig: barch's CMakeLists makes the `barch` target only
    # with Python, then links to it anyway, so configure fails without them.
    # Only barchd is built. TODO 983 fixed that after v0.5.8b; drop these two
    # once barch_commit is past it.
    build_deps=(build-essential cmake git libssl-dev pkg-config python3-dev swig)
    # only the packages this script adds are removed again
    to_remove=()
    for p in "${build_deps[@]}"; do
        dpkg-query -W -f='${Status}' "$p" 2>/dev/null | grep -q 'install ok installed' || to_remove+=("$p")
    done
    apt-get -y install --no-install-recommends "${build_deps[@]}"

    git clone --depth 1 --branch "$BARCH_REF" "$BARCH_REPO" "$work/barch"
    got=$(git -C "$work/barch" rev-parse HEAD)
    if [ -n "$BARCH_COMMIT" ] && [ "$got" != "$BARCH_COMMIT" ]; then
        echo "$BARCH_REF is $got, expected $BARCH_COMMIT" >&2
        exit 1
    fi
    n=$(grep -c -- '-march=native' "$work/barch/CMakeLists.txt" || true)
    [ "$n" -gt 0 ] || { echo "no -march=native in CMakeLists.txt; check BARCH_MARCH by hand" >&2; exit 1; }
    sed -i "s/-march=native/-march=$BARCH_MARCH/g" "$work/barch/CMakeLists.txt"
    # The configure step downloads barch's pinned dependencies from GitHub.
    cmake -S "$work/barch" -B "$work/build" -DCMAKE_BUILD_TYPE=Release
    # One compile can take about a gigabyte, so no more jobs than gigabytes.
    mem_gb=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1048576 ))
    jobs=$(nproc)
    [ "$mem_gb" -lt "$jobs" ] && jobs=$(( mem_gb > 1 ? mem_gb : 1 ))
    cmake --build "$work/build" --target barchd --parallel "$jobs"
    install -m 755 "$work/build/barchd" /opt/barch/bin/barchd
    source_note="source $BARCH_REPO $BARCH_REF $got -march=$BARCH_MARCH"

    if [ ${#to_remove[@]} -gt 0 ]; then
        apt-get -y purge "${to_remove[@]}"
    fi
    apt-get -y autoremove --purge
fi
rm -rf "$work"

# Fail the build now if a library barchd needs went with the build tools.
if ldd /opt/barch/bin/barchd | grep -q 'not found'; then
    ldd /opt/barch/bin/barchd >&2
    exit 1
fi
version=$(/opt/barch/bin/barchd --version)
echo "barchd $version"
printf '%s\n%s\n' "$version" "$source_note" >/opt/barch/VERSION
ln -sf /opt/barch/bin/barchd /usr/local/bin/barchd

useradd --system --home-dir /var/lib/barch --shell /usr/sbin/nologin barch 2>/dev/null || true
install -d -o barch -g barch -m 750 /var/lib/barch
install -d -m 755 /etc/barch

install -m 755 "$STAGE/barch-firstboot.sh" /opt/barch/libexec/barch-firstboot.sh
install -m 644 "$STAGE/barchd.env" /etc/barch/barchd.env
install -m 644 "$STAGE/barchd.service" /etc/systemd/system/barchd.service
install -m 644 "$STAGE/barch-firstboot.service" /etc/systemd/system/barch-firstboot.service
install -m 644 "$STAGE/USAGE.md" /opt/barch/USAGE.md
systemctl daemon-reload
systemctl enable barch-firstboot.service barchd.service
