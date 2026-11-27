#!/bin/bash
# Checks the image before it is cleaned and snapshotted. Runs as root. Fails the
# Packer build on the first problem.
set -euo pipefail

fail() { echo "check-image: $*" >&2; exit 1; }

install -d -m 755 /run/sshd
eff=$(sshd -T)
grep -qx 'passwordauthentication no' <<<"$eff" || fail "sshd allows passwords"
grep -qx 'kbdinteractiveauthentication no' <<<"$eff" || fail "sshd allows keyboard-interactive"
grep -qx 'permitrootlogin no' <<<"$eff" || fail "sshd allows root login"
passwd -S root | awk '{print $2}' | grep -qx L || fail "root password is not locked"

systemctl is-enabled barch-firstboot.service >/dev/null || fail "barch-firstboot is not enabled"
systemctl is-enabled barchd.service >/dev/null || fail "barchd is not enabled"
[ -e /var/lib/barch-firstboot/done ] && fail "first boot already ran in the image"
[ -e /root/barch-credentials ] && fail "credentials file is in the image"
[ -z "$(ls -A /var/lib/barch)" ] || fail "/var/lib/barch is not empty"

# CDC: installed, off until the buyer sets up Postgres, and holding no secret.
for u in barch-cdc.service debezium-server.service; do
    systemctl is-enabled "$u" >/dev/null 2>&1 && fail "$u is enabled"
done
[ -e /etc/barch/cdc-password ] && fail "cdc password is in the image"
grep -q '^DEBEZIUM_SOURCE_DATABASE_PASSWORD=$' /etc/barch/debezium.env || fail "debezium.env holds a password"
[ "$(stat -c %a /etc/barch/debezium.env)" = 600 ] || fail "debezium.env is not mode 600"
[ -z "$(ls -A /var/lib/barch-cdc)" ] || fail "/var/lib/barch-cdc is not empty"
[ -e /opt/debezium-server/conf/application.properties ] || fail "Debezium has no settings"
java -version 2>&1 | grep -q 'version "21' || fail "no Java 21"

# The Debezium sink: git for barchd's git sync, and the package as a clean
# repository barch can read.
command -v git >/dev/null || fail "no git"
sink=/opt/barch/debezium-sink
[ "$(stat -c %U "$sink")" = barch ] || fail "$sink is not owned by barch"
[ "$(runuser -u barch -- git -C "$sink" rev-parse --abbrev-ref HEAD)" = main ] || fail "$sink is not on main"
[ -z "$(runuser -u barch -- git -C "$sink" status --porcelain)" ] || fail "$sink has uncommitted changes"
[ -f /opt/barch/share/debezium-queue.luau ] || fail "no queue declaration"
[ -x /opt/barch/libexec/debezium-run.sh ] || fail "no debezium-run.sh"
bx=/opt/barch/barchex
[ "$(stat -c %U "$bx")" = barch ] || fail "$bx is not owned by barch"
[ "$(runuser -u barch -- git -C "$bx" rev-parse --abbrev-ref HEAD)" = main ] || fail "$bx is not on main"
[ -f "$bx/package.luau" ] || fail "$bx has no package.luau"

# Run the real first-boot script on scratch paths, so the image is known to make
# working credentials. Everything it writes is removed again.
scratch=$(mktemp -d)
# barchd runs as barch, which has to reach the data directory inside it
chmod 755 "$scratch"
install -d -o barch -g barch "$scratch/data"
DATA_DIR="$scratch/data" PORT=14999 CRED_FILE="$scratch/cred" MARKER="$scratch/done" \
    CDC_PASS_FILE="$scratch/cdc-password" /opt/barch/libexec/barch-firstboot.sh
grep -q '^BARCH_PASSWORD=[A-Za-z0-9]\{32\}$' "$scratch/cred" || fail "no password written"
grep -qx '[A-Za-z0-9]\{32\}' "$scratch/cdc-password" || fail "no cdc password written"
[ "$(stat -c %U "$scratch/cdc-password")" = barch-cdc ] || fail "cdc password is not owned by barch-cdc"
rm -rf "$scratch"
echo "check-image: ok"
