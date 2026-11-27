#!/bin/bash
# Installs Debezium Server, the debezium-sink package and barch-cdc, which keep
# barch current from the Postgres WAL. Runs as root, after install-barchd.sh.
# The services stay off until the buyer fills in the Postgres settings. See
# CDC.md.
set -euo pipefail

DEBEZIUM_VERSION=${DEBEZIUM_VERSION:-3.7.1.Final}
DEBEZIUM_SHA256=${DEBEZIUM_SHA256:-}
STAGE=${STAGE:-/tmp/barch-files}

export DEBIAN_FRONTEND=noninteractive

# Debezium 3 needs Java 21. python3 is part of the Ubuntu base image.
apt-get -y install --no-install-recommends openjdk-21-jre-headless python3
[ -n "$DEBEZIUM_SHA256" ] || { echo "DEBEZIUM_SHA256 is not set" >&2; exit 1; }

url=https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/$DEBEZIUM_VERSION/debezium-server-dist-$DEBEZIUM_VERSION.tar.gz
work=$(mktemp -d)
curl -fsSL -o "$work/debezium.tar.gz" "$url"
echo "$DEBEZIUM_SHA256  $work/debezium.tar.gz" | sha256sum -c -
rm -rf /opt/debezium-server
tar -xzf "$work/debezium.tar.gz" -C /opt
rm -rf "$work"
[ -x /opt/debezium-server/run.sh ] || { echo "no run.sh in the Debezium tarball" >&2; exit 1; }
chown -R root:root /opt/debezium-server
echo "debezium-server $DEBEZIUM_VERSION sha256 $DEBEZIUM_SHA256" >>/opt/barch/VERSION

useradd --system --home-dir /var/lib/barch-cdc --shell /usr/sbin/nologin barch-cdc 2>/dev/null || true
install -d -o barch-cdc -g barch-cdc -m 750 /var/lib/barch-cdc

install -m 755 "$STAGE/barch-cdc.py" /opt/barch/libexec/barch-cdc.py
install -m 755 "$STAGE/debezium-run.sh" /opt/barch/libexec/debezium-run.sh
install -d -m 755 /opt/barch/share
install -m 644 "$STAGE/debezium-queue.luau" /opt/barch/share/debezium-queue.luau

# The package barchd's git sync loads CDCAPPLY from. First boot points
# git/repositories/debezium at it. It is a git repository because the sync runs
# git; the buyer changes the mapping by committing here, or points the setting
# at a repository of their own. It belongs to barch: git refuses to read a
# repository another user owns.
sink=/opt/barch/debezium-sink
rm -rf "$sink"
cp -r "$STAGE/debezium-sink" "$sink"
git -C "$sink" init -q -b main
git -C "$sink" add -A
git -C "$sink" -c user.name=barch-ami -c user.email=barch-ami@localhost \
    commit -q -m "debezium-sink as shipped in the AMI"
chown -R barch:barch "$sink"
install -m 644 "$STAGE/cdc.conf" /etc/barch/cdc.conf
install -m 644 "$STAGE/debezium.properties" /etc/barch/debezium.properties
install -m 600 "$STAGE/debezium.env" /etc/barch/debezium.env
install -m 644 "$STAGE/CDC.md" /opt/barch/CDC.md
# Debezium Server reads conf/application.properties from its own directory.
ln -sf /etc/barch/debezium.properties /opt/debezium-server/conf/application.properties
install -m 644 "$STAGE/barch-cdc.service" /etc/systemd/system/barch-cdc.service
install -m 644 "$STAGE/debezium-server.service" /etc/systemd/system/debezium-server.service
systemctl daemon-reload
# Not enabled: there is no database to read from yet.

/usr/bin/python3 -I /opt/barch/libexec/barch-cdc.py --help >/dev/null
