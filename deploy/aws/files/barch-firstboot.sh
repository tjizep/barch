#!/bin/bash
# Makes this instance's barchd credentials. Runs once, as root, before barchd.service.
#
# A fresh barchd lets any client in as the `default` user with every right. This
# script starts barchd on 127.0.0.1 only, adds an `admin` user with a random
# password, takes the default user's rights away, saves, and stops. Only then does
# barchd.service start on the public address. The password is written to a file
# only root can read. Nothing secret is in the image: the password is made here.
#
# It also makes the `cdc` user that Debezium Server (or barch-cdc) logs in
# with, and writes its password to a file only barch-cdc can read. Then it
# loads the debezium-sink package (CDCAPPLY) and barchex through barchd's git
# sync. barchex gives barchd XADD, S3 backup and vector search. Last it
# declares the queue and has XADD forward Debezium's streams to it.
#
# The paths can be changed through the environment, which is how the test in
# deploy/aws/test runs it as an ordinary user.
set -euo pipefail

BARCHD=${BARCHD:-/opt/barch/bin/barchd}
DATA_DIR=${DATA_DIR:-/var/lib/barch}
PORT=${PORT:-14000}
CRED_FILE=${CRED_FILE:-/root/barch-credentials}
MARKER=${MARKER:-/var/lib/barch-firstboot/done}
RUN_AS=${RUN_AS:-barch}
REDIS_CLI=${REDIS_CLI:-redis-cli}
ADMIN_USER=${ADMIN_USER:-admin}
# Debezium logs in as this user. +connection is for the PING Debezium sends
# before each batch. +function is for XADD: a stored function's command needs
# it. Together they also allow ECHO, CLIENT, HELLO, PUBLISH, the route
# commands, and running and changing functions. They don't allow ACL, CONFIG,
# SAVE or FLUSHALL.
CDC_USER=${CDC_USER:-cdc}
CDC_RIGHTS=${CDC_RIGHTS:-+read +write +keys +connection +function}
CDC_PASS_FILE=${CDC_PASS_FILE:-/etc/barch/cdc-password}
CDC_OWNER=${CDC_OWNER:-barch-cdc}
# The package with CDCAPPLY, a git repository, and the queue that feeds it. The queue's dir is set to $DATA_DIR/queues below.
SINK_REPO=${SINK_REPO:-/opt/barch/debezium-sink}
QUEUE_DECL=${QUEUE_DECL:-/opt/barch/share/debezium-queue.luau}
# Debezium names each stream <topic prefix>.<schema>.<table>. The prefix is
# debezium.source.topic.prefix in debezium.properties.
CDC_STREAMS=${CDC_STREAMS:-barch.*}
# barchex, cloned at build time at a pinned commit. A local path, because this
# runs before the network is up and the image must work without internet.
BARCHEX_REPO=${BARCHEX_REPO:-/opt/barch/barchex}

if [ -e "$MARKER" ]; then
    echo "barch-firstboot: already done ($MARKER)"
    exit 0
fi

# A run that stopped part way may have locked the store already. Its password is
# in the credentials file, which is written before the store is touched, so a
# second run logs in with it.
# 32 letters and digits, about 190 bits. No symbols, so no quoting trouble in
# redis-cli or in client connection strings. head closes the pipe early and tr
# dies of SIGPIPE, so pipefail is off in here.
new_password() { (set +o pipefail; tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32); }

password=
if [ -f "$CRED_FILE" ]; then
    password=$(sed -n 's/^BARCH_PASSWORD=//p' "$CRED_FILE")
fi
[ -n "$password" ] || password=$(new_password)
[ ${#password} -eq 32 ] || { echo "barch-firstboot: no password" >&2; exit 1; }

cdc_password=
if [ -f "$CDC_PASS_FILE" ]; then
    cdc_password=$(cat "$CDC_PASS_FILE")
fi
[ -n "$cdc_password" ] || cdc_password=$(new_password)
[ ${#cdc_password} -eq 32 ] || { echo "barch-firstboot: no cdc password" >&2; exit 1; }

(
    umask 077
    cat >"$CRED_FILE" <<EOF
# barchd credentials for this instance, made on first boot.
# Change the password after the first login:
#   ACL SETUSER $ADMIN_USER on >NEW-PASSWORD +all
# then SAVE.
BARCH_USER=$ADMIN_USER
BARCH_PASSWORD=$password
BARCH_PORT=$PORT
EOF
)
chmod 600 "$CRED_FILE"
(
    umask 077
    printf '%s\n' "$cdc_password" >"$CDC_PASS_FILE"
)
if [ -n "$CDC_OWNER" ] && [ "$(id -u)" -eq 0 ]; then
    chown "$CDC_OWNER:$CDC_OWNER" "$CDC_PASS_FILE"
fi
chmod 600 "$CDC_PASS_FILE"

# setpriv, not runuser. runuser stays as barchd's parent, so $pid would be
# runuser, and runuser SIGKILLs barchd 2 s after passing SIGTERM on. setpriv
# and env exec, so $pid is barchd. env sets what runuser set; git sync runs
# git as this user and git reads HOME.
as_user=()
if [ -n "$RUN_AS" ] && [ "$(id -u)" -eq 0 ]; then
    run_home=$(getent passwd "$RUN_AS" | cut -d: -f6)
    as_user=(setpriv --reuid "$RUN_AS" --regid "$RUN_AS" --init-groups
        env HOME="$run_home" USER="$RUN_AS" LOGNAME="$RUN_AS" SHELL=/bin/sh)
fi

"${as_user[@]}" "$BARCHD" --dir "$DATA_DIR" --bind 127.0.0.1 --port "$PORT" &
pid=$!
stop_barchd() {
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid"
        wait "$pid" || true
    fi
}
trap stop_barchd EXIT

cli() { "$REDIS_CLI" -h 127.0.0.1 -p "$PORT" "$@"; }

up=0
for _ in $(seq 1 120); do
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "barch-firstboot: barchd exited during start" >&2
        exit 1
    fi
    # PONG on a new store, "not authorized" on one a run before this locked
    reply=$(cli PING 2>/dev/null || true)
    if [ -n "$reply" ]; then
        up=1
        break
    fi
    sleep 0.5
done
[ $up -eq 1 ] || { echo "barch-firstboot: barchd did not answer on $PORT" >&2; exit 1; }

# The commands go in on stdin, so the password is never on a command line that
# ps can show. The admin user comes first and logs in, because the default
# user's rights are taken away at the end. A locked store needs the login
# first.
login=
expect=$(printf 'OK\nOK\nOK\nOK\nOK\nOK\nOK\nOK\nOK\nOK\nOK')
if [ "$reply" != "PONG" ]; then
    login="AUTH $ADMIN_USER $password"
    expect=$(printf 'OK\n%s' "$expect")
fi
out=$(cli <<EOF
$login
ACL SETUSER $ADMIN_USER on >$password +all
AUTH $ADMIN_USER $password
ACL SETUSER $CDC_USER on >$cdc_password $CDC_RIGHTS
configuration:SET git/repositories/debezium/url $SINK_REPO
configuration:SET git/repositories/debezium/user $ADMIN_USER
configuration:SET git/repositories/debezium/pull on
FUNCTIONS SYNC debezium
configuration:SET git/repositories/barchex/url $BARCHEX_REPO
configuration:SET git/repositories/barchex/user $ADMIN_USER
configuration:SET git/repositories/barchex/pull on
FUNCTIONS SYNC barchex
EOF
)
[ "$out" = "$expect" ] || { printf "barch-firstboot: setup failed: %q\n" "$out" >&2; exit 1; }

# The declaration is many lines, so it goes in as one argument (-x). The
# password is in the environment, not on the command line. Its dir is set here
# because CONFIG SET queue_dir is lost on a restart, and the declaration isn't.
out=$(sed "s#/var/lib/barch/queues#$DATA_DIR/queues#" "$QUEUE_DECL" |
    REDISCLI_AUTH=$password cli --user "$ADMIN_USER" -x configuration:SETF queues/debezium)
[ "$out" = OK ] || { printf "barch-firstboot: queue not declared: %q\n" "$out" >&2; exit 1; }

# XADD refuses an entry its queue can't take, so the forward comes after the
# queue. NOSTORE: the queue is the only reader, so the streams keep nothing and
# don't grow. 1 when added or changed, 0 when a run before this added it.
out=$(printf 'AUTH %s %s\nXQUEUE ADD %s debezium NOSTORE\n' "$ADMIN_USER" "$password" "$CDC_STREAMS" | cli)
[ "$out" = "$(printf 'OK\n1')" ] || [ "$out" = "$(printf 'OK\n0')" ] || {
    printf "barch-firstboot: no stream forward: %q\n" "$out" >&2
    exit 1
}

out=$(cli <<EOF
AUTH $ADMIN_USER $password
ACL SETUSER default on -all +auth
SAVE
EOF
)
[ "$out" = "$(printf 'OK\nOK\nOK')" ] || { printf "barch-firstboot: lock failed: %q\n" "$out" >&2; exit 1; }

# Check the result on fresh connections before saying it worked.
if [ "$(cli SET barch-firstboot-probe 1 2>&1)" = "OK" ]; then
    echo "barch-firstboot: the default user can still write" >&2
    exit 1
fi
probe=$(printf 'AUTH %s %s\nPING\n' "$ADMIN_USER" "$password" | cli)
[ "$probe" = "$(printf 'OK\nPONG')" ] || { echo "barch-firstboot: admin login failed" >&2; exit 1; }
probe=$(printf 'AUTH %s %s\nFUNCTIONS COMMANDS\nQUEUE STATUS\n' "$ADMIN_USER" "$password" | cli)
grep -q '^XADD ' <<<"$probe" || { printf "barch-firstboot: no XADD: %q\n" "$probe" >&2; exit 1; }
grep -q 'declared=debezium .*call=CDCAPPLY' <<<"$probe" || {
    printf "barch-firstboot: no debezium queue: %q\n" "$probe" >&2
    exit 1
}
if grep -q 'nowhere to write' <<<"$probe"; then
    printf "barch-firstboot: the queue has no dir: %q\n" "$probe" >&2
    exit 1
fi
probe=$(printf 'AUTH %s %s\nXQUEUE LIST\n' "$ADMIN_USER" "$password" | cli)
[ "$probe" = "$(printf 'OK\n%s\ndebezium\nNOSTORE' "$CDC_STREAMS")" ] || {
    printf "barch-firstboot: the stream forward is not NOSTORE: %q\n" "$probe" >&2
    exit 1
}
# barchex: the SigV4 self-test, and VECTORS in its own space.
probe=$(printf 'AUTH %s %s\ns3.SIGV4\nUSE vectors\nFUNCTIONS COMMANDS\n' "$ADMIN_USER" "$password" | cli)
grep -qx ok <<<"$probe" && grep -q '^CLOSEST VECTORS' <<<"$probe" || {
    printf "barch-firstboot: barchex did not load: %q\n" "$probe" >&2
    exit 1
}
probe=$(printf 'AUTH %s %s\nPING\nEXISTS barch-firstboot-probe\nFLUSHALL\n' "$CDC_USER" "$cdc_password" | cli)
[ "$probe" = "$(printf 'OK\nPONG\n0\nnot authorized')" ] || {
    printf "barch-firstboot: cdc user check failed: %q\n" "$probe" >&2
    exit 1
}

stop_barchd
trap - EXIT

mkdir -p "$(dirname "$MARKER")"
date -u +%Y-%m-%dT%H:%M:%SZ >"$MARKER"
echo "barch-firstboot: credentials are in $CRED_FILE"
