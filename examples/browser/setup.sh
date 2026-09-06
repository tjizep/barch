#!/bin/sh
# Set the browser example up against a running barch - see README.md.
#
# Everything the browser loads ends up in the barch file store: the two bundles are
# fetched once, here, rather than at run time from a CDN. The repository being
# browsed is pulled by barch itself, through the git configuration.
set -e

PORT="${PORT:-14000}"
HTTP_PORT="${HTTP_PORT:-18080}"
SPACE="${SPACE:-browser}"
REPO="${REPO:-https://github.com/svar-widgets/vue-filemanager}"
HERE=$(cd "$(dirname "$0")" && pwd)
CLI="redis-cli -p $PORT"

VUE=3.5.13
FM=2.6.0

# --- the application, into app/ next to index.html ------------------------
if [ ! -f "$HERE/app/vue.js" ]; then
    echo "fetching vue $VUE and the file manager $FM"
    curl -sfL -o "$HERE/app/vue.js" \
        "https://esm.sh/vue@$VUE/es2022/vue.bundle.mjs"
    curl -sfL -o "$HERE/app/filemanager.js" \
        "https://esm.sh/@svar-ui/vue-filemanager@$FM/X-ZXZ1ZQ/es2022/vue-filemanager.bundle.mjs"
    curl -sfL -o "$HERE/app/filemanager.css" \
        "https://esm.sh/@svar-ui/vue-filemanager@$FM/dist/index.css"
fi

# --- one key space holds the lot ------------------------------------------
echo "loading the application into $SPACE:/app"
$CLI -3 <<EOF >/dev/null
USE $SPACE
LOADFS $HERE/app /app
LOADKEYS $HERE/luau
EOF

# --- and barch pulls the repository into the same space's file store ------
echo "configuring the git repository"
$CLI -3 <<EOF >/dev/null
USE configuration
SET git/repositories/browser/url $REPO
SET git/repositories/browser/space $SPACE
SET git/repositories/browser/as fs
SET git/repositories/browser/fs_root /repo
SET git/repositories/browser/pull on
SET git/repositories/browser/ms 300000
EOF

echo "cloning and importing - this is the slow part"
$CLI FUNCTIONS SYNC browser
$CLI FUNCTIONS STATUS

echo "starting the http server"
$CLI -3 <<EOF
USE $SPACE
HTTP START CONF $HTTP_PORT 127.0.0.1
EOF

echo
echo "open http://127.0.0.1:$HTTP_PORT/browser"
