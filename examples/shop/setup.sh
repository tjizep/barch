#!/bin/sh
# Set the shop up against a running barch - see README.md.
set -e

PORT="${PORT:-14000}"
HTTP_PORT="${HTTP_PORT:-18090}"
SPACE="${SPACE:-shop}"
IMAGES="${IMAGES:-shopimg}"
HERE=$(cd "$(dirname "$0")" && pwd)
CLI="redis-cli -p $PORT"

if [ ! -f "$HERE/build/index.json" ]; then
    echo "preparing the catalog"
    python3 "$HERE/prepare.py"
fi

# --- the image space is configured before anything touches it -------------
# a key space reads its configuration when it is built, so `foreign` has to be
# set before the first USE or the space comes up as an ordinary one
echo "configuring $IMAGES as a foreign space"
$CLI -3 <<EOF >/dev/null
USE configuration
SET $IMAGES.foreign_script imgfetch
SET $IMAGES.foreign luau
SET $IMAGES.missing_ttl 60000
EOF

# the fill script goes in with LOADKEYS, not SETF through a heredoc: redis-cli reads
# a heredoc a line at a time and a luau function is not one line
$CLI -3 <<EOF >/dev/null
USE $IMAGES
LOADKEYS $HERE/imgspace
EOF

# --- the catalog, as a file tree whose directories are the categories -----
echo "loading the catalog into $SPACE"
# index.json and names.json go in as files with everything else: a 400KB value
# does not want to travel as a shell argument
$CLI -3 <<EOF
USE $SPACE
LOADFS $HERE/build/catalog /catalog
LOADFS $HERE/build/meta /meta
LOADFS $HERE/modules /modules
LOADFS $HERE/app /app
EOF

# The routes call a stored function (SEARCHCAT) and write an order, so the user the
# handlers run as needs `function` and `data` on top of what the built-in `web` has.
# Naming a user `web` here replaces that default for this server.
echo "granting the web user what the routes need"
$CLI ACL SETUSER web on +read +write +data +keys +function >/dev/null

echo "loading the functions"
$CLI -3 <<EOF >/dev/null
USE $SPACE
LOADKEYS $HERE/luau RELOAD
EOF

echo "starting the http server"
$CLI -3 <<EOF
USE $SPACE
HTTP START CONF $HTTP_PORT 127.0.0.1
EOF

echo
echo "open http://127.0.0.1:$HTTP_PORT/shop"
