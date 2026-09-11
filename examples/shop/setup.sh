#!/bin/sh
# Set the shop up against a running barch - see README.md.
set -e

PORT="${PORT:-14000}"
HTTP_PORT="${HTTP_PORT:-18090}"
SPACE="${SPACE:-shop}"
HERE=$(cd "$(dirname "$0")" && pwd)
CLI="redis-cli -p $PORT"

if [ ! -f "$HERE/build/index.json" ]; then
    echo "preparing the catalog"
    python3 "$HERE/prepare.py"
fi

# --- the space fetches an image it does not have, once -------------------
# `fs_source` names a stored function that produces a file by path; the route that
# serves them opts in with `source = true`. A missing one is remembered for
# missing_ttl so a 404 is not a round trip every time
echo "configuring the file source"
$CLI -3 <<EOF >/dev/null
USE configuration
SET geo.ordered 1
SET $SPACE.fs_source imgsource
SET $SPACE.fs_source_list imglist
SET $SPACE.missing_ttl 60000
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

# --- accounts and ratings live in key spaces of their own -------------------
# The code goes with the data: `users/modules` and `ratings/modules` are loaded
# into those spaces' file stores and shopapi.luau reaches them with
# `require("users:/modules/accounts.luau")`. Both the require and the
# `barch.space.NAME` the modules use look a space up rather than creating one,
# so each has to exist before the first request does. USE is what brings a key
# space into being.
echo "creating the users, ratings, geo and orders spaces"
$CLI -3 <<EOF >/dev/null
USE users
LOADFS $HERE/users/modules /modules
USE ratings
LOADFS $HERE/ratings/modules /modules
USE geo
LOADFS $HERE/geo/modules /modules
USE orders
LOADFS $HERE/orders/modules /modules
EOF

# The address step looks streets and suburbs up out of `geo`. The code is loaded
# above with everything else; the data is not shipped - `geo.py` pulls it from
# Overture Maps, and the checkout says so plainly when the space is empty rather
# than offering a picker that finds nothing.

# The routes call a stored function (SEARCHCAT) and write an order, so the user the
# handlers run as needs `function` and `data` on top of what the built-in `web` has.
# Naming a user `web` here replaces that default for this server.
#
# `config` is here for the space viewer's global settings page and it is the one
# grant worth thinking about before copying: CONFIG is a single command carrying
# `read`, `write` and `config` together, so there is no way to let a route read
# the settings without also letting one change them. Drop `+config` if you do not
# want that - the viewer then says the grant is missing rather than breaking.
echo "granting the web user what the routes need"
$CLI ACL SETUSER web on +read +write +data +keys +function +config >/dev/null

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
