#!/bin/bash
# Installs barchex at a pinned commit, for S3 backup and vector search. Runs as
# root, after install-barchd.sh, which installs git. First boot points
# git/repositories/barchex at this clone, because it runs before the network is
# up. origin stays GitHub, so the buyer can pull a newer barchex. See
# CDC.md and README.md.
set -euo pipefail

BARCHEX_URL=${BARCHEX_URL:-https://github.com/tjizep/barchex.git}
BARCHEX_COMMIT=${BARCHEX_COMMIT:-}
[ ${#BARCHEX_COMMIT} -eq 40 ] || { echo "BARCHEX_COMMIT must be a full hash" >&2; exit 1; }

dir=/opt/barch/barchex
rm -rf "$dir"
git clone -q --no-checkout "$BARCHEX_URL" "$dir"
# The sync checks out main, so main is the pinned commit.
git -C "$dir" checkout -q -B main "$BARCHEX_COMMIT"
# so that a plain git pull updates it
git -C "$dir" branch -q --set-upstream-to=origin/main main
[ "$(git -C "$dir" rev-parse HEAD)" = "$BARCHEX_COMMIT" ] || { echo "barchex is not at $BARCHEX_COMMIT" >&2; exit 1; }
# git refuses to read a repository another user owns.
chown -R barch:barch "$dir"
echo "barchex $BARCHEX_COMMIT" >>/opt/barch/VERSION
