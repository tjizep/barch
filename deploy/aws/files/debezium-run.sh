#!/bin/bash
# Starts Debezium Server with the cdc user's password in its environment.
# debezium.properties reads it as ${BARCH_CDC_PASSWORD}. First boot makes the
# password, so it can't be in debezium.env, which is in the image.
set -euo pipefail

CDC_PASS_FILE=${CDC_PASS_FILE:-/etc/barch/cdc-password}

BARCH_CDC_PASSWORD=$(cat "$CDC_PASS_FILE")
export BARCH_CDC_PASSWORD
cd /opt/debezium-server
exec ./run.sh
