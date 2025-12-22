#!/bin/bash
# generate_badge.sh
LCOV_FILE=$1
OUTPUT_FILE=$2

# 1. Extract percentage
PERCENT=$(awk -F: '/LF:/ {found += $2} /LH:/ {hit += $2} END {printf "%.0f", (found > 0 ? (hit/found)*100 : 0)}' "$LCOV_FILE")

# 2. Determine color (Green > 80, Yellow otherwise)
[ "$PERCENT" -ge 80 ] && COLOR="#4c1" || COLOR="#dfb317"

# 3. Generate SVG
cat <<EOF > "$OUTPUT_FILE"
<svg xmlns="www.w3.org" width="104" height="20">
  <linearGradient id="b" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient>
  <mask id="a"><rect width="104" height="20" rx="3" fill="#fff"/></mask>
  <g mask="url(#a)"><path fill="#555" d="M0 0h63v20H0z"/><path fill="$COLOR" d="M63 0h41v20H63z"/><path fill="url(#b)" d="M0 0h104v20H0z"/></g>
  <g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11">
    <text x="31.5" y="14">coverage</text>
    <text x="82.5" y="14">$PERCENT%</text>
  </g>
</svg>
EOF
