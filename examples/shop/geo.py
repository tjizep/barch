#!/usr/bin/env python3
"""South African places, out of Overture Maps and into one barch key space.

    venv/bin/pip install overturemaps h3 redis
    python3 geo.py --port 14000

What this is for: the shop's checkout asks for a delivery address, and the
address screen looks streets and suburbs up as you type. That lookup is two
range walks over one key space.

## Why not the shape examples/flask/overture.py uses

That script loads Overture's `address` theme for Canada and spreads it over nine
key spaces - `streets`, `spatial_data`, `overflows`, `postcodes`,
`postcode_street`, `street_province`, `provinces`, `cities`, `tokey`. Two things
are different here.

**There are no South African addresses in Overture.** The `address` theme comes
from OpenAddresses and South Africa is not in it: Cape Town, Johannesburg,
Pretoria and Durban bboxes all answer zero rows. What does have coverage, both
from OpenStreetMap, is `division` - suburbs, cities, provinces - and `segment`,
which carries named roads. So this loads those two, and the checkout asks for the
house number and the postal code as text, because that part genuinely is not in
the data.

**One key space is enough now.** Nine spaces is what you do when a point lookup
and a prefix walk want different structures. With hybrid keys the ART owns the
leaves and a hash indexes them, so the same space answers `street:SOM` as an
ordered range walk and the full key as a hash hit. The trick is only that the
text you search by has to lead the key:

    street:<NAME>:<METRO>         {name, metro, lat, lon, ways}
    sub:<NAME>:<KIND>:<id>        {name, kind, province, lat, lon}
    prov:<NAME>                   {name, code}
    meta:loaded                   what this script put there, and when

Names are upper-cased and stripped of accents so a prefix compares the way
somebody types it; the JSON keeps the original spelling for display.
"""

import argparse
import json
import re
import sys
import time
import unicodedata
from collections import defaultdict

import redis
import overturemaps

# Divisions are loaded for the whole country. Roads are not: the segment theme is
# every road in the bounding box and the four metros are where the customers are,
# so this is the coverage-for-download-time trade the loader makes. --metro adds
# one, --all-roads is the national version and takes hours.
SOUTH_AFRICA = (16.45, -34.90, 32.95, -22.10)

METROS = {
    "cape-town":    (18.30, -34.15, 18.95, -33.65),
    "johannesburg": (27.75, -26.45, 28.30, -26.00),
    "pretoria":     (28.05, -25.90, 28.45, -25.60),
    "durban":       (30.75, -30.05, 31.15, -29.65),
}

# Overture division subtypes worth offering as "suburb or city". `locality` is
# the suburb-sized one and the bulk of what a South African address names;
# `county` and `region` come along so a lookup can say which municipality and
# which province a place is in.
SUB_KINDS = {"locality", "county", "localadmin", "macrohood", "neighborhood"}


def plain(s):
    """Upper case, no accents, single spaces - what a key is compared by."""
    if s is None:
        return ""
    nfd = unicodedata.normalize("NFD", str(s))
    base = nfd.encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"\s+", " ", base).strip().upper()


def usable(name):
    """Names that would only ever be noise in a picker."""
    if not name or len(name) < 2:
        return False
    # a key holding our separator would split into a composite and stop matching
    # the prefix it was written under - see the shop README on TODO 260
    if ":" in name:
        return False
    # unnamed OSM ways come through as refs like M4 or R102; those are useful,
    # but a bare number or a fragment of one is not
    return not re.fullmatch(r"[\d\W_]+", name)


def centroid(geom):
    """Overture hands geometry over as WKB; we only ever want a point."""
    try:
        from shapely import wkb
        g = wkb.loads(bytes(geom))
        c = g.centroid
        return round(c.y, 6), round(c.x, 6)
    except Exception:
        return None, None


class Writer:
    """Batched writes over RESP, so one round trip does not cost one key."""

    def __init__(self, r, space, batch=2000):
        self.r = r
        self.space = space
        self.batch = batch
        self.pipe = r.pipeline(transaction=False)
        self.n = 0
        self.pending = 0

    def set(self, key, value):
        self.pipe.execute_command("SET", key, value)
        self.n += 1
        self.pending += 1
        if self.pending >= self.batch:
            self.flush()

    def flush(self):
        if self.pending:
            self.pipe.execute()
            self.pending = 0


def read(kind, bbox, want=None, timeout=None):
    """Batches out of Overture, with a row count as it goes."""
    started = time.time()
    seen = 0
    reader = overturemaps.record_batch_reader(
        kind, bbox, connect_timeout=60, request_timeout=600)
    for batch in reader:
        seen += batch.num_rows
        yield batch
        if want and seen >= want:
            return
        if timeout and time.time() - started > timeout:
            print("    (stopping at the time budget, %d rows in)" % seen, file=sys.stderr)
            return


def load_divisions(w, bbox, timeout):
    """Suburbs, cities and provinces, nationally."""
    kept = skipped = 0
    provinces = {}
    for batch in read("division", bbox, timeout=timeout):
        d = batch.to_pydict()
        for i in range(batch.num_rows):
            if d["country"][i] != "ZA":
                skipped += 1
                continue
            subtype = d["subtype"][i]
            names = d["names"][i] or {}
            name = names.get("primary") if isinstance(names, dict) else None
            if not usable(name):
                skipped += 1
                continue
            key_name = plain(name)
            if subtype == "region":
                provinces[key_name] = name
                w.set("prov:%s" % key_name, json.dumps({"name": name}))
                kept += 1
                continue
            if subtype not in SUB_KINDS:
                skipped += 1
                continue
            lat, lon = centroid(d["geometry"][i])
            w.set("sub:%s:%s:%s" % (key_name, plain(subtype), d["id"][i][:8]),
                  json.dumps({"name": name, "kind": subtype, "lat": lat, "lon": lon}))
            kept += 1
    w.flush()
    return kept, skipped, len(provinces)


def load_roads(w, metro, bbox, timeout):
    """Named roads in one metro, folded down to one key per distinct name.

    A road is many segments and we want one entry per name, so the segments are
    collapsed here rather than written and deduplicated later: the count of ways
    goes in the record, and the centroid is the mean of what was seen.
    """
    acc = defaultdict(lambda: {"n": 0, "lat": 0.0, "lon": 0.0, "name": None})
    rows = skipped = 0
    for batch in read("segment", bbox, timeout=timeout):
        d = batch.to_pydict()
        for i in range(batch.num_rows):
            rows += 1
            if d["subtype"][i] != "road":
                skipped += 1
                continue
            names = d["names"][i] or {}
            name = names.get("primary") if isinstance(names, dict) else None
            if not usable(name):
                skipped += 1
                continue
            lat, lon = centroid(d["geometry"][i])
            if lat is None:
                skipped += 1
                continue
            a = acc[plain(name)]
            a["n"] += 1
            a["lat"] += lat
            a["lon"] += lon
            a["name"] = a["name"] or name
    for key_name, a in acc.items():
        w.set("street:%s:%s" % (key_name, plain(metro)),
              json.dumps({"name": a["name"], "metro": metro, "ways": a["n"],
                          "lat": round(a["lat"] / a["n"], 6),
                          "lon": round(a["lon"] / a["n"], 6)}))
    w.flush()
    return len(acc), rows, skipped


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=14000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--space", default="geo")
    ap.add_argument("--metro", action="append", choices=sorted(METROS),
                    help="load only these metros; repeatable, default is all four")
    ap.add_argument("--all-roads", action="store_true",
                    help="every named road in the country instead of the metros. Hours.")
    ap.add_argument("--skip-divisions", action="store_true")
    ap.add_argument("--timeout", type=float, default=0,
                    help="seconds to spend per source before moving on, 0 for no limit")
    args = ap.parse_args()

    timeout = args.timeout or None
    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    r.ping()
    # `USE` brings the space into being; ordered is what makes a prefix a range,
    # and hybrid is what keeps the exact-key read a hash hit rather than a walk
    conf = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    conf.execute_command("USE", "configuration")
    conf.execute_command("SET", "%s.ordered" % args.space, "1")
    r.execute_command("USE", args.space)

    w = Writer(r, args.space)
    started = time.time()
    totals = {}

    if not args.skip_divisions:
        print("divisions: suburbs, cities and provinces for South Africa")
        kept, skipped, provs = load_divisions(w, SOUTH_AFRICA, timeout)
        print("  %d kept, %d skipped, %d provinces" % (kept, skipped, provs))
        totals["divisions"] = kept

    boxes = ({"south-africa": SOUTH_AFRICA} if args.all_roads
             else {m: METROS[m] for m in (args.metro or sorted(METROS))})
    streets = 0
    for metro, bbox in boxes.items():
        print("roads: %s" % metro)
        names, rows, skipped = load_roads(w, metro, bbox, timeout)
        print("  %d distinct street names from %d segments (%d skipped)"
              % (names, rows, skipped))
        streets += names
    totals["streets"] = streets

    # merged, not replaced: loading a second metro with --skip-divisions is the
    # normal way to use this, and a run that then reported only its own metros
    # and no divisions would be describing the space wrongly
    was = {}
    try:
        prev = r.execute_command("GET", "meta:loaded")
        if prev:
            was = json.loads(prev)
    except Exception:
        pass
    metros = sorted(set(was.get("metros", [])) | set(boxes))
    w.set("meta:loaded", json.dumps({
        "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "Overture Maps: division + segment, OpenStreetMap derived",
        "note": "Overture's address theme has no South African coverage",
        "metros": metros,
        "divisions": totals.get("divisions", was.get("divisions", 0)),
        "streets": totals.get("streets", 0) + (was.get("streets", 0) if args.skip_divisions else 0),
    }))
    w.flush()
    print("\n%d keys into %s in %.0fs" % (w.n, args.space, time.time() - started))
    print("try: USE %s then KEYS street:LONG*" % args.space)


if __name__ == "__main__":
    main()
