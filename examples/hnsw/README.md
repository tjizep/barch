# HNSW over Levenshtein, as stored Luau functions

A tiny hierarchical navigable small-world index, kept in the `HNSW` key
space. Distance is Levenshtein, so the keys are words and short phrases
rather than vectors.

The walk uses `barch.art()` as the candidate queue (min) and the found
set (max to drop the furthest). The graph itself is ordinary keys.

## Load

From a RelWithDebInfo (or Release) build, with redis-py installed:

```
cd examples/hnsw
python3 -m venv ./venv
./venv/bin/pip install ../../cmake-build-release/
source ./venv/bin/activate
pip install redis
python3 deploy.py --start --demo
```

`--start` boots an embedded server on 14000. `--demo` inserts a few
dozen similar words and asks for nearest neighbours of some typos.

Without `--start`, the same script talks to whatever is already on
`--port`.

## Commands

Once loaded, the functions are commands. A colon is the builtin in
that space (`HNSW:SET a b` is ordinary SET). A dot is the stored
function, running against the current space:

```
USE HNSW
HNSW.SET hello
HNSW.SET hallo
HNSW.CLOSEST helo
HNSW.CLOSEST hello 3
HNSW.TUNE
HNSW.PARAMS
```

All four come from the one `HNSW` key. It has a `transport()` of kind
`"resp"`, which names the commands it answers to and says what each one
needs:

```lua
function transport()
    return {
        kind = "resp",
        methods = {SET = cmd_set, CLOSEST = cmd_closest, ...},
        categories = {SET = {"write", "data"}, CLOSEST = {"read"}, ...},
        arity = {SET = -1, CLOSEST = -1, TUNE = 0, PARAMS = 0},
    }
end
```

`FUNCTIONS COMMANDS` lists them with the key they came from and the
categories they declared. `HNSW.HNSW` - the key's own name - prints the
same four with their arguments.

Insert is `SET`. It does not overload SET: `HNSW:SET "alpha" "beta"`
still writes the key `alpha` in `HNSW`, while `HNSW.SET "alpha"`
builds the graph in whatever space `USE` selected. `CLOSEST` and `TUNE`
are not builtins, so `HNSW:CLOSEST` and `HNSW.CLOSEST` both run the
function; the colon one searches `HNSW`, the dotted one searches here.
`PARAMS` is the read-only half of `TUNE`, and is spelt that way because
`STATS` is a builtin and would never reach the function.

`CLOSEST` with one argument answers the nearest word. A trailing
integer is how many to return, as a flat list of word, distance, ...

## What the categories buy

Before this the index was four keys - `graph`, `set`, `closest`, `tune`
- and every one of them was authorized against the single category set
that calling any stored function needs. Searching cost the same rights
as rebuilding.

Now `SET` and `TUNE` declare `@write @data` while `CLOSEST` and
`PARAMS` declare `@read`, so:

```
ACL SETUSER search on >secret +read +keys +function +connection
```

can `HNSW:CLOSEST` and `HNSW:PARAMS` and is refused `HNSW.SET` and
`HNSW:TUNE`. Reading the graph from inside the script wants `+keys`
as well as `+read` - that is the store gate, one layer below the
command, and it is checked whatever the command declared.

Declaring `@write @data` also means the two writing commands are sent
on to a replication destination the way a builtin write is. A stored
function had no way to say that before, so nothing the index did was
ever replicated.

## Speed vs recall

`HNSW:PARAMS` (or `HNSW:TUNE` with no arguments) reports `count, M,
efConstruction, efSearch, heuristic, entry`.

| preset    | M  | efConstruction | efSearch | heuristic |
|-----------|----|----------------|----------|-----------|
| fast      | 8  | 16             | 8        | off       |
| default   | 16 | 64             | 32       | off       |
| accurate  | 32 | 200            | 100      | on        |

Or set the numbers yourself:

```
HNSW:TUNE 16 64 32 0
```

`M` is the neighbour cap per node (twice that on layer 0).
`efConstruction` is the beam during insert — higher is slower and
usually more accurate. `efSearch` is the same thing at query time, and
is cheap to raise later. Heuristic `1` keeps a more diverse
neighbourhood instead of the M nearest; it costs extra Levenshtein
calls on insert.

Tune *before* loading a large vocabulary. Changing M does not rebuild
edges that are already there.

The space is created with a 20M instruction slice and a 30s deadline so
a larger insert is not cut off the way a tight one-liner budget would.
A graph of tens of thousands of long phrases will still want those
raised.

## Files

- `luau/graph.luau` — distance, ART queues, insert and search
- `luau/hnsw.luau` — the four commands and the `transport()` that names them
- `deploy.py` — SETF in order (`graph` first, because `hnsw` requires it)
