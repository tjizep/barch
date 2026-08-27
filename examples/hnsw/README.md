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
python3 deploy.py --start --demo
```

`--start` boots an embedded server on 14000. `--demo` inserts a few
dozen similar words and asks for nearest neighbours of some typos.

Without `--start`, the same script talks to whatever is already on
`--port`.

## Commands

Once loaded, the functions are commands. The colon form runs in the
`HNSW` space, which is where the graph lives (one shard):

```
HNSW:PUT hello
HNSW:PUT hallo
HNSW:CLOSEST helo
HNSW:CLOSEST hello 3
HNSW:TUNE
```

`ADD` is already a barch command, so insert is `PUT`. The dotted form
`HNSW.PUT` is the same function, but it writes the graph in *the
current space*. That is the usual dotted-name rule: definition comes
from `HNSW`, data from wherever the call ran.

`CLOSEST` with one argument answers the nearest word. A trailing
integer is how many to return, as a flat list of word, distance, …

## Speed vs recall

`HNSW:TUNE` with no arguments reports `count, M, efConstruction,
efSearch, heuristic, entry`.

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
- `luau/put.luau` / `closest.luau` / `tune.luau` — the commands
- `deploy.py` — SETF in order (`graph` first, because the others require it)
