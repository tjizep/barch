-- The HNSW commands, all from one key.
--
-- transport() with kind = "resp" lets this single function key answer to
-- several RESP command names, each with its own rights. That matters here
-- because the four are not the same kind of thing: SET and TUNE write keys,
-- CLOSEST and PARAMS only read them. As one command per key they all needed
-- whatever calling any stored function needs, so a search cost the same
-- rights as a rebuild.
--
-- Declaring write + data also means the two writing ones are sent on to a
-- replication destination like a builtin write is. A stored function had no
-- way to say that before, so nothing it did was ever replicated.
--
-- The graph itself is in graph.luau, which has to be stored first.

local g = require("graph")

-- Insert. This does not overload the builtin SET: HNSW:SET a b is still an
-- ordinary SET in HNSW, and HNSW.SET a builds the graph in whatever space
-- USE selected.
function cmd_set(...)
    local parts = {...}
    if #parts == 0 then
        return {err = "HNSW.SET needs a word or phrase"}
    end
    local word = table.concat(parts, " ")
    local id, added
    barch.store.locked("hnsw.count", function()
        id, added = g.insert(word)
    end)
    if added then
        return {"OK", id}
    end
    return {"exists", id}
end

-- CLOSEST <word...> [k]. One argument answers the nearest word; a trailing
-- integer asks for that many, as a flat list of word, distance, ...
function cmd_closest(...)
    local parts = {...}
    if #parts == 0 then
        return {err = "HNSW.CLOSEST needs a word or phrase"}
    end
    local k = 1
    local last = tonumber(parts[#parts])
    local phrase
    if last ~= nil and last >= 1 and #parts > 1 then
        k = math.floor(last)
        phrase = table.concat(parts, " ", 1, #parts - 1)
    else
        phrase = table.concat(parts, " ")
    end
    local hits = g.closest(phrase, k)
    if #hits == 0 then
        return nil
    end
    if k == 1 then
        return hits[1][1]
    end
    local out = {}
    for i = 1, #hits do
        out[#out + 1] = hits[i][1]
        out[#out + 1] = hits[i][2]
    end
    return out
end

-- TUNE fast | default | accurate
-- TUNE <M> <efConstruction> <efSearch> [0|1]
-- 0 = pick the M nearest (faster build)
-- 1 = heuristic, keeps a more diverse neighbourhood (slower, usually better recall)
--
-- Writes the parameter keys, so it wants @write @data. Reading them back is
-- PARAMS, which does not.
function cmd_tune(...)
    local a = {...}
    if #a == 0 then
        return g.stats()
    end
    local p = string.lower(a[1])
    if p == "fast" or p == "default" or p == "accurate" then
        g.apply_tune(p)
        return g.stats()
    end
    local m = tonumber(a[1])
    local efc = tonumber(a[2])
    local efs = tonumber(a[3])
    local heur = tonumber(a[4])
    if m == nil then
        return {err = "HNSW.TUNE fast|default|accurate or M efConstruction efSearch [heuristic]"}
    end
    g.apply_tune(nil, m, efc, efs, heur)
    return g.stats()
end

-- count, M, efConstruction, efSearch, heuristic, entry
function cmd_params()
    return g.stats()
end

-- the key's own name, HNSW.HNSW, says what the key answers to. FUNCTIONS
-- COMMANDS gives the same names with the categories attached.
function call()
    return {
        "SET <word...>",
        "CLOSEST <word...> [k]",
        "TUNE [fast|default|accurate | M efConstruction efSearch [heuristic]]",
        "PARAMS",
    }
end

function transport()
    return {
        kind = "resp",
        methods = {
            SET = cmd_set,
            CLOSEST = cmd_closest,
            TUNE = cmd_tune,
            PARAMS = cmd_params,
        },
        categories = {
            SET = {"write", "data"},
            CLOSEST = {"read"},
            TUNE = {"write", "data"},
            PARAMS = {"read"},
        },
        -- -1 is "at least one", 0 is "whatever it is given"
        arity = {SET = -1, CLOSEST = -1, TUNE = 0, PARAMS = 0},
    }
end
