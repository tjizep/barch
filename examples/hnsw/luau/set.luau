local g = require("graph")

-- insert. SET is a builtin, so HNSW:SET is ordinary SET in HNSW;
-- HNSW.SET is this function, against the current space.
function call(...)
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
