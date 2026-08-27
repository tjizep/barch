local g = require("graph")

-- ADD is already a barch command, so the insert is PUT
function call(...)
    local parts = {...}
    if #parts == 0 then
        return {err = "HNSW.PUT needs a word or phrase"}
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
