local g = require("graph")

function call(...)
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
