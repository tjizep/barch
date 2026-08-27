local g = require("graph")

-- HNSW.TUNE fast | default | accurate
-- HNSW.TUNE <M> <efConstruction> <efSearch> [0|1]
-- 0 = pick the M nearest (faster build)
-- 1 = heuristic, keeps a more diverse neighbourhood (slower, usually better recall)
function call(...)
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
