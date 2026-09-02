-- GET /hits-str — the same counter as hits.luau, via tonumber/tostring.
-- Kept so the increment bench has both sides of the comparison in one place.

function call()
    return "hits-str"
end

function hits(req, res)
    local n
    barch.store.locked("hits-str", function()
        n = tonumber(barch.store.get("hits-str")) or 0
        n = n + 1
        barch.store.set("hits-str", tostring(n))
    end)
    res.body = simdjson.encode({n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/hits-str",
        methods = {GET = hits},
        send = "application/json",
        cors = "*",
    }
end
