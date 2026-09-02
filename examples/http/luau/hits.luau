-- GET /hits — a visit counter stored as a little-endian int32, not a decimal string.
--
-- The Redis-shaped version is session.luau: tonumber(get()) then tostring(set()).
-- That round-trip allocates two strings on every request. This writes four bytes
-- in place once the key exists, which is what setInt32At is for.

function call()
    return "hits"
end

function hits(req, res)
    local n
    barch.store.locked("hits", function()
        n = barch.store.getInt32At("hits") or 0
        n = n + 1
        barch.store.setInt32At("hits", n)
    end)
    res.body = simdjson.encode({n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/hits",
        methods = {GET = hits},
        send = "application/json",
        cors = "*",
    }
end
