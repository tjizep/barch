-- Cookie + barch.store, the usual Redis session pattern.
-- GET /sess sets sid if missing and bumps a visit count at sess:<sid>.

function call()
    return "sess"
end

local function newsid()
    local t = {}
    for i = 1, 16 do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return table.concat(t)
end

function hits(req, res)
    local id = req:cookie("sid")
    if not id or id == "" then
        id = newsid()
        res:cookie("sid", id, {path = "/", httponly = true})
    end
    local key = "sess:" .. id
    local n
    barch.store.locked(key, function()
        n = tonumber(barch.store.get(key)) or 0
        n = n + 1
        barch.store.set(key, tostring(n))
    end)
    res.body = simdjson.encode({sid = id, n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/sess",
        methods = {GET = hits},
        send = "application/json",
        cors = "*",
    }
end
