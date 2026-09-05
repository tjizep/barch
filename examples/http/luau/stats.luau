-- GET /stats — STATS and OPS as JSON, always as the web user.
-- transport().user pins the route, so a login cookie does not change it.

local function kvmap(arr)
    local m = {}
    if type(arr) ~= "table" then
        return m
    end
    local i = 1
    while arr[i] ~= nil do
        m[tostring(arr[i])] = arr[i + 1]
        i = i + 2
    end
    return m
end

function call()
    return "stats"
end

function stats(req, res)
    res.body = simdjson.encode({
        user = barch.user(),
        stats = kvmap(barch.call("STATS")),
        ops = kvmap(barch.call("OPS")),
    })
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/stats",
        methods = {GET = stats},
        send = "application/json",
        cors = "*",
        user = "web",
    }
end
