-- GET /health — liveness via PING and OPS, plus whoever this request is.

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
    return "health"
end

function health(req, res)
    local ops = kvmap(barch.call("OPS"))
    res.body = simdjson.encode({
        ok = barch.call("PING") == "PONG",
        space = barch.running(),
        user = barch.user(),
        retrieve_ops = ops.retrieve_ops,
    })
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/health",
        methods = {GET = health},
        send = "application/json",
        cors = "*",
    }
end
