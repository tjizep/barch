-- GET /health — liveness. ops() is atomics, no shard walk.

function call()
    return "health"
end

function health(req, res)
    local ops = barch.ops()
    res.body = simdjson.encode({
        ok = true,
        space = barch.running(),
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
