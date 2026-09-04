-- GET /stats — the STATS and OPS counters, as JSON.
-- stats() takes a shared latch on every shard, the same as the STATS command.
-- HTTP handlers do not get barch.call; Crow is unauthenticated.

function call()
    return "stats"
end

function stats(req, res)
    res.body = simdjson.encode({
        stats = barch.stats(),
        ops = barch.ops(),
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
    }
end
