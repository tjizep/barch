-- POST /json — parse the body and send the same document back.

function call()
    return "json"
end

function json(req, res)
    res.body = simdjson.encode(simdjson.parse(req.body))
    res.code = 200
end

function service()
    return {
        kind = "resource",
        route = "/json",
        methods = {POST = json},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
