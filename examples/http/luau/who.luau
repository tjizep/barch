-- GET /who — the user this request is running as.
-- Unpinned, so a login cookie shows up here; /stats stays on web.

function call()
    return "who"
end

function who(req, res)
    res.body = simdjson.encode({user = barch.user()})
    res.code = 200
end

function service()
    return {
        kind = "resource",
        route = "/who",
        methods = {GET = who},
        send = "application/json",
        cors = "*",
    }
end
