-- POST /login — bind this browser's sid cookie to a user from the auth shard.
-- The cookie is an opaque token; the user name lives at http:sess:<sid>.

function call()
    return "login"
end

function login(req, res)
    local j = simdjson.parse(req.body)
    local user = j.user
    local pass = j.pass
    if type(user) ~= "string" or type(pass) ~= "string" then
        res.body = simdjson.encode({ok = false, error = "user and pass required"})
        res.code = 400
        return
    end
    if barch.auth(user, pass) then
        res.body = simdjson.encode({ok = true, user = barch.user()})
        res.code = 200
    else
        res.body = simdjson.encode({ok = false})
        res.code = 401
    end
end

function transport()
    return {
        kind = "resource",
        route = "/login",
        methods = {POST = login},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
