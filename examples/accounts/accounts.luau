-- Accounts and sessions, in the key space that holds them.
--
-- This file lives in the `users` file store, not the shop's, and the handlers in
-- shopapi.luau reach it with `require("users:/modules/accounts.luau")`. The point
-- is that the code for a customer record sits next to the customer records: the
-- shop's catalog has been dropped and reloaded more than once and neither the
-- accounts nor the code that reads them should notice.
--
-- What does *not* move is where this runs. An HTTP route is a stored function in
-- the server's space, so a module required from here still has `barch.store` and
-- `barch.call` pointed at `shop`. Everything below therefore goes through the
-- `users` handle, which is what `space()` hands back.
--
--   user:<email>   {email, name, salt, hash}
--   sess:<sid>     the email a signed-in cookie belongs to

-- The space this module's data is in. `barch.space.users` looks a space up and
-- will not create one, so setup.sh runs a bare `USE users` first.
local function space()
    return barch.space.users
end

-- Note the space name on the front. A bare `:/modules/sha256.luau` would resolve
-- against the *running* space - shop - and not against the space this file came
-- out of; require's colon form takes the space from the string or from the call,
-- never from the requiring module.
local function sha256()
    return require("users:/modules/sha256.luau")
end

-- A random token, hex - used both for a password salt and for a session id.
-- Same shape as the newsid() in examples/http/luau/session.luau.
local function token(nbytes)
    local t = {}
    for i = 1, nbytes do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return table.concat(t)
end

-- Exported, not local: an order filed under a typed address has to land on the
-- same key the account is under, and the rule for that belongs here rather than
-- copied into whatever else needs it.
function normalize_email(email)
    return string.lower(string.match(email or "", "^%s*(.-)%s*$"))
end

-- The signed-in user, from the sid cookie: nil if there is none or it does not
-- (or no longer) map to an account. Sessions live here alongside the accounts,
-- since a session is data about a registered user.
function current(req)
    local sid = req:cookie("sid")
    if sid == nil or sid == "" then return nil end
    local email = space()["sess:" .. sid]
    if email == nil then return nil end
    local raw = space()["user:" .. email]
    if raw == nil then return nil end
    return simdjson.parse(raw)
end

local function start_session(res, email)
    local sid = token(16)
    space()["sess:" .. sid] = email
    res:cookie("sid", sid, { path = "/", httponly = true })
end

-- What the routes get back: {ok, user} on success, {error, code} on failure, so
-- the caller can turn either one into a response without knowing anything about
-- how an account is stored.
local function no(why, code)
    return { error = why, code = code or 400 }
end

local function yes(u)
    return { ok = true, user = { email = u.email, name = u.name } }
end

-- POST /api/register  {name, email, password}
function register(req, res)
    if req.body == nil or req.body == "" then return no("name, email and password are required") end
    local j = simdjson.parse(req.body)
    local email = normalize_email(j.email)
    local password = j.password
    if email == "" or type(password) ~= "string" or #password < 6 then
        return no("an email and a password of at least 6 characters are required")
    end
    local key = "user:" .. email
    if space()[key] ~= nil then return no("an account with that email already exists", 409) end

    local salt = token(8)
    local doc = {
        email = email,
        name = j.name or "",
        salt = salt,
        hash = sha256().hex(salt .. password),
    }
    space()[key] = simdjson.encode(doc)
    start_session(res, email)
    return yes(doc)
end

-- POST /api/signon  {email, password}
function signon(req, res)
    if req.body == nil or req.body == "" then return no("email and password are required") end
    local j = simdjson.parse(req.body)
    local email = normalize_email(j.email)
    local raw = space()["user:" .. email]
    if raw == nil then return no("no such account, or the password is wrong", 401) end

    local u = simdjson.parse(raw)
    if sha256().hex(u.salt .. tostring(j.password)) ~= u.hash then
        return no("no such account, or the password is wrong", 401)
    end
    start_session(res, u.email)
    return yes(u)
end

-- POST /api/signout
function signout(req, res)
    local sid = req:cookie("sid")
    if sid ~= nil and sid ~= "" then space()["sess:" .. sid] = nil end
    res:cookie("sid", "", { path = "/", max_age = 0, httponly = true })
    return { ok = true }
end
