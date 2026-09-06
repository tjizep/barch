-- The REST endpoints the SVAR file manager speaks, answered out of the fs store.
--
-- The contract is read out of @svar-ui/filemanager-data-provider 2.6.0 rather than
-- guessed at:
--
--     GET    files            the root
--     GET    files/<id>       one folder, the id url encoded
--     GET    info/<id>        one entry
--     POST   files/<parent>   create a file or a folder
--     POST   upload?id=<p>    multipart - refused, nothing here parses it
--     PUT    files/<id>       rename
--     PUT    files            move or copy
--     DELETE files            delete
--
-- The write half is `barch.fs` doing the work: a rename is a rewrite of the name
-- records under a path and touches no file content, which is why moving a large
-- folder is cheap. Every one of these is a single batch, so a move of five files
-- either happens or does not.

local ROOT = "/repo"          -- where the git repository was imported
local LIMIT = 5000            -- one range batch; see the paging loop in list()

function call()
    return "fmapi"
end

-- "%2Fsrc%2Fmain" -> "/src/main". Crow decodes the query but not a path segment.
local function unescape(s)
    if not s then return "" end
    return (string.gsub(s, "%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end))
end

-- one level of a folder. This was thirty lines of prefix ranging and first-segment
-- deduplication, which is exactly what barch.fs.list does in C++ now - and the
-- reason it does: writing it here was what showed the primitive was missing.
-- See TODO 254 and 256.
local function list(id)
    local entries = {}
    for _, e in ipairs(barch.fs.list(ROOT .. id, nil, LIMIT)) do
        entries[#entries + 1] = {
            id = string.sub(e.path, string.len(ROOT) + 1),
            type = e.dir and "folder" or "file",
            size = e.size or 0,
        }
    end
    return entries
end

local function send(res, body, code)
    res.code = code or 200
    res:header("Content-Type", "application/json")
    res:header("Access-Control-Allow-Origin", "*")
    res.body = body
end

local function fail(res, why, code)
    -- the provider reads .error off the body when the status is not ok
    send(res, simdjson.encode({ error = why }), code or 400)
end

local function parent_of(id)
    local at = string.find(id, "/[^/]*$")
    if not at or at == 1 then return "" end
    return string.sub(id, 1, at - 1)
end

local function name_of(id)
    return string.match(id, "[^/]+$") or ""
end

local function body_of(req)
    if req.body == nil or req.body == "" then return nil end
    return simdjson.parse(req.body)
end

-- GET files, GET files/<id>, GET info/<id>
function get(req, res)
    local tail = string.sub(req.url, string.len("/api/") + 1)
    local q = string.find(tail, "?", 1, true)
    if q then tail = string.sub(tail, 1, q - 1) end

    local what, rest = string.match(tail, "^([a-z]+)/?(.*)$")
    local id = unescape(rest)
    if id == "/" then id = "" end

    if what == "files" then
        local entries = list(id)
        -- an empty table encodes as {} and the widget wants a list
        if #entries == 0 then
            send(res, "[]")
        else
            send(res, simdjson.encode(entries))
        end
        return
    end

    if what == "info" then
        local m = barch.fs.stat(ROOT .. id)
        if not m or m.dir then
            send(res, simdjson.encode({ Type = "folder", Path = id }))
            return
        end
        send(res, simdjson.encode({
            Path = id,
            Size = m.size,
            Type = m.type ~= "" and m.type or "file",
            Chunks = m.chunks,
            Version = m.version,
        }))
        return
    end

    fail(res, "no such endpoint", 404)
end

-- POST files/<parent>  {name, type}
function post(req, res)
    local tail = string.sub(req.url, string.len("/api/") + 1)
    local what, rest = string.match(tail, "^([a-z]+)/?(.*)$")
    if what == "upload" then
        -- multipart, which nothing here parses. Refused rather than half done
        return fail(res, "upload is not implemented in this example", 405)
    end
    if what ~= "files" then return fail(res, "no such endpoint", 404) end

    local j = body_of(req)
    if not j or not j.name then return fail(res, "a name is required") end
    local parent = unescape(rest)
    if parent == "/" then parent = "" end
    local id = parent .. "/" .. j.name
    local ok, why
    if j.type == "folder" then
        ok, why = pcall(function() barch.fs.mkdir(ROOT .. id) end)
        if not ok then return fail(res, tostring(why)) end
    else
        ok, why = pcall(function() barch.fs.put(ROOT .. id, "", "") end)
        if not ok then return fail(res, tostring(why)) end
    end
    send(res, simdjson.encode({ result = { id = id } }))
end

-- PUT files/<id> {operation="rename", name}, or PUT files {operation, ids, target}
function put(req, res)
    local tail = string.sub(req.url, string.len("/api/") + 1)
    local what, rest = string.match(tail, "^([a-z]+)/?(.*)$")
    if what ~= "files" then return fail(res, "no such endpoint", 404) end
    local j = body_of(req)
    if not j then return fail(res, "a body is required") end

    if j.operation == "rename" then
        local id = unescape(rest)
        local to = parent_of(id) .. "/" .. j.name
        local ok, why = barch.fs.rename(ROOT .. id, ROOT .. to)
        if not ok then return fail(res, why) end
        return send(res, simdjson.encode({ result = { id = to } }))
    end

    if j.operation == "move" or j.operation == "copy" then
        local target = j.target or ""
        if target == "/" then target = "" end
        local moved = {}
        for _, id in ipairs(j.ids or {}) do
            local to = target .. "/" .. name_of(id)
            local ok, why
            if j.operation == "move" then
                ok, why = barch.fs.rename(ROOT .. id, ROOT .. to)
            else
                ok, why = barch.fs.copy(ROOT .. id, ROOT .. to)
            end
            if not ok then return fail(res, why) end
            moved[#moved + 1] = { id = to }
        end
        return send(res, simdjson.encode({ result = moved }))
    end

    fail(res, "unknown operation " .. tostring(j.operation))
end

-- DELETE files {ids}
function del(req, res)
    local j = body_of(req)
    if not j then return fail(res, "a body is required") end
    for _, id in ipairs(j.ids or {}) do
        local e = barch.fs.stat(ROOT .. id)
        if e and e.dir then
            barch.fs.rmdir(ROOT .. id, true)
        else
            barch.fs.remove(ROOT .. id)
        end
    end
    send(res, simdjson.encode({ result = {} }))
end

function transport()
    return {
        kind = "resource",
        route = "/api/*",
        methods = { GET = get, POST = post, PUT = put, DELETE = del },
        send = "application/json",
        cors = "*",
    }
end
