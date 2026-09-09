-- POST /api/send-email — the shape of a transactional-email API, with no
-- mail sent. Registration and password reset are the callers that would
-- want this in a real deployment (a welcome mail, a reset link); wiring an
-- actual provider (SMTP, SES, Postgrid, ...) is out of scope here, so this
-- validates the request and answers as if it were queued, and writes
-- nothing anyone could mistake for an outbox.

function call()
    return "sendemail"
end

local function send(res, body, code)
    res.code = code or 200
    res:header("Content-Type", "application/json")
    res:header("Access-Control-Allow-Origin", "*")
    res.body = body
end

local function fail(res, why, code)
    send(res, simdjson.encode({ ok = false, error = why }), code or 400)
end

-- POST /api/send-email {to, subject, body}
function post(req, res)
    if req.body == nil or req.body == "" then return fail(res, "a message is required") end
    local ok, msg = pcall(simdjson.parse, req.body)
    if not ok then return fail(res, "bad JSON") end
    if type(msg.to) ~= "string" or msg.to == "" then return fail(res, "'to' is required") end
    if type(msg.subject) ~= "string" or msg.subject == "" then return fail(res, "'subject' is required") end

    -- a real implementation would hand {to, subject, body} to a provider here
    -- and probably return 202 once it is accepted rather than sent
    send(res, simdjson.encode({
        ok = true,
        queued = true,
        note = "stub: no email provider is wired up, nothing was actually sent",
        to = msg.to,
        subject = msg.subject,
    }))
end

function transport()
    return {
        kind = "resource",
        route = "/api/send-email",
        methods = { POST = post },
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
