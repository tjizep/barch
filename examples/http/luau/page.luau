-- GET /page — a tiny HTML page, so the same server is REST and HTML.

function call()
    return "page"
end

function getpage(req, res)
    res.body = "<html><body><h1>barch</h1><p>hello from " .. req.url .. "</p></body></html>"
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/page",
        methods = {GET = getpage},
        send = "text/html",
        cors = "*",
    }
end
