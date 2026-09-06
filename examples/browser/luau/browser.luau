-- The entry point: /browser serves the little Vue application out of the file
-- store, under /app.
--
-- kind = "files" means C++ answers it and no luau runs per request, which matters
-- here because the page pulls three files at once and a luau handler would hold a
-- VM slot for each of them.
--
-- `index` is what makes /browser work rather than only /browser/index.html: a url
-- naming a directory gets the index appended. See TODO 253.

function call()
    return "browser"
end

function transport()
    return {
        kind = "files",
        route = "/browser/*",
        root = "/app",
        index = "index.html",
    }
end
