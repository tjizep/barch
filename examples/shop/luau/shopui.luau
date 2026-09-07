-- The storefront itself, out of the file store. `index` is what makes /shop an
-- entry point rather than a 404.
function call()
    return "shopui"
end

function transport()
    return {
        kind = "files",
        route = "/shop/*",
        root = "/app",
        index = "index.html",
    }
end
