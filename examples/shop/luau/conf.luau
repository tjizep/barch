-- The http key: which routes this space serves, and who they run as.
function call()
    return "shop conf"
end

function transport()
    return {
        kind = "http",
        bind = "127.0.0.1",
        user = "web",
        keys = {"SHOPUI", "SHOPAPI", "SHOPIMG"},
    }
end
