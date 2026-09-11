-- The http key: which routes this space serves, and who they run as.
function call()
    return "shop conf"
end

function transport()
    return {
        kind = "http",
        bind = "127.0.0.1",
        user = "web",
        -- SENDEMAIL and SPACESAPI have to come before SHOPAPI: all three can
        -- answer under /api, and a route is matched in the order this list
        -- gives Crow, not by how specific its pattern is - SHOPAPI's /api/*
        -- would otherwise shadow SENDEMAIL's exact /api/send-email and
        -- SPACESAPI's /api/admin/*
        keys = {"SHOPUI", "SENDEMAIL", "SPACESAPI", "SHOPAPI", "SHOPIMG"},
    }
end
