-- kind=http: bind options plus the resource keys.
-- HTTP START CONF uses this. Port can still come from the command.

function call()
    return "http"
end

function transport()
    return {
        kind = "http",
        bind = "127.0.0.1",
        keys = {"ECHO", "PAGE", "SESSION"},
    }
end
