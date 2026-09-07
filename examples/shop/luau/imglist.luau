-- What `/img` could hold: every product in the catalog, whether its picture has
-- been fetched or not. `configuration:shop.fs_source_list` names this.
--
-- Without it, `FS LS /img` shows the images somebody happened to look at, which is
-- a strange thing to hand anyone browsing. With it, the listing is the catalog and
-- what has been fetched is marked as a file rather than as `remote`.

function call(path)
    if path ~= "/img" then
        return nil
    end
    local raw = barch.fs.get("/meta/index.json")
    if raw == nil then
        return nil
    end
    local names = {}
    for _, p in ipairs(simdjson.parse(raw)) do
        names[#names + 1] = p.asin
    end
    return names
end
