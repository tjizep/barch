-- HNSW graph helpers. Required by SET and CLOSEST.
-- The live graph lives in the caller's space under hnsw.* keys.
-- Candidate and found-sets during a walk are private ART trees (barch.art).

local PFX = "hnsw."

local function k_count()  return PFX .. "count" end
local function k_entry()  return PFX .. "entry" end
local function k_M()      return PFX .. "M" end
local function k_efc()    return PFX .. "efc" end
local function k_efs()    return PFX .. "efs" end
local function k_heur()   return PFX .. "heur" end
local function k_word(id) return PFX .. "w." .. id end
local function k_id(w)    return PFX .. "id." .. w end
local function k_level(id) return PFX .. "lv." .. id end
local function k_nbs(id, layer) return PFX .. "n." .. id .. "." .. layer end

function getn(key, default)
    local v = barch.store.get(key)
    if v == nil then return default end
    return tonumber(v) or default
end

function M()    return getn(k_M(), 16) end
function efc()  return getn(k_efc(), 64) end
function efs()  return getn(k_efs(), 32) end
function heur() return getn(k_heur(), 0) ~= 0 end
function count() return getn(k_count(), 0) end
function entry()
    local v = barch.store.get(k_entry())
    if v == nil then return nil end
    return v
end

function word_of(id)
    return barch.store.get(k_word(id))
end

function id_of(word)
    return barch.store.get(k_id(word))
end

function level_of(id)
    return getn(k_level(id), 0)
end

function nbs_of(id, layer)
    local s = barch.store.get(k_nbs(id, layer))
    if s == nil or s == "" then return {} end
    local out = {}
    for tok in string.gmatch(s, "%S+") do
        out[#out + 1] = tok
    end
    return out
end

function set_nbs(id, layer, ids)
    barch.store.set(k_nbs(id, layer), table.concat(ids, " "))
end

-- integer Levenshtein. phrases are fine; this is the HNSW metric
function levenshtein(a, b)
    if a == b then return 0 end
    a = a or ""
    b = b or ""
    local la, lb = #a, #b
    if la == 0 then return lb end
    if lb == 0 then return la end
    if la > lb then
        a, b, la, lb = b, a, lb, la
    end
    local prev = {}
    local curr = {}
    for j = 0, lb do
        prev[j] = j
    end
    for i = 1, la do
        curr[0] = i
        local ca = string.byte(a, i)
        for j = 1, lb do
            local cost = 0
            if ca ~= string.byte(b, j) then cost = 1 end
            local del = prev[j] + 1
            local ins = curr[j - 1] + 1
            local sub = prev[j - 1] + cost
            local m = del
            if ins < m then m = ins end
            if sub < m then m = sub end
            curr[j] = m
        end
        prev, curr = curr, prev
    end
    return prev[lb]
end
local distance = levenshtein
-- ART priority queue keyed by "dist id" so equal distances still order
local function pq_key(dist, id)
    return tostring(dist) .. " " .. tostring(id)
end

local function pq_dist(key)
    return tonumber((string.match(key, "^(%S+)"))) or 0
end

function random_level(m)
    local mL = 1 / math.log(math.max(m, 2))
    local r = math.random()
    if r < 1e-12 then r = 1e-12 end
    return math.floor(-math.log(r) * mL)
end

function Mmax(layer, m)
    if layer == 0 then return m * 2 end
    return m
end

-- greedy walk on one layer: always step to a closer neighbour
function greedy(q, start, layer)
    local curr = start
    local cw = word_of(curr)
    if cw == nil then return curr end
    local cd = distance(q, cw)
    local moved = true
    while moved do
        moved = false
        local nbs = nbs_of(curr, layer)
        for i = 1, #nbs do
            local nb = nbs[i]
            local nw = word_of(nb)
            if nw ~= nil then
                local d = distance(q, nw)
                if d < cd then
                    curr, cd, moved = nb, d, true
                end
            end
        end
    end
    return curr
end

-- ef-search on one layer. C is a min queue of candidates, W the found set.
-- returns a list of ids, closest first, at most ef of them
function search_layer(q, eps, ef, layer)
    if ef < 1 then ef = 1 end
    local visited = {}
    local C = barch.art()
    local W = barch.art()

    local function consider(id)
        if visited[id] then return end
        visited[id] = true
        local w = word_of(id)
        if w == nil then return end
        local d = distance(q, w)
        local far = W:max()
        if W:size() < ef or far == nil or d < pq_dist(far) then
            local pk = pq_key(d, id)
            C:set(pk, id)
            W:set(pk, id)
            if W:size() > ef then
                W:popmax()
            end
        end
    end

    for i = 1, #eps do
        consider(eps[i])
    end

    while C:size() > 0 do
        local ck, cid = C:popmin()
        if cid == nil then break end
        local far = W:max()
        if far ~= nil and pq_dist(ck) > pq_dist(far) then
            break
        end
        local nbs = nbs_of(cid, layer)
        for i = 1, #nbs do
            consider(nbs[i])
        end
    end

    local out = {}
    while W:size() > 0 do
        local _, id = W:popmin()
        out[#out + 1] = id
    end
    return out
end

function select_simple(ids, m)
    local out = {}
    local n = m
    if n > #ids then n = #ids end
    for i = 1, n do
        out[i] = ids[i]
    end
    return out
end

-- keep neighbours that are not already closer to each other than to the query
function select_heuristic(q, ids, m)
    local selected = {}
    for i = 1, #ids do
        if #selected >= m then break end
        local c = ids[i]
        local cw = word_of(c)
        local dq = distance(q, cw or "")
        local keep = true
        for j = 1, #selected do
            local sw = word_of(selected[j])
            if distance(cw or "", sw or "") < dq then
                keep = false
                break
            end
        end
        if keep then
            selected[#selected + 1] = c
        end
    end
    if #selected < m then
        local have = {}
        for i = 1, #selected do have[selected[i]] = true end
        for i = 1, #ids do
            if #selected >= m then break end
            if not have[ids[i]] then
                selected[#selected + 1] = ids[i]
            end
        end
    end
    return selected
end

function prune(id, layer, cap)
    local nbs = nbs_of(id, layer)
    if #nbs <= cap then return end
    local w = word_of(id) or ""
    local Q = barch.art()
    for i = 1, #nbs do
        local nb = nbs[i]
        local d = distance(w, word_of(nb) or "")
        Q:set(pq_key(d, nb), nb)
    end
    local keep = {}
    for i = 1, cap do
        if Q:size() == 0 then break end
        local _, nb = Q:popmin()
        keep[#keep + 1] = nb
    end
    set_nbs(id, layer, keep)
end

local function has(list, id)
    for i = 1, #list do
        if list[i] == id then return true end
    end
    return false
end

function link(a, b, layer, cap)
    local na = nbs_of(a, layer)
    if not has(na, b) then
        na[#na + 1] = b
        set_nbs(a, layer, na)
        prune(a, layer, cap)
    end
    local nb = nbs_of(b, layer)
    if not has(nb, a) then
        nb[#nb + 1] = a
        set_nbs(b, layer, nb)
        prune(b, layer, cap)
    end
end

function insert(word)
    local exists = id_of(word)
    if exists ~= nil then
        return exists, false
    end
    local m = M()
    local n = count() + 1
    local id = tostring(n)
    local lvl = random_level(m)
    barch.store.set(k_count(), tostring(n))
    barch.store.set(k_word(id), word)
    barch.store.set(k_id(word), id)
    barch.store.set(k_level(id), tostring(lvl))

    local ep = entry()
    if ep == nil then
        barch.store.set(k_entry(), id)
        return id, true
    end

    local ep_lvl = level_of(ep)
    local curr = ep
    if ep_lvl > lvl then
        for layer = ep_lvl, lvl + 1, -1 do
            curr = greedy(word, curr, layer)
        end
    end

    local top = lvl
    if ep_lvl < top then top = ep_lvl end
    for layer = top, 0, -1 do
        local found = search_layer(word, {curr}, efc(), layer)
        local pick
        if heur() then
            pick = select_heuristic(word, found, m)
        else
            pick = select_simple(found, m)
        end
        local cap = Mmax(layer, m)
        for i = 1, #pick do
            link(id, pick[i], layer, cap)
        end
        if #found > 0 then
            curr = found[1]
        end
    end

    if lvl > ep_lvl then
        barch.store.set(k_entry(), id)
    end
    return id, true
end

function closest(q, k)
    if k == nil or k < 1 then k = 1 end
    local ep = entry()
    if ep == nil then return {} end
    local curr = ep
    local top = level_of(ep)
    if top > 0 then
        for layer = top, 1, -1 do
            curr = greedy(q, curr, layer)
        end
    end
    local found = search_layer(q, {curr}, math.max(efs(), k), 0)
    local out = {}
    local n = k
    if n > #found then n = #found end
    for i = 1, n do
        local id = found[i]
        out[i] = {word_of(id), distance(q, word_of(id) or "")}
    end
    return out
end

function apply_tune(preset, m, construction, search, use_heur)
    if preset == "fast" then
        m, construction, search, use_heur = 8, 16, 8, 0
    elseif preset == "default" then
        m, construction, search, use_heur = 16, 64, 32, 0
    elseif preset == "accurate" then
        m, construction, search, use_heur = 32, 200, 100, 1
    end
    if m ~= nil then barch.store.set(k_M(), tostring(m)) end
    if construction ~= nil then barch.store.set(k_efc(), tostring(construction)) end
    if search ~= nil then barch.store.set(k_efs(), tostring(search)) end
    if use_heur ~= nil then barch.store.set(k_heur(), tostring(use_heur)) end
end

function stats()
    return {
        count(),
        M(),
        efc(),
        efs(),
        heur() and 1 or 0,
        entry() or "",
    }
end

function call(...)
    return "graph"
end
