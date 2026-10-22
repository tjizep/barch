--[[
An auction as a stored function. ../auction.py has the job and its rules;
`auction.py --luau` runs it through this.

    CALLF auction bid <bidder> <item> <raise by, cents> <reserve, cents>
        the new top bid in cents, nil if refused (already winning, or can't pay)
]]
local tx = require("txretry")

local function bid(me, item, raise_by, reserve)
    local top = barch.store.get("item:" .. item .. ":top", true)    -- "bidder cents", or none yet
    local leader, best = nil, tx.whole(reserve)
    if top then
        local who, cents = string.match(top, "^(%S+) (%d+)$")
        leader, best = who, tx.whole(cents)
    end
    if leader == me then
        tx.refuse()                              -- already winning
    end
    local offer = best + tx.whole(raise_by)
    local wallet = tx.for_update("wallet:" .. me)
    if wallet < offer then
        tx.refuse()
    end
    tx.put("wallet:" .. me, wallet - offer)
    if leader then
        -- the outbid bidder gets their money back
        tx.add("wallet:" .. leader, best)
    end
    barch.store.set("item:" .. item .. ":top", me .. " " .. string.format("%d", offer))
    local n = string.format("%d", tx.add("item:" .. item .. ":bids", 1))
    tx.put("item:" .. item .. ":bid:" .. n, offer)
    return offer
end

function call(action, ...)
    if action == "bid" then
        return tx.in_transaction(bid, ...)
    end
    error("auction takes bid", 0)
end
