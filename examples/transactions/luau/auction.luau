--[[
An auction as a stored function. ../auction.py has the job and its rules;
`auction.py --luau` runs it through this.

    CALLF auction bid <bidder> <item> <raise by, cents> <reserve, cents>
        the new top bid in cents, nil if refused (already winning, or can't pay)

Wallets and bids are int64 cents (see txretry.luau). The top bid is a record,
"bidder cents", so it stays text.
]]
local tx = require("txretry")

local ONE = integer.create(1)

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
    local offer = tx.plus(best, tx.whole(raise_by))
    local wallet = tx.for_update("wallet:" .. me)
    if integer.lt(wallet, offer) then
        tx.refuse()
    end
    tx.put("wallet:" .. me, tx.minus(wallet, offer))
    if leader then
        -- the outbid bidder gets their money back
        tx.add("wallet:" .. leader, best)
    end
    barch.store.set("item:" .. item .. ":top", me .. " " .. tostring(offer))
    local n = tx.add("item:" .. item .. ":bids", ONE)
    tx.put("item:" .. item .. ":bid:" .. tostring(n), offer)
    return offer
end

function call(action, ...)
    if action == "bid" then
        return tx.in_transaction(bid, ...)
    end
    error("auction takes bid", 0)
end
