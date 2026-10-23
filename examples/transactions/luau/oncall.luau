--[[
Doctors on call as a stored function. ../oncall.py has the job and its rules;
`oncall.py --luau` runs it through this.

    CALLF oncall off <doctor> <doctor> [doctor ...]   go off call, if someone else stays on: true, nil if refused
    CALLF oncall on <doctor>                          go on call: true
    CALLF oncall count <doctor> [doctor ...]          how many are on call

`off` is told the doctor going off first, then every doctor on the shift. A doctor's
key is an int64 (see txretry.luau), 1 on call and 0 off.
]]
local tx = require("txretry")

local ZERO, ONE = integer.create(0), integer.create(1)

local function off(doctor, ...)
    -- for update: the doctor's own key is written next, and holding the others too is
    -- what stops two from going off at once (the write skew)
    local count, mine = 0, false
    for _, d in {...} do
        if tx.for_update(d) == ONE then
            count += 1
            mine = mine or d == doctor
        end
    end
    if not mine or count < 2 then
        tx.refuse()                             -- the last one on call stays
    end
    tx.put(doctor, ZERO)
    return true
end

local function on(doctor)
    tx.put(doctor, ONE)
    return true
end

local function count(...)
    local n = ZERO
    for _, d in {...} do
        n = tx.plus(n, tx.read(d))
    end
    return n
end

function call(action, ...)
    if action == "off" then
        return tx.in_transaction(off, ...)
    elseif action == "on" then
        return tx.in_transaction(on, ...)
    elseif action == "count" then
        return tx.in_transaction(count, ...)
    end
    error("oncall takes off, on or count", 0)
end
