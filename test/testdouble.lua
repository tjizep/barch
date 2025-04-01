local vk
vk = redis

local count = 1000
local result = {}
local i = 1
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local keylen = 16
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0

local inc = function()
    index = index + 1
    return index
end

local tocharsdbl = function(num)
    return num+0.5
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}
    result[inc()] = vk.call('B.LB',"abaachcd")

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('B.SET',k,v)
    end
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('B.GET',k) ~= v then
            result[inc()] = {k, v, vk.call('B.GET',k)} --vk.call('cdict.lb',k)
        else
            successes = successes + 1
        end

    end
    result[inc()] = {"'B.RANGE',convert(2), convert(count-2), 10", vk.call('B.RANGE',convert(2), convert(count-2), 4)}
    result[inc()] = {[['B.MIN']], vk.call('B.MIN')}
    result[inc()] = {[['B.MAX']], vk.call('B.MAX')}
    result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
    result[inc()] = {[['B.SIZE']], vk.call('B.SIZE')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end

local clear = function()

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('B.GET',k) == nil then
            result[inc()] = {"Failed get before remove",k, v, vk.call('B.GET',k)}
            failures = failures + 1
        end
        if vk.call('B.REM',k) == v then
            successes = successes + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('B.GET',k)}
        end

        if vk.call('B.GET',k) then
            result[inc()] = {"Failed remove",k, v, vk.call('B.GET',k)}
            failures = failures + 1
        end
    end

    result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
    result[inc()] = {[['B.SIZE']], vk.call('B.SIZE')}
    result[inc()] = {[['B.OPS']], vk.call('B.OPS')}
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}

end

--[[ Testing doubles]]
vk.call('B.CLEAR')

convert = tocharsdbl
test()
clear()
assert(successes==2000, "test failures")
assert(failures==0, "test failures")

return result