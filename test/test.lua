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
local inc = function()
    index = index + 1
    return index
end

local tocharsabc = function(num)
    local n = num
    local r = ''
    while (true) do
        r = chars[math.mod(n, radix)+1]..r
        n = math.floor(n / radix)
        if n == 0 then
            break
        end
    end
    r = string.format("%"..keylen.."s", r)
    r = string.gsub(r," ", "a")
    return r
end

local tocharsnum = function(num)
    if num % 2 == 0 then
        return (num + 1634556)
    else
        return -(num + 1634556)
    end
end

local tochars123 = function(num)
    return 'a'..(90 + num)
end

local tocharsdbl = function(num)
    return num+0.5
end

local test = function()

    local succeses = 0
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
            result[inc()] = {k, v, vk.call('B.GET',k)} --vk.call('B.lb',k)
        else
            succeses = succeses + 1
        end
        
    end
    result[inc()] = {"'B.RANGE',convert(2), convert(count-2), 10", vk.call('B.RANGE',convert(2), convert(count-2), 4)}
    result[inc()] = {[['B.MIN']], vk.call('B.MIN')}
    result[inc()] = {[['B.MAX']], vk.call('B.MAX')}
    result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
    result[inc()] = {[['B.SIZE']], vk.call('B.SIZE')}
    result[inc()] = {"succeses for test "..tests..": "..succeses}
end

local abctest = function()
    result[inc()] = {"'B.RANGE',convert(200), convert(210), 1000", vk.call('B.RANGE',convert(200), convert(210), 1000)}
    result[inc()] = {"'B.MIN'", vk.call('B.MIN')}
    result[inc()] = {"'B.MAX'", vk.call('B.MAX')}
    result[inc()] = {"'B.LB'", vk.call('B.LB',"abaachcd")}
    result[inc()] = {[['B.LB',"ddddddde"]], vk.call('B.LB',"ddddddde")}
    result[inc()] = {[['B.LB',"z"]], vk.call('B.LB',"z")}
    result[inc()] = {[['B.LB',"dddddddd"]], vk.call('B.LB',"dddddddd")}
    result[inc()] = {[['B.LB',"aaaaaad#"]], vk.call('B.LB',"aaaaaad#")}
    result[inc()] = {[['B.LB',"aaaaaad1"]], vk.call('B.LB',"aaaaaad1")}
    result[inc()] = {[['B.LB',"aaaadaee"]], vk.call('B.LB',"aaaadaee")}
    result[inc()] = {[['B.LB',"aaaaaaad"]], vk.call('B.LB',"aaaaaaad")}
    result[inc()] = {[['B.LB',"aaaachcd"]], vk.call('B.LB',"aaaachcd")}
end

local clear = function()
    local failures = 0
    local success = 0
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('B.GET',k) == nil then
            result[inc()] = {"Failed get before remove",k, v, vk.call('B.GET',k)}
            failures = failures + 1
        end
        if vk.call('B.REM',k) == v then
            success = success + 1
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
    result[inc()] = {'REMOVE FAILURES', failures}
    result[inc()] = {'REMOVE SUCCESSES', success}
    
end 

--[[ Testing ints,doubles and string key types]]
--[[]
convert = tocharsnum
test()
clear()
]]
convert = tochars123
test()
--clear()
--[[
convert = tocharsabc
test()
abctest()
clear()

convert = tocharsdbl
test()
clear()
]]
return result