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
    return '#'..num
end

local tocharsdbl = function(num)
    return num+0.5
end

local test = function()

    local succeses = 0
    tests = tests + 1
    result[inc()] = {"running test "..tests}
    result[inc()] = vk.call('ODLB',"abaachcd")

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('ODSET',k,v)
    end
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('ODGET',k) ~= v then
            result[inc()] = {k, v, vk.call('ODGET',k)} --vk.call('cdict.lb',k)
        else
            succeses = succeses + 1
        end
        
    end
    result[inc()] = {"'ODRANGE',convert(2), convert(count-2), 10", vk.call('ODRANGE',convert(2), convert(count-2), 4)}
    result[inc()] = {[['ODMIN']], vk.call('ODMIN')}
    result[inc()] = {[['ODMAX']], vk.call('ODMAX')}
    result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], vk.call('ODSIZE')}
    result[inc()] = {"succeses for test "..tests..": "..succeses}
end

local abctest = function()
    result[inc()] = {"'ODRANGE',convert(200), convert(210), 1000", vk.call('ODRANGE',convert(200), convert(210), 1000)}
    result[inc()] = {"'ODMIN'", vk.call('ODMIN')}
    result[inc()] = {"'ODMAX'", vk.call('ODMAX')}
    result[inc()] = {"'ODLB'", vk.call('ODLB',"abaachcd")}
    result[inc()] = {[['ODLB',"ddddddde"]], vk.call('ODLB',"ddddddde")}
    result[inc()] = {[['ODLB',"z"]], vk.call('ODLB',"z")}
    result[inc()] = {[['ODLB',"dddddddd"]], vk.call('ODLB',"dddddddd")}
    result[inc()] = {[['ODLB',"aaaaaad#"]], vk.call('ODLB',"aaaaaad#")}
    result[inc()] = {[['ODLB',"aaaaaad1"]], vk.call('ODLB',"aaaaaad1")}
    result[inc()] = {[['ODLB',"aaaadaee"]], vk.call('ODLB',"aaaadaee")}
    result[inc()] = {[['ODLB',"aaaaaaad"]], vk.call('ODLB',"aaaaaaad")}
    result[inc()] = {[['ODLB',"aaaachcd"]], vk.call('ODLB',"aaaachcd")}    
end

local clear = function()
    local failures = 0
    local success = 0
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('ODREM',k) == v then
            success = success + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('ODGET',k)}
        end

        if vk.call('ODGET',k) then
            result[inc()] = {"Failed remove",k, v, vk.call('ODGET',k)}
            failures = failures + 1
        end
    end
    
    result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], vk.call('ODSIZE')}
    result[inc()] = {[['ODOPS']], vk.call('ODOPS')}
    result[inc()] = {'REMOVE FAILURES', failures}
    result[inc()] = {'REMOVE SUCCESSES', success}
    
end 

--[[ Testing ints,doubles and string key types]]

convert = tocharsnum
test()
clear()

convert = tochars123
test()
clear()

convert = tocharsabc
test()
abctest()
clear()

convert = tocharsdbl
test()
clear()
--[[]]
return result