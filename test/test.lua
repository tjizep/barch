local count = 1000
local result = {}
local i = 1
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local keylen = 8
local index = 0
local succeses = 0
local convert
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
    return num
end

local tochars123 = function(num)
    return '#'..num
end

local test = function()

    result[inc()] = redis.call('ODLB',"abaachcd")

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        redis.call('ODSET',k,v)
        if not redis.call('ODGET',k) == v then
            result[inc()] = {k, v, redis.call('ODGET',k)} --redis.call('cdict.lb',k)
        else
            succeses = succeses + 1
        end
        
    end

    result[inc()] = {"'ODRANGE',convert(200), convert(210), 1000", redis.call('ODRANGE',convert(200), convert(210), 1000)}
    result[inc()] = {"'ODMIN'",redis.call('ODMIN')}
    result[inc()] = {"'ODMAX'",redis.call('ODMAX')}
    result[inc()] = {"'ODLB'",redis.call('ODLB',"abaachcd")}
    result[inc()] = {[['ODLB',"ddddddde"]], redis.call('ODLB',"ddddddde")}
    result[inc()] = {[['ODLB',"z"]], redis.call('ODLB',"z")}
    result[inc()] = {[['ODLB',"dddddddd"]], redis.call('ODLB',"dddddddd")}
    result[inc()] = {[['ODLB',"aaaaaad#"]], redis.call('ODLB',"aaaaaad#")}
    result[inc()] = {[['ODLB',"aaaaaad1"]], redis.call('ODLB',"aaaaaad1")}
    result[inc()] = {[['ODLB',"aaaadaee"]], redis.call('ODLB',"aaaadaee")}
    result[inc()] = {[['ODLB',"aaaaaaad"]], redis.call('ODLB',"aaaaaaad")}
    result[inc()] = {[['ODLB',"aaaachcd"]], redis.call('ODLB',"aaaachcd")}

    result[inc()] = {[['ODSTATS']], redis.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], redis.call('ODSIZE')}

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        redis.call('ODREM',k)
    end
    
    result[inc()] = {[['ODSTATS']], redis.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']],redis.call('ODSIZE')}
    result[inc()] = {[['ODOPS']], redis.call('ODOPS')}
    result[inc()] = {"succeses", succeses}
end

convert = tochars123
test()
convert = tocharsnum
test()
convert = tocharsabc
test()

return result