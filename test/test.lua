local count = 1000
local result = {}
local i = 1
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local keylen = 8
local index = 0

local func = {
    tochars = function(num)
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
    end,
    tochars123 = function(num)
        return '#'..num
    end,
    inc = function()
        index = index + 1
        return index
    end
}
local succeses = 0

result[func.inc()] = redis.call('ODLB',"abaachcd")

for i = 1, count do
    local k = func.tochars(i-1)
    local v = '#'..i
    
    redis.call('ODSET',k,v)
    if not redis.call('ODGET',k) == v then
        result[func.inc()] = {k, v, redis.call('ODGET',k)} --redis.call('cdict.lb',k)
    else
        succeses = succeses + 1
    end
    
end

result[func.inc()] = redis.call('ODRANGE',func.tochars(200), func.tochars(210), 1000)

result[func.inc()] = redis.call('ODLB',"abaachcd")
result[func.inc()] = redis.call('ODLB',"ddddddde")
result[func.inc()] = redis.call('ODLB',"z")
result[func.inc()] = redis.call('ODLB',"dddddddd")
result[func.inc()] = redis.call('ODLB',"aaaaaad#")
result[func.inc()] = redis.call('ODLB',"aaaaaad1")
result[func.inc()] = redis.call('ODLB',"aaaadaee")
result[func.inc()] = redis.call('ODLB',"aaaaaaad")
result[func.inc()] = redis.call('ODLB',"aaaachcd")

result[func.inc()] = redis.call('ODSTATS')
result[func.inc()] = redis.call('ODSIZE')

for i = 1, count do
    local k = func.tochars(i-1)
    local v = '#'..i
    redis.call('ODREM',k)
end
result[func.inc()] = redis.call('ODSTATS')
result[func.inc()] = redis.call('ODSIZE')
result[func.inc()] = redis.call('ODOPS')
result[func.inc()] = succeses
return result