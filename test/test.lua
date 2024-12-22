local count = 1000
local result = {}
local i = 1
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local keylen = 8
    
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
    end
}

for i = 1, count do
    local k = func.tochars(i-1)
    local v = '#'..i
    redis.call('cdict.set',k,v)
    result[i] = {k, v, redis.call('cdict.get',k)} --redis.call('cdict.lb',k)
end

result[count+1] = redis.call('cdict.lb',"aaaaaaad")
result[count+2] = redis.call('cdict.lb',"aaaachcd")
result[count+3] = redis.call('cdict.lb',"abaachcd")
result[count+4] = redis.call('cdict.range',func.tochars(200), func.tochars(210), 1000)

result[count+5] = redis.call('cdict.statistics')
result[count+6] = redis.call('cdict.size')

for i = 1, count do
    local k = func.tochars(i-1)
    local v = '#'..i
    redis.call('cdict.rem',k)
end
result[count+7] = redis.call('cdict.statistics')
result[count+8] = redis.call('cdict.size')
result[count+9] = redis.call('cdict.operations')
return result