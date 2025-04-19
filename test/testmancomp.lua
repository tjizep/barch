local vk
vk = redis
vk.call("B.CONFIG", "SET","max_memory_bytes", "2000m")
vk.call("B.CONFIG", "SET","active_defrag", "off")
vk.call("B.CONFIG", "SET","compression", "off")
vk.call("B.CONFIG", "SET","save_interval", "10000000000")
vk.call("B.CONFIG", "SET","max_modifications_before_save", "10000000000")

vk.call('B.CLEAR')
local t = vk.call('B.MILLIS')
for i=1,40000000 do
    vk.call('B.ZADD', i, i+1.1, 'first') --i+2.0, 'second' ,i+3.5, 'third'
end
local tr = {vk.call('B.SIZE'),vk.call('B.MILLIS')-t}
vk.call('B.CLEAR')
return tr
--return {vk.call('DBSIZE'),vk.call('B.MILLIS')-t}
