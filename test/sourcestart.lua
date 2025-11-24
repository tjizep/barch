local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.SAVE')
vk.call('B.START','127.0.0.1','14000')
--vk.call('B.PUBLISH','127.0.0.1','13000')

vk.call('B.SET','1','one:test')
vk.call('B.SET','2','two:test')
vk.call('B.SET','3','three:test')
for i = 4, 10000 do
    vk.call('B.SET',i,'data'..i)
end
vk.call('B.SAVE')
return vk.call('B.SIZE')