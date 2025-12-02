
import barch
import redis
import os
print(f'running {__file__}')
exec(open(f"{os.path.dirname(os.path.realpath(__file__))}/test_data.py").read())

barch.start("0.0.0.0", 15000) # start barch on port 15000
gr = redis.Redis(host="127.0.0.0", port=15000, db=0)
def test():
    r = redis.Redis(host="127.0.0.0", port=15000, db=0)

    for w in words:
        if r.exists(w):
            print(f'{w} data exists- testing')
            assert test_set[w] == r.get(w)
        else:
            print(f'{w} does not exist- setting')
            r.set(w, test_set[w])
    r.execute_command('SAVE')
    for w in words:
        print(f"testing:{w}")
        assert r.get(w) == test_set[w]

test()
#assert(barch.stats().value_bytes_compressed > 0)
