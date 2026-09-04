
import scale
import barch
import redis
import os

# both ctest runs of this script share one directory: the second reads
# what the first saved
scale.workdir("largetest")

PORT = scale.port(default=15000)
print(f'running {__file__}')
exec(open(f"{os.path.dirname(os.path.realpath(__file__))}/test_data.py").read())

barch.start("0.0.0.0", PORT)
gr = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
def test():
    r = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)

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
