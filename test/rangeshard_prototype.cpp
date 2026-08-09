// Prototype for ordered range sharding. NOT wired into the build or into barch - it
// exists to answer questions about the algorithm before any of it is written against
// real shards. See DONE 30 and DONE 31 for what it found and what was built from it.
//
//   g++ -O2 -std=c++20 test/rangeshard_prototype.cpp -o /tmp/rs && /tmp/rs [N] [budget] [keys] [tolerance]
//
// The shards are std::map, standing in for an art. The index is a sorted flat vector.
// Seven strategies are kept, selected in main(), so a change can be compared against
// what came before rather than replacing it. 4 is what the insert path would do, 5 and
// 6 are what the maintenance thread does - 5 asking only that no shard is over the
// threshold, 6 reading the tolerance as a band around the average, which is what
// src/range_index.cpp implements.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <chrono>
#include <map>
#include <random>
#include <string>
#include <vector>

typedef std::string Key;

struct stats { uint64_t moves = 0, rebalances = 0, inserts = 0, max_move_one_insert = 0; };

struct range_shards {
    size_t N;                 // shard count, constant
    size_t budget;            // max keys moved in one rebalance step, constant
    double tolerance;         // a shard is over when size > tolerance * total / N
    int strategy;             // 0 up-only, 1 bidi one step, 2 bidi cascade, 3 worst-first
    // strategy 5 only: the insert path does no work at all and a sweep runs every
    // `sweep_every` inserts, standing in for the maintenance thread. Each sweep runs
    // `sweep_passes` cascades, every one starting at the largest shard - a sweep, unlike
    // an insert, has no particular shard to start from, so it has to look for the worst
    size_t sweep_every = 1;
    size_t sweep_passes = 1;
    uint64_t since_sweep = 0;
    uint64_t sweeps = 0;
    std::vector<std::map<Key, int>> shards;
    // the index is the minimum key of each shard above 0. Small - at most shard_count
    // entries - looked at on every route and written only by a rebalance, so a sorted
    // flat vector beats a node based map: the whole thing is a couple of cache lines
    struct entry { Key key; size_t shard; };
    std::vector<entry> index;
    size_t total = 0;
    stats st;

    range_shards(size_t n, size_t b, double tol, int strat)
        : N(n), budget(b), tolerance(tol), strategy(strat), shards(n) {}

    size_t route(const Key& k) const {
        auto it = std::upper_bound(index.begin(), index.end(), k,
                                   [](const Key& a, const entry& e) { return a < e.key; });
        if (it == index.begin()) return 0;      // below every boundary
        return std::prev(it)->shard;
    }

    void reindex(size_t s) {
        index.erase(std::remove_if(index.begin(), index.end(),
                                   [s](const entry& e) { return e.shard == s; }), index.end());
        if (s >= 1 && !shards[s].empty()) {
            entry e{shards[s].begin()->first, s};
            index.insert(std::upper_bound(index.begin(), index.end(), e.key,
                             [](const Key& a, const entry& x) { return a < x.key; }), e);
        }
    }

    /**
     * What a load would do: the index is nothing but the minimum key of each shard, so
     * it never has to be written down or read back - it is rebuilt by asking each shard
     * for its first key, which an art finds in a walk down the left spine.
     */
    std::vector<entry> rebuilt_index() const {
        std::vector<entry> out;
        for (size_t s = 1; s < N; ++s)
            if (!shards[s].empty()) out.push_back({shards[s].begin()->first, s});
        return out;   // already in key order, because the shards are a partition
    }

    size_t threshold() const {
        size_t t = (size_t) (tolerance * (double) total / (double) N);
        return t < 1 ? 1 : t;
    }

    // move the top `k` keys of `from` into `from+1`
    void shed_up(size_t from, size_t k) {
        auto& a = shards[from]; auto& b = shards[from + 1];
        for (size_t i = 0; i < k && !a.empty(); ++i) {
            auto it = std::prev(a.end());
            b.insert(*it); a.erase(it);
            ++st.moves;
        }
        reindex(from); reindex(from + 1);
    }
    // move the bottom `k` keys of `from` into `from-1`
    void shed_down(size_t from, size_t k) {
        auto& a = shards[from]; auto& b = shards[from - 1];
        for (size_t i = 0; i < k && !a.empty(); ++i) {
            auto it = a.begin();
            b.insert(*it); a.erase(it);
            ++st.moves;
        }
        reindex(from); reindex(from - 1);
    }

    // one shed from `cur` toward whichever neighbour has room. returns where it went,
    // or cur if it could not move
    size_t shed_once(size_t cur, size_t cap) {
        // 6 drops the threshold test: a shard sheds whenever a neighbour is meaningfully
        // smaller, whether or not it is over. Without that a cascade dies at the first
        // shard under the threshold and never reaches the ones past it, which is how the
        // tail of the space is left empty while the space still measures balanced
        if (strategy != 6 && shards[cur].size() <= threshold()) return cur;
        size_t take = std::min(shards[cur].size() > threshold() ? shards[cur].size() - threshold() : 0, cap);
        bool up_ok = cur + 1 < N;
        bool down_ok = (strategy != 0) && cur > 0;
        if (!up_ok && !down_ok) return cur;
        bool go_up = up_ok && (!down_ok || shards[cur + 1].size() <= shards[cur - 1].size());
        size_t nb = go_up ? shards[cur + 1].size() : shards[cur - 1].size();
        if (strategy == 4 || strategy == 5 || strategy == 6) {
            // move only enough to meet the neighbour half way, rather than everything
            // above the threshold. A shard that is over by a lot then bleeds into its
            // neighbour over several inserts instead of dumping into it and pushing the
            // whole cascade over in one go
            if (shards[cur].size() <= nb + 1) return cur;
            take = std::min(budget, (shards[cur].size() - nb) / 2);
            if (take == 0) return cur;
        }
        ++st.rebalances;
        if (go_up) { shed_up(cur, take); return cur + 1; }
        shed_down(cur, take); return cur - 1;
    }

    size_t largest() const {
        size_t worst = 0;
        for (size_t i = 0; i < N; ++i) if (shards[i].size() > shards[worst].size()) worst = i;
        return worst;
    }

    // what the maintenance thread does: no shard in particular to start from, so start
    // at the largest and cascade away from it, and keep doing that until nothing is over
    // the threshold any more.
    //
    // The work has to be driven by how far out of balance the space is, not by a fixed
    // number of passes: between two sweeps an arbitrary number of inserts can arrive,
    // and a fixed pass count silently stops keeping up - see what a fixed 4 passes does
    // to an ascending workload once sweeps are 1024 inserts apart. `sweep_passes` is
    // the cap that stops a sweep hogging the thread, not the amount of work it does.
    size_t smallest() const {
        size_t least = 0;
        for (size_t i = 0; i < N; ++i) if (shards[i].size() < shards[least].size()) least = i;
        return least;
    }

    /**
     * 5 asks only that no shard is over the threshold. 6 reads the tolerance as a band
     * around the average rather than a ceiling over it, and asks that no shard is as far
     * under as another may be over.
     *
     * The difference matters more than it sounds. A ceiling alone is satisfied by 6 of 8
     * shards sitting at exactly 1.25x the average and the last two holding almost
     * nothing - the arithmetic works out, and the space is measurably balanced with a
     * quarter of its shards unused.
     */
    bool balanced() const {
        double avg = (double) total / (double) N;
        if (!total) return true;
        if ((double) shards[largest()].size() > tolerance * avg) return false;
        if (strategy == 6 && (double) shards[smallest()].size() < avg / tolerance) return false;
        return true;
    }

    void sweep() {
        ++sweeps;
        for (size_t p = 0; p < sweep_passes; ++p) {
            if (balanced()) break;                        // nothing left to do
            size_t cur = largest();
            bool moved = false;
            for (size_t step = 0; step < N; ++step) {
                size_t next = shed_once(cur, budget);
                if (next == cur) break;
                cur = next;
                moved = true;
            }
            if (!moved) break;    // over the threshold but unable to shed: give up
        }
    }

    void rebalance_from(size_t s) {
        if (strategy == 5 || strategy == 6) {
            // the insert path does nothing; the sweep is on its own clock
            if (++since_sweep < sweep_every) return;
            since_sweep = 0;
            sweep();
            return;
        }
        if (strategy == 3) {
            // spend the budget where it is worst, not where the insert happened
            size_t worst = 0;
            for (size_t i = 0; i < N; ++i) if (shards[i].size() > shards[worst].size()) worst = i;
            shed_once(worst, budget);
            return;
        }
        if (strategy == 2 || strategy == 4) {
            // cascade, each level capped at the budget. Bounded by N * budget, and in
            // steady state each level only has a key or two of excess
            size_t cur = s;
            for (size_t step = 0; step < N; ++step) {
                size_t next = shed_once(cur, budget);
                if (next == cur) break;
                cur = next;
            }
            return;
        }
        // 0 and 1: one budget shared across the walk, as first defined
        size_t left = budget, cur = s, guard = 0;
        while (left > 0 && guard++ < N * 2) {
            size_t take = std::min(shards[cur].size() > threshold() ? shards[cur].size() - threshold() : 0, left);
            if (take == 0) break;
            size_t next = shed_once(cur, take);
            if (next == cur) break;
            left -= take; cur = next;
        }
    }

    void insert(const Key& k) {
        size_t s = route(k);
        if (!shards[s].count(k)) ++total;
        shards[s][k] = 1;
        uint64_t before = st.moves;
        rebalance_from(s);
        st.max_move_one_insert = std::max(st.max_move_one_insert, st.moves - before);
        ++st.inserts;
        // the imbalance at the end of the run is not the whole story once rebalancing
        // is on its own clock: between sweeps the space is allowed to drift, and how
        // far it drifts is the cost of taking the work off the insert path
        peak = std::max(peak, imbalance());
    }
    double peak = 0;

    // --- invariants -------------------------------------------------------------
    std::string check() const {
        size_t count = 0;
        for (auto& s : shards) count += s.size();
        if (count != total) return "key count lost: " + std::to_string(count) + " != " + std::to_string(total);
        for (size_t i = 0; i + 1 < N; ++i) {
            if (shards[i].empty()) continue;
            for (size_t j = i + 1; j < N; ++j) {
                if (shards[j].empty()) continue;
                if (!(std::prev(shards[i].end())->first < shards[j].begin()->first))
                    return "partition broken between shard " + std::to_string(i) + " and " + std::to_string(j);
                break;
            }
        }
        for (size_t i = 0; i < N; ++i)
            for (auto& kv : shards[i])
                if (route(kv.first) != i)
                    return "index disagrees for key " + kv.first + ": routes to "
                         + std::to_string(route(kv.first)) + " but lives in " + std::to_string(i);
        auto rb = rebuilt_index();
        if (rb.size() != index.size()) return "rebuilt index has a different size";
        for (size_t i = 0; i < rb.size(); ++i)
            if (rb[i].key != index[i].key || rb[i].shard != index[i].shard)
                return "rebuilt index differs at " + std::to_string(i);
        for (size_t i = 1; i < index.size(); ++i)
            if (!(index[i-1].key < index[i].key)) return "index not in key order";
        return "";
    }

    double imbalance() const {          // max shard / ideal
        size_t mx = 0; for (auto& s : shards) mx = std::max(mx, s.size());
        return (double) mx / ((double) total / (double) N);
    }
    size_t empties() const {
        size_t e = 0; for (auto& s : shards) if (s.empty()) ++e; return e;
    }
};

static Key kof(uint64_t n) { char b[32]; snprintf(b, sizeof b, "%012llu", (unsigned long long) n); return b; }

static void run(const char* name, range_shards& rs, const std::vector<Key>& keys) {
    // fill with the first half, then measure only the second: the cost of spreading an
    // empty space out is a one off, the steady state cost is what bounds an insert
    size_t half = keys.size() / 2;
    for (size_t i = 0; i < half; ++i) rs.insert(keys[i]);
    rs.st = stats{};
    rs.peak = 0;
    for (size_t i = half; i < keys.size(); ++i) rs.insert(keys[i]);
    std::string bad = rs.check();
    printf("  %-22s imbalance %5.2fx  peak %6.2fx  steady moves/insert %6.2f  worst %4llu  empty %2zu  %s\n",
           name, rs.imbalance(), rs.peak, (double) rs.st.moves / (double) rs.st.inserts,
           (unsigned long long) rs.st.max_move_one_insert, rs.empties(),
           bad.empty() ? "ok" : ("BROKEN: " + bad).c_str());
}

int main(int argc, char** argv) {
    size_t N = argc > 1 ? atoi(argv[1]) : 16;
    size_t B = argc > 2 ? atoi(argv[2]) : 64;
    size_t COUNT = argc > 3 ? atoi(argv[3]) : 200000;
    double TOL = argc > 4 ? atof(argv[4]) : 1.0;

    std::mt19937_64 g(12345);
    std::vector<Key> ascending, descending, random_keys, clustered;
    for (size_t i = 0; i < COUNT * 2; ++i) ascending.push_back(kof(i));
    descending = ascending; std::reverse(descending.begin(), descending.end());
    random_keys = ascending; std::shuffle(random_keys.begin(), random_keys.end(), g);
    // 90% of keys into 10% of the space
    for (size_t i = 0; i < COUNT * 2; ++i) {
        uint64_t v = (g() % 100 < 90) ? (g() % (COUNT / 10)) : (g() % COUNT);
        clustered.push_back(kof(v));
    }

    printf("shards=%zu budget=%zu keys=%zu tolerance=%.2f\n", N, B, COUNT, TOL);
    const char* names[4] = {"-- 0: upwards only, one budget (as first defined)",
                            "-- 1: bidirectional, one budget",
                            "-- 2: bidirectional, cascade with a budget per level",
                            "-- 3: bidirectional, spend the budget on the worst shard"};
    const std::vector<std::pair<const char*, const std::vector<Key>*>> workloads{
        {"random", &random_keys}, {"ascending", &ascending},
        {"descending", &descending}, {"clustered", &clustered}};

    for (int bidi = 4; bidi < 5; ++bidi) {
        printf("%s\n", bidi==4 ? "-- 4: cascade, shedding to meet the neighbour half way" : names[bidi]);
        for (auto& [nm, ks] : workloads) {
            range_shards rs(N, B, TOL, bidi);
            run(nm, rs, *ks);
        }
    }

    // 5: the same shedding, but off the insert path. The insert path only counts, and
    // a sweep every `every` inserts does the work, standing in for the maintenance
    // thread barch already runs per key space. The question this answers is how far the
    // space drifts between sweeps - the end of run imbalance hides it, the peak does not
    for (int strat : {5, 6}) {
        for (size_t every : {16u, 256u, 4096u}) {
            printf("-- %d: sweep every %zu inserts, cascading from the largest shard until %s\n",
                   strat, every, strat == 5 ? "no shard is over the threshold"
                                            : "every shard is inside the band");
            for (auto& [nm, ks] : workloads) {
                range_shards rs(N, B, TOL, strat);
                rs.sweep_every = every;
                rs.sweep_passes = 64 * N;  // the cap that stops a sweep hogging the thread
                run(nm, rs, *ks);
            }
        }
    }
    return 0;
}
