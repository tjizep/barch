// Where a range sweep starts its cascade - TODO 421.
//
// CI had range_index::sweep stuck at [1682, 1682, 951, 1087, 1119, 1144, 1162, 1173]:
// 1.35x against a 1.25 tolerance, for good. It started from the first largest shard,
// shard 0, whose only neighbour held the same, so nothing moved and the sweep gave up.
// These are plain sizes run through the sweep's cascade rule. It meets a neighbour
// half way, picking the smaller one, and stops at one within a key. That's enough to
// show the old start stalls and shed_start doesn't, without shards or timing.
#include "range_balance.h"

#include <algorithm>
#include <cstdio>
#include <vector>

static int failures = 0;

static void check(bool ok, const char* what) {
    if (!ok) {
        std::printf("FAIL %s\n", what);
        ++failures;
    }
}

static double ratio(const std::vector<size_t>& s) {
    size_t total = 0;
    for (size_t v : s) total += v;
    double avg = (double) total / (double) s.size();
    return (double) *std::max_element(s.begin(), s.end()) / avg;
}

// range_index::sweep over sizes: the same balance test, the same cascade. `fixed`
// picks the start with shed_start; otherwise it's the first largest, as it was
static std::vector<size_t> sweep(std::vector<size_t> s, bool fixed, double tolerance = 1.25) {
    const size_t n = s.size();
    auto size_at = [&](size_t i) { return s[i]; };
    for (int pass = 0; pass < 1000; ++pass) {
        size_t total = 0, cur = 0, least = 0;
        for (size_t i = 0; i < n; ++i) {
            total += s[i];
            if (s[i] > s[cur]) cur = i;
            if (s[i] < s[least]) least = i;
        }
        double average = (double) total / (double) n;
        bool over = (double) s[cur] > tolerance * average;
        bool under = (double) s[least] < average / tolerance;
        if (!over && !under) return s;
        if (fixed) {
            cur = barch::shed_start(n, size_at);
            if (cur == n) return s;
        }
        bool progressed = false;
        for (size_t step = 0; step < n; ++step) {
            bool up_ok = cur + 1 < n, down_ok = cur > 0;
            if (!up_ok && !down_ok) break;
            bool up = up_ok && (!down_ok || s[cur + 1] <= s[cur - 1]);
            size_t other = up ? cur + 1 : cur - 1;
            if (s[cur] <= s[other] + 1) break;
            size_t take = (s[cur] - s[other]) / 2;
            if (take == 0) break;
            s[cur] -= take;
            s[other] += take;
            progressed = true;
            cur = other;
        }
        if (!progressed) return s;
    }
    return s;
}

int main() {
    auto at = [](const std::vector<size_t>& v) {
        return [&v](size_t i) { return v[i]; };
    };
    const std::vector<size_t> ci{1682, 1682, 951, 1087, 1119, 1144, 1162, 1173};

    // where it starts
    check(barch::shed_start(ci.size(), at(ci)) == 1, "the CI sizes start at shard 1, which can shed into 2");
    std::vector<size_t> flat{5, 5, 5, 5};
    check(barch::shed_start(flat.size(), at(flat)) == flat.size(), "equal shards: nowhere to start");
    std::vector<size_t> near{6, 5, 6, 5};
    check(barch::shed_start(near.size(), at(near)) == near.size(), "within a key everywhere: nowhere to start");
    std::vector<size_t> top{10, 10, 10, 2};
    check(barch::shed_start(top.size(), at(top)) == 2, "a plateau sheds from its edge");
    std::vector<size_t> step{10, 9, 1};
    check(barch::shed_start(step.size(), at(step)) == 1, "the largest can't shed, the next one can");
    std::vector<size_t> two{3, 10, 10, 3};
    check(barch::shed_start(two.size(), at(two)) == 1, "a tie between shards that can both shed takes the first");
    std::vector<size_t> one{7};
    check(barch::shed_start(one.size(), at(one)) == 1, "one shard: nowhere to start");

    // the stall, and the way out of it
    auto old_end = sweep(ci, false);
    check(old_end == ci, "the old start moves nothing from the CI sizes");
    check(ratio(old_end) > 1.30, "and stays over what the test allows");
    auto new_end = sweep(ci, true);
    std::printf("CI sizes: old %.2fx, fixed %.2fx\n", ratio(old_end), ratio(new_end));
    check(ratio(new_end) <= 1.25, "shed_start gets the CI sizes within the tolerance");
    size_t before = 0, after = 0;
    for (size_t v : ci) before += v;
    for (size_t v : new_end) after += v;
    check(before == after, "no key is lost or made up on the way");

    // more ties at the ends and in the middle, from both sides
    for (auto sizes : std::vector<std::vector<size_t>>{
             {1682, 1682, 951, 1087, 1119, 1144, 1162, 1173},
             {951, 1087, 1119, 1144, 1162, 1173, 1682, 1682},
             {900, 2000, 2000, 2000, 900, 900},
             {3000, 3000, 3000, 10, 10, 10},
             {10, 10, 3000, 3000}}) {
        auto end = sweep(sizes, true);
        if (ratio(end) > 1.25) {
            std::printf("  from [%zu ...] reached %.2fx\n", sizes[0], ratio(end));
            check(false, "the fixed sweep balances every tie it is given");
        }
    }

    if (failures) {
        std::printf("range balance test FAILED: %d\n", failures);
        return 1;
    }
    std::printf("range balance test ok\n");
    return 0;
}
