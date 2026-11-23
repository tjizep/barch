// getBufferAt copies once - TODO 961.
//
// A Luau function runs the real binding over a store of this test's own. The store
// reads Luau's byte count around the callback it was handed. This checks:
//   - the buffer is made inside the callback, so there's no copy in between. That
//     is seen for values over 1024 bytes: smaller blocks share Luau's pages, and
//     need not move the count;
//   - the collector frees nothing in there, and runs after;
//   - the store's bytes can change once it returns, and the buffer keeps its own;
//   - tombed is barch.tomb and absent is nil;
//   - a store that throws after the callback is an error the script can catch;
//   - a space handle's getBufferAt does the same;
//   - a store with no cursors refuses one by name - TODO 962.
#include "foreign/driver.h"
#include "sastam.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <future>
#include <stdexcept>
#include <string>
#include <vector>

using namespace barch::foreign;
using read_state = store_access::read_state;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static size_t reads = 0;
static size_t big_reads = 0;
static size_t made_inside = 0;
static uint64_t most_used = 0;
static std::vector<char> leaf(100000);
static const size_t shared_pages = 1024; // Luau's kMaxSmallSize

static read_state get_buffer_at(const std::string& key, size_t offset,
                                const std::function<void(const void*, size_t)>& cb) {
    if (key == "gone")
        return read_state::tombed;
    if (key == "none")
        return read_state::absent;
    const bool late = key == "late";
    const size_t n = late ? 64 : std::stoul(key);
    if (offset > n)
        return read_state::absent;
    std::memset(leaf.data(), 'v', n);
    const uint64_t before = heap::luau_allocated;
    cb(leaf.data() + offset, n - offset);
    const uint64_t after = heap::luau_allocated;
    if (late)
        throw std::runtime_error("refused after the read");
    ++reads;
    if (n - offset > shared_pages) {
        ++big_reads;
        // less than the buffer is no buffer, or a collection in here
        if (after >= before + (n - offset))
            ++made_inside;
    }
    most_used = std::max(most_used, after);
    // what the buffer mustn't see
    std::memset(leaf.data(), 'x', n);
    return read_state::present;
}

static const char* const script = R"(
function call()
    local sp = barch.current()
    local function both(k, off)
        return barch.store.getBufferAt(k, off), sp:getBufferAt(k, off)
    end
    for _, n in {0, 1, 7, 1000, 4095, 4096, 7090, 100000} do
        for _, off in {0, math.min(n, 3)} do
            local want = string.rep("v", n - off)
            local a, b = both(tostring(n), off)
            assert(buffer.tostring(a) == want, "store bytes at " .. n)
            assert(buffer.tostring(b) == want, "handle bytes at " .. n)
        end
    end
    local a, b = both("gone", 0)
    assert(a == barch.tomb and b == barch.tomb, "tombed")
    a, b = both("none", 0)
    assert(a == nil and b == nil, "absent")
    assert(select("#", barch.store.getBufferAt("none")) == 1, "absent is one value")
    a, b = both("9", 10)
    assert(a == nil and b == nil, "past the end")
    local ok, err = pcall(barch.store.getBufferAt, "late")
    assert(not ok and string.find(tostring(err), "refused after the read"), "late refusal")
    ok = pcall(function() return sp:getBufferAt("late") end)
    assert(not ok, "late refusal on the handle")
    ok, err = pcall(barch.store.cursor, "a", "b")
    assert(not ok and err == "FUNCTION cursor is not available here", "no cursors")
    ok, err = pcall(function() return sp:cursor("a", "b") end)
    assert(not ok and err == "FUNCTION cursor is not available here", "no cursors on the handle")
    -- 320 MB in all, dropped as it's read
    for i = 1, 5000 do
        local held = barch.store.getBufferAt("65536")
        assert(buffer.readu8(held, 65535) == 118, "bytes in the loop")
    end
    return "ok"
end
)";

int main() {
    auto iface = std::make_shared<call_interface>();
    iface->load = [](const std::string&, const std::string&, bool, std::string& source) {
        source = script;
        return true;
    };
    iface->store.may_read = true;
    iface->store.getBufferAt = get_buffer_at;
    iface->open_space = [&store = iface->store](const std::string&, store_access& out) {
        out = store;
        return true;
    };
    iface->running_in = iface->defined_in = "t";

    call_limits limits;
    limits.inline_only = true;
    std::promise<std::pair<bool, std::string>> answered;
    const uint64_t at_start = heap::luau_allocated;
    start_function("t", "f", iface, {}, limits, nullptr,
                   [&](bool ok, Variable, std::string err) { answered.set_value({ok, err}); });
    const auto [ok, err] = answered.get_future().get();
    if (!ok)
        std::printf("%s\n", err.c_str());
    check(ok, "each answer is the right one, on the store and on a handle");
    check(reads > 5000 && big_reads > 5000, "the store was read");
    std::printf("reads over 1024 bytes %zu, buffers made inside the callback %zu, peak %llu bytes\n",
                big_reads, made_inside,
                (unsigned long long) (most_used - std::min(most_used, at_start)));
    check(made_inside == big_reads, "each buffer is made inside the callback, with no collection");
    check(most_used < at_start + (64u << 20), "the steps owed run: the heap stays small");
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
