//
// TODO 249: run a stored function on a schedule.
//
#include "cron.h"

#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <mutex>
#include <sstream>
#include <thread>

namespace barch {
namespace cron {

namespace {

std::string lower_copy(std::string s) {
    for (auto& c : s)
        c = (char) tolower((unsigned char) c);
    return s;
}

bool parse_uint(const std::string& s, size_t& at, uint64_t& out) {
    size_t start = at;
    while (at < s.size() && isdigit((unsigned char) s[at]))
        ++at;
    if (at == start)
        return false;
    out = strtoull(s.substr(start, at - start).c_str(), nullptr, 10);
    return true;
}

} // namespace

bool parse_duration(const std::string& s, uint64_t& ms, std::string& err) {
    ms = 0;
    if (s.empty()) {
        err = "an empty duration";
        return false;
    }
    size_t at = 0;
    bool any = false;
    while (at < s.size()) {
        uint64_t n = 0;
        if (!parse_uint(s, at, n)) {
            err = "'" + s + "' is not a duration";
            return false;
        }
        // the unit is "ms", or one letter - "ms" has to be tried first or the
        // "m" alone would swallow it and read "500ms" as five hundred minutes
        std::string unit;
        if (at + 1 < s.size() && s[at] == 'm' && s[at + 1] == 's') {
            unit = "ms";
            at += 2;
        } else if (at < s.size()) {
            unit = std::string(1, s[at]);
            ++at;
        } else {
            err = "'" + s + "' has a number with no unit";
            return false;
        }
        uint64_t factor;
        if (unit == "ms") factor = 1;
        else if (unit == "s") factor = 1000ull;
        else if (unit == "m") factor = 60ull * 1000;
        else if (unit == "h") factor = 60ull * 60 * 1000;
        else if (unit == "d") factor = 24ull * 60 * 60 * 1000;
        else {
            err = "'" + unit + "' is not a duration unit - use ms, s, m, h or d";
            return false;
        }
        ms += n * factor;
        any = true;
    }
    if (!any) {
        err = "'" + s + "' is not a duration";
        return false;
    }
    return true;
}

namespace {

const char* MONTH_NAMES[] = {"jan","feb","mar","apr","may","jun",
                             "jul","aug","sep","oct","nov","dec"};
const char* DOW_NAMES[] = {"sun","mon","tue","wed","thu","fri","sat"};

bool name_to_num(const std::string& tok, const char* const* names, int n, int base, int& out) {
    auto lo = lower_copy(tok);
    for (int i = 0; i < n; ++i) {
        if (lo.compare(0, 3, names[i]) == 0 && lo.size() <= 3) {
            out = base + i;
            return true;
        }
    }
    return false;
}

// one comma separated field: a run of a, a-b, a-b/c or a star, with an
// optional /step on any of those
bool parse_field(const std::string& field, int lo, int hi,
                 const char* const* names, int name_base, bool* out, size_t out_n,
                 std::string& err) {
    std::stringstream ss(field);
    std::string part;
    while (std::getline(ss, part, ',')) {
        if (part.empty()) {
            err = "an empty field in '" + field + "'";
            return false;
        }
        int step = 1;
        auto slash = part.find('/');
        std::string range = part;
        if (slash != std::string::npos) {
            range = part.substr(0, slash);
            std::string step_s = part.substr(slash + 1);
            size_t at = 0;
            uint64_t n = 0;
            if (!parse_uint(step_s, at, n) || at != step_s.size() || n == 0) {
                err = "'" + step_s + "' is not a step";
                return false;
            }
            step = (int) n;
        }
        int a, b;
        if (range == "*") {
            a = lo; b = hi;
        } else {
            auto dash = range.find('-');
            std::string a_s = dash == std::string::npos ? range : range.substr(0, dash);
            std::string b_s = dash == std::string::npos ? range : range.substr(dash + 1);
            // numbers first, names only where a table was given - months and
            // weekdays each have their own alphabet
            if (!a_s.empty() && isdigit((unsigned char) a_s[0])) {
                size_t at = 0; uint64_t n = 0;
                if (!parse_uint(a_s, at, n) || at != a_s.size()) {
                    err = "'" + a_s + "' is not a number";
                    return false;
                }
                a = (int) n;
            } else if (names && name_to_num(a_s, names, name_base == 0 ? 7 : 12, name_base, a)) {
                // matched
            } else {
                err = "'" + a_s + "' is not understood";
                return false;
            }
            if (!b_s.empty() && isdigit((unsigned char) b_s[0])) {
                size_t at = 0; uint64_t n = 0;
                if (!parse_uint(b_s, at, n) || at != b_s.size()) {
                    err = "'" + b_s + "' is not a number";
                    return false;
                }
                b = (int) n;
            } else if (names && name_to_num(b_s, names, name_base == 0 ? 7 : 12, name_base, b)) {
                // matched
            } else {
                err = "'" + b_s + "' is not understood";
                return false;
            }
        }
        // day-of-week's 7 means Sunday too, the usual crontab allowance
        if (name_base == 0 && a == 7) a = 0;
        if (name_base == 0 && b == 7) b = 0;
        if (a < lo || a > hi || b < lo || b > hi) {
            err = "'" + part + "' is out of range";
            return false;
        }
        if (a <= b) {
            for (int v = a; v <= b; v += step) {
                if ((size_t) v < out_n) out[v] = true;
            }
        } else {
            // a wrapping range, e.g. fri-mon on dow
            for (int v = a; v <= hi; v += step)
                if ((size_t) v < out_n) out[v] = true;
            for (int v = lo; v <= b; v += step)
                if ((size_t) v < out_n) out[v] = true;
        }
    }
    return true;
}

} // namespace

bool parse_expr(const std::string& raw, expr& out, std::string& err) {
    std::string s = raw;
    // trim
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char) s[a])) ++a;
    while (b > a && isspace((unsigned char) s[b - 1])) --b;
    s = s.substr(a, b - a);

    if (!s.empty() && s[0] == '@') {
        auto word = lower_copy(s);
        if (word == "@yearly" || word == "@annually") s = "0 0 1 1 *";
        else if (word == "@monthly") s = "0 0 1 * *";
        else if (word == "@weekly") s = "0 0 * * 0";
        else if (word == "@daily" || word == "@midnight") s = "0 0 * * *";
        else if (word == "@hourly") s = "0 * * * *";
        else {
            err = "'" + raw + "' is not a known @shorthand";
            return false;
        }
    }

    std::vector<std::string> fields;
    std::stringstream ss(s);
    std::string tok;
    while (ss >> tok)
        fields.push_back(tok);
    if (fields.size() != 5) {
        err = "a cron expression needs five fields: minute hour dom month dow";
        return false;
    }

    if (!parse_field(fields[0], 0, 59, nullptr, 0, out.minute, 60, err)) return false;
    if (!parse_field(fields[1], 0, 23, nullptr, 0, out.hour, 24, err)) return false;
    if (!parse_field(fields[2], 1, 31, nullptr, 0, out.dom, 32, err)) return false;
    if (!parse_field(fields[3], 1, 12, MONTH_NAMES, 1, out.month, 13, err)) return false;
    if (!parse_field(fields[4], 0, 7, DOW_NAMES, 0, out.dow, 7, err)) return false;

    out.dom_restricted = fields[2] != "*";
    out.dow_restricted = fields[4] != "*";
    return true;
}

std::chrono::system_clock::time_point next_after(const expr& e,
                                                  std::chrono::system_clock::time_point after) {
    using namespace std::chrono;
    // round up to the next whole minute
    time_t t = system_clock::to_time_t(after) + 60;
    t -= t % 60;
    // four years of minutes is the cap - long enough for "29 Feb" to find a leap
    // year, short enough that an impossible date (30 Feb) does not spin forever
    const time_t limit = t + (time_t) 4 * 366 * 24 * 60 * 60;
    while (t < limit) {
        struct tm g{};
        gmtime_r(&t, &g);
        bool dom_ok = e.dom[g.tm_mday];
        bool dow_ok = e.dow[g.tm_wday];
        // the classic OR: restricted on both means either is enough
        bool day_ok = (e.dom_restricted && e.dow_restricted) ? (dom_ok || dow_ok)
                                                              : (dom_ok && dow_ok);
        if (e.minute[g.tm_min] && e.hour[g.tm_hour] && e.month[g.tm_mon + 1] && day_ok)
            return system_clock::from_time_t(t);
        t += 60;
    }
    return system_clock::time_point::max();
}

namespace {

using steady = std::chrono::steady_clock;

/** what is known about one job between ticks, kept across a rescan by name */
struct job_state {
    bool running{false};
    std::string last_error;
    uint64_t run_count{0};
    steady::time_point due{};
    bool due_set{false};
};

/*
 * All three never destroyed, and reached through a pointer for exactly that reason -
 * the canonical form TODO 57 names.
 *
 * A host that never calls stop() - the python binding has no shutdown hook to hang
 * one on - reaches static destruction with the scheduler thread still parked in
 * cv.wait_for(). Destroying a condition variable that has a waiter is undefined, and
 * what glibc actually does is block in pthread_cond_destroy until the waiter leaves,
 * so a test process that had finished its work sat for minutes on the way out and
 * looked like a hang. Destroying the joinable thread itself is std::terminate. None
 * of it is reachable if the objects simply outlive the process.
 */
std::mutex& mu() {
    static auto* m = new std::mutex();
    return *m;
}
std::condition_variable& cv() {
    static auto* c = new std::condition_variable();
    return *c;
}
std::atomic<bool> running{false};
std::thread* worker{nullptr};

std::mutex& state_mu() {
    static auto* m = new std::mutex();
    return *m;
}
heap::string_map<job_state> states;
std::atomic<bool> kick{false};

job_state& state_of(const std::string& name) {
    return states[name];
}

/** the schedule half of a parsed entry - resolved once per rescan, not per tick */
struct resolved_job {
    barch::functions::cron_entry entry;
    bool use_every{false};
    uint64_t every_ms{0};
    expr calendar{};
    uint64_t jitter_ms{0};
};

bool resolve_schedule(const barch::functions::cron_entry& e, resolved_job& out, std::string& err) {
    out.entry = e;
    if (!e.spec.jitter.empty() && !parse_duration(e.spec.jitter, out.jitter_ms, err))
        return false;
    if (!e.spec.every.empty()) {
        out.use_every = true;
        if (!parse_duration(e.spec.every, out.every_ms, err))
            return false;
        if (out.every_ms == 0) {
            // an interval of nothing is a busy loop wearing a schedule
            err = "'" + e.spec.every + "' is not an interval";
            return false;
        }
        return true;
    }
    out.use_every = false;
    return parse_expr(e.spec.cron, out.calendar, err);
}

/*
 * A steady_clock due time computed from a system_clock schedule. The two clocks do
 * not agree on an origin, so this is the offset between "now" on each, applied once
 * per computation rather than assumed constant - a wall clock jump moves the next
 * calendar fire with it, the way a real cron would notice a jump too.
 */
steady::time_point due_from_wall(std::chrono::system_clock::time_point wall) {
    auto now_sys = std::chrono::system_clock::now();
    auto now_steady = steady::now();
    if (wall <= now_sys)
        return now_steady;
    return now_steady + std::chrono::duration_cast<steady::duration>(wall - now_sys);
}

/*
 * The unnamed space every plain connection writes to has no name to give, so a
 * job that means it says "default" - which is what `all_spaces` already calls it
 * when it reports one. Every other name is passed through untouched.
 */
std::string target_space(const std::string& declared) {
    return declared == "default" ? std::string{} : declared;
}

void run_job(const resolved_job& job) {
    auto& st = job.entry;
    std::string err;
    // this runs detached on a thread of its own, so an exception that escaped
    // would be an uncaught one and take the process down with it - the one
    // thing a scheduled job must not be able to do to an otherwise fine server
    try {
        barch::key_space_ptr space;
        auto target = target_space(st.spec.space);
        if (barch::is_keyspace(target))
            space = barch::get_keyspace(target);
        if (!space) {
            err = "target space '" + st.spec.space + "' is not loaded";
        } else {
            heap::vector<std::string> args;
            for (const auto& a : st.spec.args)
                args.push_back(a);
            Variable out;
            barch::functions::call_as(space, st.spec.user, st.spec.call, args, out, err);
        }
    } catch (const std::exception& e) {
        err = e.what();
    } catch (...) {
        err = "cron job threw something that was not a std::exception";
    }
    std::lock_guard<std::mutex> g(state_mu());
    auto& s = state_of(st.name);
    s.running = false;
    if (err.empty()) {
        s.last_error.clear();
        ++s.run_count;
    } else {
        s.last_error = err;
        barch::err({"cron", st.name, err});
    }
}

} // namespace

void request_rescan() {
    kick.store(true);
    cv().notify_all();
}

std::string status() {
    std::ostringstream o;
    auto jobs = barch::functions::cron_jobs();
    std::lock_guard<std::mutex> g(state_mu());
    bool first = true;
    for (const auto& e : jobs) {
        if (!first) o << "\n";
        first = false;
        auto& s = state_of(e.name);
        o << "name=" << e.name
          << " space=" << e.spec.space
          << " call=" << e.spec.call
          << " user=" << e.spec.user
          << " enabled=" << (e.spec.enabled ? "on" : "off")
          << " schedule=" << (e.spec.every.empty() ? e.spec.cron : e.spec.every)
          << " overlap=" << e.spec.overlap
          << " runs=" << s.run_count
          << " running=" << (s.running ? "yes" : "no");
        if (!e.parse_err.empty())
            o << " error=" << e.parse_err;
        else if (!s.last_error.empty())
            o << " last_error=" << s.last_error;
    }
    if (first)
        return "no cron jobs are configured";
    return o.str();
}

/*
 * One thread, a due time per job - the same shape start_function_sync uses for its
 * repositories. Node-local, skip rather than overlap, and a missed fire is dropped
 * rather than caught up: the first cut TODO 249 asks for, to see whether the shape
 * is right before anything distributed is built on top of it.
 */
void start() {
    if (running.exchange(true))
        return;
    worker = new std::thread([] {
        while (running.load()) {
            /*
             * Taken before the job list is read, not after. A rescan asked for while
             * this pass was already reading would otherwise be answered by the list
             * it read *before* the write - the kick consumed, the new job unseen, and
             * nothing to look at it again until the idle cap an entire minute later.
             * Clearing it first means such a kick survives into the next pass.
             */
            bool woke = kick.exchange(false);
            auto jobs = barch::functions::cron_jobs();
            auto now = steady::now();
            uint64_t wait_ms = 60000; // an idle cap, so a new job is not missed for long
            std::vector<resolved_job> due;
            {
                std::lock_guard<std::mutex> g(state_mu());
                for (auto& e : jobs) {
                    auto& s = state_of(e.name);
                    if (!e.parse_err.empty()) {
                        s.last_error = e.parse_err;
                        continue;
                    }
                    if (!e.spec.enabled)
                        continue;
                    resolved_job job;
                    std::string err;
                    if (!resolve_schedule(e, job, err)) {
                        s.last_error = err;
                        continue;
                    }
                    if (!s.due_set) {
                        // staggered rather than fired at once, the same reason the
                        // function sync watcher spreads its first runs out
                        uint64_t jitter = job.jitter_ms ? (rand() % (job.jitter_ms + 1)) : 0;
                        s.due = now + std::chrono::milliseconds(jitter);
                        s.due_set = true;
                    }
                    if (woke || now >= s.due) {
                        // overlap=skip and overlap=queue (no queue in this first
                        // cut, so it behaves as skip) both refuse a second run;
                        // overlap=allow starts one anyway
                        if (!s.running || job.entry.spec.overlap == "allow") {
                            due.push_back(job);
                            s.running = true;
                        }
                        // catchup is always false in this first cut - the next due
                        // time is always strictly after now, never after the fire
                        // that was missed while the server was down or busy
                        s.due = job.use_every
                            ? now + std::chrono::milliseconds(job.every_ms)
                            : due_from_wall(next_after(job.calendar, std::chrono::system_clock::now()));
                    }
                    // the wait has to account for the job that just fired as well
                    // as the ones that did not: otherwise a job on a 300ms interval
                    // fires and then the loop sleeps the idle cap before looking at
                    // it again, so `every` would mean "once a minute" for anything
                    // faster than the cap
                    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                        s.due - steady::now()).count();
                    if (left <= 0)
                        wait_ms = std::min<uint64_t>(wait_ms, 5);
                    else if ((uint64_t) left < wait_ms)
                        wait_ms = (uint64_t) left;
                }
            }
            for (const auto& job : due) {
                if (!running.load())
                    break;
                std::thread([job] { run_job(job); }).detach();
            }
            if (!running.load())
                break;
            {
                std::unique_lock<std::mutex> lk(mu());
                cv().wait_for(lk, std::chrono::milliseconds(wait_ms), [] {
                    return !running.load() || kick.load();
                });
            }
        }
    });
}

void stop() {
    if (!running.exchange(false))
        return;
    cv().notify_all();
    if (worker && worker->joinable())
        worker->join();
    delete worker;
    worker = nullptr;
}

}
}
