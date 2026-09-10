//
// TODO 249: run a stored function on a schedule.
//
#ifndef BARCH_CRON_H
#define BARCH_CRON_H
#include <string>
#include <chrono>
#include <cstdint>

namespace barch {
namespace cron {

    /**
     * `<n><unit>` repeated - `5m`, `90s`, `2h30m` - units `ms s m h d`. Covers
     * "every so often" with no calendar and no timezone to be wrong about.
     * Empty or malformed fills `err` and answers false.
     */
    bool parse_duration(const std::string& s, uint64_t& ms, std::string& err);

    /**
     * The classic five field expression - minute hour dom month dow - with
     * `*`, `,`, `-`, `/`, names for month and weekday, and the `@hourly` /
     * `@daily` / `@weekly` / `@monthly` / `@yearly` shorthands. Deliberately not
     * the six field variant with seconds, and no `@reboot` - see TODO 249.
     *
     * Day-of-month and day-of-week are OR'd when both are restricted, as every
     * crontab since the seventies has done.
     */
    struct expr {
        bool minute[60]{};
        bool hour[24]{};
        bool dom[32]{};    // 1..31, index 0 unused
        bool month[13]{};  // 1..12, index 0 unused
        bool dow[7]{};     // 0..6, 0 is Sunday
        bool dom_restricted{false};
        bool dow_restricted{false};
    };
    bool parse_expr(const std::string& s, expr& out, std::string& err);

    /**
     * The first whole UTC minute strictly after `after` that `e` matches.
     * Minute resolution, walked forward one minute at a time up to a few years
     * out - cheap next to how rarely a schedule actually needs recomputing.
     * Answers `after` unchanged (time_point::max) if nothing matches within the
     * search window, which a schedule that names an impossible date can do -
     * 30 February, for instance.
     */
    std::chrono::system_clock::time_point next_after(
        const expr& e, std::chrono::system_clock::time_point after);

    /** one line per job, in the shape FUNCTIONS CRON reports it */
    std::string status();

    void start();
    void stop();
    /** wake the scheduler early - a job was just written, removed, or asked for */
    void request_rescan();

}
}

#endif //BARCH_CRON_H
