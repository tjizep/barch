#ifndef BARCH_SCAN_PAGE_CORE_H
#define BARCH_SCAN_PAGE_CORE_H
/*
 * One page of a container scan, and where the next one starts - TODO 951.
 *
 * SCAN's promise: a full iteration reports every element that is there throughout
 * it. Repeats are allowed. COUNT says how much work a call does.
 *
 * HSCAN and ZSCAN walk keys in order from a saved key. A page takes up to COUNT
 * live entries. The first live entry it does not take is the saved key, and the
 * next page starts at it, inclusive. So nothing between two pages is skipped.
 *
 * The order must not move an element while it stays. A hash walks field names.
 * ZSCAN walks the member index, ordered by name, and not the score keys. A score
 * change moves a score key, and one moved behind the saved key would be missed.
 *
 * This header includes nothing, and TestCoreBoundary fails if it starts to.
 * test/scanpagetest.cpp runs every short interleaving of changes through it.
 */

namespace barch::scan_page_core {
    enum class step {
        take,   // report this entry
        resume  // the page is full: save this key, and start the next page at it
    };

    struct page {
        long long count;
        long long taken = 0;

        explicit page(long long count) : count(count) {}

        /** at each live entry, in walk order */
        step at() {
            if (taken >= count) return step::resume;
            ++taken;
            return step::take;
        }
    };

    /** the next page starts at the saved key itself, not after it */
    constexpr bool resume_inclusive = true;
}

#endif //BARCH_SCAN_PAGE_CORE_H
