#ifndef BARCH_SCORE_MOVE_CORE_H
#define BARCH_SCORE_MOVE_CORE_H
/*
 * Whether a score change moves a member's key - TODO 950.
 *
 * A sorted set member's score key holds its encoded score and its name. ZINCRBY
 * moves a member by writing the new score key, pointing the member index at it,
 * and removing the old key. When the new score encodes the same as the old one,
 * the new key is the old key. The write does nothing, and the remove takes the
 * only copy. An increment of 0, or one too small to change the double, did that.
 *
 * So a move only replaces the keys when they differ. Otherwise it keeps them.
 *
 * This header includes nothing, and TestCoreBoundary fails if it starts to.
 * test/scoremovetest.cpp runs every short run of increments through it.
 */

namespace barch::score_move_core {
    enum class step {
        keep,    // the same key: write nothing, remove nothing
        replace  // write the new key, point the index at it, remove the old
    };

    /** compares the encoded keys, not the doubles: 0 and -0 can encode apart */
    template<typename Key>
    step plan(const Key& old_key, const Key& new_key) {
        return old_key == new_key ? step::keep : step::replace;
    }
}

#endif //BARCH_SCORE_MOVE_CORE_H
