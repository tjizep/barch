#ifndef BARCH_PAIR_SHAPE_CORE_H
#define BARCH_PAIR_SHAPE_CORE_H
/*
 * How a reply lays out member-score and field-value pairs - TODO 948.
 *
 * Under RESP3, Redis 7 answers a list of pairs as an array of two-element arrays:
 * ZRANGE WITHSCORES, ZRANDMEMBER and HRANDFIELD with a count, ZUNION, ZINTER and
 * ZDIFF WITHSCORES, and ZPOPMIN and ZPOPMAX with a count. Under RESP2 the same
 * reply is flat. A pop without a count answers one pair, and that stays flat
 * under both.
 *
 * The commands call open_pair and close_pair around each pair, through the
 * caller's start_pair and end_pair. Both take the same layout, so an array is
 * only closed where one was opened.
 *
 * This header includes nothing, and TestCoreBoundary fails if it starts to.
 * test/pairshapetest.cpp runs every protocol, list or lone pair, and count of
 * pairs through it.
 */

namespace barch::pair_shape_core {
    enum class layout {
        flat,   // member, score, member, score
        nested  // [member, score], [member, score]
    };

    /** a list of pairs nests each one under RESP3; a lone pair never does */
    inline layout pairs(int protocol, bool list) {
        return protocol >= 3 && list ? layout::nested : layout::flat;
    }

    /** before a pair's two values */
    template<typename Builder>
    void open_pair(Builder& b, layout l) {
        if (l == layout::nested)
            b.start_array();
    }

    /** after them */
    template<typename Builder>
    void close_pair(Builder& b, layout l) {
        if (l == layout::nested)
            b.end_array();
    }
}

#endif //BARCH_PAIR_SHAPE_CORE_H
