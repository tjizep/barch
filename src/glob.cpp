//
// Created by linuxlite on 3/22/25.
//

#include "glob.h"
#include <ctype.h>
#include <iostream>
#include <ostream>
// if there's only asterisks in the glob (a very common pattern)
static int asterisk_impl(const char *pattern,
                              int patternLen,
                              const char *string,
                              int stringLen,
                              int nocase,
                              int *skipLongerMatches,
                              int nesting) {
    /* Protection against abusive patterns. */
    if (nesting > 1000) return 0;

    while (patternLen && stringLen) {
        switch (pattern[0]) {
            case '*':
                while (patternLen && pattern[1] == '*') { // TODO: ?BUG?  assumes pattern[1] exists if patternLen == 1
                    pattern++;
                    patternLen--;
                }
                if (patternLen == 1) return 1; /* match */
                if (nesting == 0 && patternLen > 4){
                    auto asterisk = (const char *)memchr(pattern+1, '*', patternLen-1);
                    auto q = memchr(pattern+1, '?', patternLen-1);
                    auto slash = memchr(pattern+1, '\\', patternLen-1);
                    if (!q && !slash &&
                        (
                            !asterisk // there'r no further asterisks
                            || (asterisk - pattern) > 3
                        ) // or its at least a few characters away
                    ) {
                        _memchr_section:
                        // this method works but its weakness is that pattern[1] can be a very frequent character
                        auto str = (const char *)memchr(string, pattern[1], stringLen); // we would really like to choose the least frequent char in the pattern
                        if (!str) {
                            return 0;
                        }
                        // TODO: further opts are possible
                        stringLen -= (str - string); // we can now quickly advance the string pointer
                        string = str;
                        if (stringLen > 3 && pattern[2] != string[1]) { // pattern len > 4 and asterisk - patterm > 3
                            // we can try again
                            string++;
                            stringLen--;
                            goto _memchr_section; // this opt adds a few percentage points
                        }
                    }
                }
                while (stringLen) {
                    if (asterisk_impl(pattern + 1, patternLen - 1, string, stringLen, nocase, skipLongerMatches,
                                            nesting + 1))
                        return 1; /* match */
                    if (*skipLongerMatches) return 0; /* no match */
                    string++;
                    stringLen--;


                }
            /* There was no match for the rest of the pattern starting
             * from anywhere in the rest of the string. If there were
             * any '*' earlier in the pattern, we can terminate the
             * search early without trying to match them to longer
             * substrings. This is because a longer match for the
             * earlier part of the pattern would require the rest of the
             * pattern to match starting later in the string, and we
             * have just determined that there is no match for the rest
             * of the pattern starting from anywhere in the current
             * string. */
                *skipLongerMatches = 1;
                return 0; /* no match */
                break;

            case '?': // TODO: this is slow
                string++;
                stringLen--;
                break;
            case '\\':
                if (patternLen >= 2) {
                    pattern++;
                    patternLen--;
                }
                /* fall through */
            default:
                if (!nocase) {
                    if (pattern[0] != string[0]) return 0; /* no match */
                } else {
                    if (tolower((int) pattern[0]) != tolower((int) string[0])) return 0; /* no match */
                }
                string++;
                stringLen--;
                break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0) {
            while (*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringLen == 0) return 1;
    return 0;
}

int glob::stringmatchlen_impl(const char *pattern,
                              int patternLen,
                              const char *string,
                              int stringLen,
                              int nocase,
                              int *skipLongerMatches,
                              int nesting) {
    /* Protection against abusive patterns. */
    if (nesting > 1000) return 0;

    while (patternLen && stringLen) {
        switch (pattern[0]) {
            case '*':
                while (patternLen && pattern[1] == '*') {
                    pattern++;
                    patternLen--;
                }
                if (patternLen == 1) return 1; /* match */
                while (stringLen) {
                    if (stringmatchlen_impl(pattern + 1, patternLen - 1, string, stringLen, nocase, skipLongerMatches,
                                            nesting + 1))
                        return 1; /* match */
                    if (*skipLongerMatches) return 0; /* no match */
                    string++;
                    stringLen--;
                }
            /* There was no match for the rest of the pattern starting
             * from anywhere in the rest of the string. If there were
             * any '*' earlier in the pattern, we can terminate the
             * search early without trying to match them to longer
             * substrings. This is because a longer match for the
             * earlier part of the pattern would require the rest of the
             * pattern to match starting later in the string, and we
             * have just determined that there is no match for the rest
             * of the pattern starting from anywhere in the current
             * string. */
                *skipLongerMatches = 1;
                return 0; /* no match */
                break;
            case '?': // this is slow
                string++;
                stringLen--;
                break;
            case '[': {
                int not_op, match;

                pattern++;
                patternLen--;
                not_op = pattern[0] == '^';
                if (not_op) {
                    pattern++;
                    patternLen--;
                }
                match = 0;
                while (1) {
                    if (pattern[0] == '\\' && patternLen >= 2) {
                        pattern++;
                        patternLen--;
                        if (pattern[0] == string[0]) match = 1;
                    } else if (pattern[0] == ']') {
                        break;
                    } else if (patternLen == 0) {
                        pattern--;
                        patternLen++;
                        break;
                    } else if (patternLen >= 3 && pattern[1] == '-') {
                        int start = pattern[0];
                        int end = pattern[2];
                        int c = string[0];
                        if (start > end) {
                            int t = start;
                            start = end;
                            end = t;
                        }
                        if (nocase) {
                            start = tolower(start);
                            end = tolower(end);
                            c = tolower(c);
                        }
                        pattern += 2;
                        patternLen -= 2;
                        if (c >= start && c <= end) match = 1;
                    } else {
                        if (!nocase) {
                            if (pattern[0] == string[0]) match = 1;
                        } else {
                            if (tolower((int) pattern[0]) == tolower((int) string[0])) match = 1;
                        }
                    }
                    pattern++;
                    patternLen--;
                }
                if (not_op) match = !match;
                if (!match) return 0; /* no match */
                string++;
                stringLen--;
                break;
            }
            case '\\':
                if (patternLen >= 2) {
                    pattern++;
                    patternLen--;
                }
            /* fall through */
            default:
                if (!nocase) {
                    if (pattern[0] != string[0]) return 0; /* no match */
                } else {
                    if (tolower((int) pattern[0]) != tolower((int) string[0])) return 0; /* no match */
                }
                string++;
                stringLen--;
                break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0) {
            while (*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringLen == 0) return 1;
    return 0;
}
static bool star_only(art::value_type patter) {
    for (unsigned i = 0; i < patter.size; i++) {
        switch (patter.bytes[i]) {
            case '*':
            case '?':
            case '\\':
                continue;
            case '[':
            // case ']': ?
                return false;
            default:
                continue;
        }
    }
    return true;
}
int glob::stringmatchlen(art::value_type pattern, art::value_type string, int nocase) {
    int skipLongerMatches = 0;
    if (star_only(pattern))
        return asterisk_impl(pattern.chars(), pattern.size, string.chars(), string.size, nocase,
                              &skipLongerMatches, 0);

    return stringmatchlen_impl(pattern.chars(), pattern.size, string.chars(), string.size, nocase,
                               &skipLongerMatches, 0);
}
