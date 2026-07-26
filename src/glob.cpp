//
// Created by linuxlite on 3/22/25.
//

#include "glob.h"
#include <ctype.h>
#include <iostream>
#include <ostream>
// if there's only asterisks in the glob (a very common pattern)
template<int N>
inline bool no_token(const char* pattern) {
    for (int i = 0; i < N; i++) {
        if (pattern[i] == '\\'|| pattern[i] == '?'|| pattern[i] == '*') {
            return false;
        }
    }
    return true;
}
// the shortcuts below look for a literal run of the pattern inside the string. memchr
// and memmem compare bytes, so when case folding is asked for they would skip over a
// perfectly good match - these fold both sides first. the case sensitive path is left
// on memchr/memmem so the common case costs nothing.
static inline bool fold_eq(char a, char b, int nocase) {
    if (!nocase) return a == b;
    return tolower((unsigned char) a) == tolower((unsigned char) b);
}
static const char *fold_chr(const char *s, char c, size_t n, int nocase) {
    if (!nocase) return (const char *) memchr(s, c, n);
    for (size_t i = 0; i < n; i++) {
        if (fold_eq(s[i], c, 1)) return s + i;
    }
    return nullptr;
}
static const char *fold_mem(const char *s, size_t n, const char *needle, size_t m, int nocase) {
    if (!nocase) return (const char *) memmem(s, n, needle, m);
    if (m > n) return nullptr;
    for (size_t i = 0; i + m <= n; i++) {
        size_t j = 0;
        while (j < m && fold_eq(s[i + j], needle[j], 1)) j++;
        if (j == m) return s + i;
    }
    return nullptr;
}
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
                while (patternLen > 1 && pattern[1] == '*') { // patternLen > 1 so pattern[1] is inside the pattern
                    pattern++;
                    patternLen--;
                }
                if (patternLen == 1) return 1; /* match */
                if (nesting == 0 && patternLen > 4){
                    auto asterisk = (const char *)memchr(pattern+1, '*', patternLen-1);
                    // '*?' means the same as '?*', so a '?' sitting just after the star
                    // can be lifted out of the way - but only by consuming the character
                    // it stands for, otherwise the minimum length it imposes is lost.
                    // pattern[0] is left as a placeholder for the star: everything below
                    // only ever looks at pattern+1.
                    while (pattern[1]=='?' && patternLen > 4 && stringLen > 0) {
                        ++pattern;
                        --patternLen;
                        ++string;
                        --stringLen;
                    }
                    if (stringLen == 0) {
                        // the '?'s took the rest of the string with them, so all that can
                        // still match is a run of stars - the same tail the main loop applies
                        const char *rest = pattern + 1;
                        int restLen = patternLen - 1;
                        while (restLen && *rest == '*') {
                            ++rest;
                            --restLen;
                        }
                        return restLen == 0;
                    }
                    auto slash = memchr(pattern+1, '\\', patternLen-1);
                    if (!slash) {
                        if (patternLen > 4 && no_token<4>(pattern+1)) { // were hoping the 4 chars is enough to find a unique sequence far away
                            auto str = fold_mem(string, stringLen, pattern+1, 4, nocase); // we would really like to choose the least frequent char in the pattern
                            if (!str) {
                                return 0;
                            }
                            stringLen -= (str - string); // we can now quickly advance the string pointer
                            string = str;
                        }else
                        if (pattern[1] != '?' &&
                        (
                            !asterisk // there'r no further asterisks
                            || (asterisk - pattern) > 3
                        ) // or its at least a few characters away
                        ) {
                            _memchr_section:
                            // this method works but its weakness is that pattern[1] can be a very frequent character
                            auto str = fold_chr(string, pattern[1], stringLen, nocase); // we would really like to choose the least frequent char in the pattern
                            if (!str) {
                                return 0;
                            }
                            // TODO: further opts are possible
                            stringLen -= (str - string); // we can now quickly advance the string pointer
                            string = str;
                            if (stringLen > 3 && pattern[2] != '?' && !fold_eq(pattern[2], string[1], nocase)) { // pattern len > 4 and asterisk - patterm > 3
                                // we can try again
                                string++;
                                stringLen--;
                                goto _memchr_section; // this opt adds a few percentage points
                            }
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
            while (patternLen && *pattern == '*') { // the pattern can already be spent here
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
                while (patternLen > 1 && pattern[1] == '*') { // patternLen > 1 so pattern[1] is inside the pattern
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
            while (patternLen && *pattern == '*') { // the pattern can already be spent here
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
