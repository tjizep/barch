//
// What barchd says when it crashes - TODO 646. See crash_report.h.
//
// Everything here can run with the process half broken: a signal handler, or a
// terminate with the heap in any state. So it writes with write(2) from a fixed
// buffer, formats numbers itself, and allocates nothing. Except in the terminate
// handler, which asks the exception for its what(); nothing else would say what
// went wrong.
//
#include "crash_report.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <exception>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#  define BARCH_CRASH_SANITIZED 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#    define BARCH_CRASH_SANITIZED 1
#  endif
#endif

#if !defined(_WIN32) && defined(__GLIBC__)
#  include <execinfo.h>
#  define BARCH_CRASH_BACKTRACE 1
#endif

namespace barch {
    namespace {
        // ---- writing without allocating ---------------------------------------------
        struct line {
            char buf[512];
            size_t n = 0;
            line& text(const char* s) {
                while (s && *s && n < sizeof(buf) - 1) buf[n++] = *s++;
                return *this;
            }
            line& hex(uint64_t v) {
                char tmp[16];
                int k = 0;
                do {
                    tmp[k++] = "0123456789abcdef"[v & 15];
                    v >>= 4;
                } while (v && k < 16);
                text("0x");
                while (k > 0 && n < sizeof(buf) - 1) buf[n++] = tmp[--k];
                return *this;
            }
            line& num(int64_t v) {
                char tmp[24];
                int k = 0;
                const bool neg = v < 0;
                uint64_t u = neg ? 0 - (uint64_t) v : (uint64_t) v;
                do {
                    tmp[k++] = (char) ('0' + u % 10);
                    u /= 10;
                } while (u && k < 23);
                if (neg) text("-");
                while (k > 0 && n < sizeof(buf) - 1) buf[n++] = tmp[--k];
                return *this;
            }
            void out() {
                buf[n++] = '\n';
                size_t done = 0;
                while (done < n) {
                    const auto w = ::write(2, buf + done, (unsigned) (n - done));
                    if (w <= 0) break;
                    done += (size_t) w;
                }
                n = 0;
            }
        };

#ifdef _WIN32
        // a frame as <module>+<offset>, which addr2line can look up in that build
        void where(line& l, const void* addr) {
            HMODULE mod = nullptr;
            char name[MAX_PATH] = {};
            if (GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                                   GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                                   (LPCSTR) addr, &mod) && mod &&
                GetModuleFileNameA(mod, name, sizeof(name))) {
                const char* base = name;
                for (const char* p = name; *p; ++p)
                    if (*p == '\\' || *p == '/') base = p + 1;
                l.text(base).text("+").hex((uint64_t) ((const char*) addr - (const char*) mod));
            } else {
                l.hex((uint64_t) (uintptr_t) addr);
            }
        }

        void stack(const char* why) {
            void* frames[48];
            const USHORT n = CaptureStackBackTrace(0, 48, frames, nullptr);
            line l;
            l.text("barchd crash: ").text(why).text(", stack (").num(n).text(" frames):");
            l.out();
            for (USHORT i = 0; i < n; ++i) {
                l.text("  #").num(i).text(" ");
                where(l, frames[i]);
                l.out();
            }
        }
#else
        void stack(const char* why) {
            line l;
#  ifdef BARCH_CRASH_BACKTRACE
            void* frames[48];
            const int n = backtrace(frames, 48);
            l.text("barchd crash: ").text(why).text(", stack (").num(n).text(" frames):");
            l.out();
            backtrace_symbols_fd(frames, n, 2);
#  else
            l.text("barchd crash: ").text(why).text(" (no stack on this platform)");
            l.out();
#  endif
        }
#endif

        // set once a report is written: the abort that ends a terminate, or a second
        // crash while one is being reported, then only finishes the job
        std::atomic<bool> reported{false};

        // ---- std::terminate ---------------------------------------------------------
        [[noreturn]] void on_terminate() {
            line l;
            if (reported.exchange(true)) {
                l.text("barchd crash: terminate again while reporting one");
                l.out();
                std::abort();
            }
            l.text("barchd crash: terminate");
            if (auto e = std::current_exception()) {
                try {
                    std::rethrow_exception(e);
                } catch (const std::exception& x) {
                    l.text(" with an exception nothing caught: ").text(x.what());
                } catch (...) {
                    l.text(" with an exception nothing caught, not a std::exception");
                }
            } else {
                l.text(" with no exception (a noexcept function threw, or a joinable "
                       "std::thread was destroyed)");
            }
            l.out();
            stack("terminate");
            std::abort();
        }

#ifdef _WIN32
        // ---- an unhandled Windows exception: access violation and the like ----------
        const char* code_name(DWORD c) {
            switch (c) {
                case EXCEPTION_ACCESS_VIOLATION: return "access violation";
                case EXCEPTION_STACK_OVERFLOW: return "stack overflow";
                case EXCEPTION_ILLEGAL_INSTRUCTION: return "illegal instruction";
                case EXCEPTION_INT_DIVIDE_BY_ZERO: return "integer divide by zero";
                case EXCEPTION_ARRAY_BOUNDS_EXCEEDED: return "array bounds exceeded";
                case EXCEPTION_IN_PAGE_ERROR: return "in page error";
                case EXCEPTION_PRIV_INSTRUCTION: return "privileged instruction";
                case 0xC0000409: return "stack buffer overrun / fail fast";
                default: return "exception";
            }
        }

        // MinGW throws a C++ exception as an SEH exception with this code (" GCC"). When
        // nothing catches it, it ends up in this filter. The CRT's own filter, which this
        // one replaces, continues it, so RaiseException returns and libgcc calls
        // std::terminate, and on_terminate reports it with its what(). Do the same here.
        constexpr DWORD gcc_throw = 0x20474343;

        LONG WINAPI on_exception(EXCEPTION_POINTERS* info) {
            const auto* rec = info ? info->ExceptionRecord : nullptr;
            if (rec && (rec->ExceptionCode & 0x20ffffff) == gcc_throw &&
                !(rec->ExceptionFlags & EXCEPTION_NONCONTINUABLE))
                return EXCEPTION_CONTINUE_EXECUTION;
            if (reported.exchange(true))
                return EXCEPTION_CONTINUE_SEARCH;
            line l;
            l.text("barchd crash: ");
            if (rec) {
                l.text(code_name(rec->ExceptionCode)).text(" (code ").hex(rec->ExceptionCode).text(") at ");
                where(l, rec->ExceptionAddress);
                if (rec->ExceptionCode == EXCEPTION_ACCESS_VIOLATION && rec->NumberParameters >= 2)
                    l.text(rec->ExceptionInformation[0] == 1 ? ", writing " :
                           rec->ExceptionInformation[0] == 8 ? ", executing " : ", reading ")
                     .hex((uint64_t) rec->ExceptionInformation[1]);
            } else {
                l.text("unhandled exception, no record");
            }
            l.text(", thread ").num((int64_t) GetCurrentThreadId());
            l.out();
            stack("unhandled exception");
            return EXCEPTION_CONTINUE_SEARCH;
        }

        void on_abort(int) {
            if (reported.exchange(true))
                std::_Exit(3);                  // a terminate already said why
            line l;
            l.text("barchd crash: abort, thread ").num((int64_t) GetCurrentThreadId());
            l.out();
            stack("abort");
            std::signal(SIGABRT, SIG_DFL);
            std::_Exit(3);
        }
#elif !defined(BARCH_CRASH_SANITIZED)
        // ---- a fatal signal ---------------------------------------------------------
        void on_signal(int sig) {
            if (reported.exchange(true)) {
                std::signal(sig, SIG_DFL);      // a terminate already said why
                raise(sig);
                return;
            }
            line l;
            l.text("barchd crash: signal ").num(sig).text(" (").text(strsignal(sig)).text(")");
            l.out();
            stack("signal");
            // the default action, so the exit status and any core say what it was
            std::signal(sig, SIG_DFL);
            raise(sig);
        }
#endif
    }

    void install_crash_report() {
        std::set_terminate(on_terminate);
#ifdef _WIN32
        SetUnhandledExceptionFilter(on_exception);
        std::signal(SIGABRT, on_abort);
#elif !defined(BARCH_CRASH_SANITIZED)
        for (int sig : {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT}) {
            struct sigaction sa{};
            sa.sa_handler = on_signal;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = SA_RESETHAND;
            sigaction(sig, &sa, nullptr);
        }
#endif
    }

    void crash_if_asked() {
        const char* what = std::getenv("BARCH_TEST_CRASH");
        if (!what) return;
        if (std::strcmp(what, "terminate") == 0) {
            std::thread([] {
                throw std::runtime_error("BARCH_TEST_CRASH asked for it");
            }).join();
        } else if (std::strcmp(what, "segv") == 0) {
            volatile int* nowhere = nullptr;
            *nowhere = 1;
        }
    }
}
