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
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#  define BARCH_CRASH_SANITIZED 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#    define BARCH_CRASH_SANITIZED 1
#  endif
#endif

#if !defined(_WIN32) && defined(__GLIBC__)
#  include <dlfcn.h>
#  include <execinfo.h>
#  include <link.h>
#  define BARCH_CRASH_BACKTRACE 1
#endif

#if defined(BARCH_CRASH_BACKTRACE) && defined(__linux__)
#  include <filesystem>
#  include <sys/syscall.h>
#  include "debuggable_server_lock.h"
#  define BARCH_THREAD_DUMP 1
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
#  ifdef BARCH_CRASH_BACKTRACE
        // barchd's own load address and path, read once at install: dladdr isn't
        // safe in a signal handler, and backtrace_symbols_fd, which calls it,
        // stopped after the handler's own frames in a real crash - TODO 644
        uintptr_t self_base = 0;
        uintptr_t self_end = 0;
        char self_name[256] = {};
#  endif
        void stack(const char* why) {
            line l;
#  ifdef BARCH_CRASH_BACKTRACE
            void* frames[48];
            const int n = backtrace(frames, 48);
            l.text("barchd crash: ").text(why).text(", stack (").num(n).text(" frames, ")
             .text(self_name).text(" at ").hex(self_base).text("):");
            l.out();
            for (int i = 0; i < n; ++i) {
                const auto a = (uintptr_t) frames[i];
                l.text("  #").num(i).text(" ").hex(a);
                if (self_base && a >= self_base && a < self_end)
                    l.text(" (barchd+").hex(a - self_base).text(")");
                l.out();
            }
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
#else
#  ifdef BARCH_CRASH_BACKTRACE
        Dl_info info{};
        if (dladdr((void*) &install_crash_report, &info) && info.dli_fbase) {
            self_base = (uintptr_t) info.dli_fbase;
            if (info.dli_fname)
                std::strncpy(self_name, info.dli_fname, sizeof(self_name) - 1);
        }
        // where barchd's own segments end, so a frame in a library isn't called barchd's
        dl_iterate_phdr([](dl_phdr_info* pi, size_t, void*) -> int {
            if (pi->dlpi_addr != self_base) return 0;
            for (int i = 0; i < pi->dlpi_phnum; ++i)
                if (pi->dlpi_phdr[i].p_type == PT_LOAD) {
                    const uintptr_t end = pi->dlpi_addr + pi->dlpi_phdr[i].p_vaddr + pi->dlpi_phdr[i].p_memsz;
                    if (end > self_end) self_end = end;
                }
            return 1;
        }, nullptr);
        void* warm[1];
        backtrace(warm, 1);         // loads libgcc now, not in a handler
#  endif
#  ifndef BARCH_CRASH_SANITIZED
        for (int sig : {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT}) {
            struct sigaction sa{};
            sa.sa_handler = on_signal;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = SA_RESETHAND;
            sigaction(sig, &sa, nullptr);
        }
#  endif
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
#ifdef BARCH_THREAD_DUMP
        else if (std::strcmp(what, "dump") == 0) {
            // a latch timeout while barchd runs - TODO 741. One thread holds a latch
            // for good, another gives up waiting on it and dumps. barchd goes on.
            static debuggable_server_lock latch;
            std::thread([] {
                latch.lock();
                while (true) std::this_thread::sleep_for(std::chrono::hours(1));
            }).detach();
            std::thread([] {
                std::this_thread::sleep_for(std::chrono::seconds(3));
                if (!latch.try_lock_shared_for(std::chrono::milliseconds(100)))
                    dump_threads("BARCH_TEST_CRASH asked for a latch timeout", &latch,
                                 &debuggable_server_lock::held_by_this_thread);
            }).detach();
        }
#endif
    }

    // ---- every thread's stack, when a latch wait times out - TODO 741 --------------
#ifdef BARCH_THREAD_DUMP
    namespace {
        constexpr int dump_frames = 40;
        constexpr int dump_held = 16;

        // one thread's answer. The dumper asks one thread at a time: it puts that
        // thread's tid in `claim`, signals it, and the handler swaps the tid for -1
        // before writing, so a late answer to an ask the dumper gave up on writes nothing
        struct answer {
            std::atomic<int> claim{0};
            std::atomic<bool> done{false};
            void* frames[dump_frames];
            int nframes = 0;
            const void* lks[dump_held];
            char modes[dump_held];
            int nheld = 0;
        } asked;
        std::atomic<held_latches_fn> asked_held{nullptr};

        int dump_signal() { return SIGRTMIN + 3; }
        int gettid_() { return (int) syscall(SYS_gettid); }

        void on_dump(int) {
            const int saved = errno;
            int me = gettid_();
            if (asked.claim.compare_exchange_strong(me, -1)) {
                asked.nframes = backtrace(asked.frames, dump_frames);
                auto h = asked_held.load();
                asked.nheld = h ? h(asked.lks, asked.modes, dump_held) : 0;
                asked.done.store(true, std::memory_order_release);
            }
            errno = saved;
        }

        struct thread_seen {
            int tid = 0;
            std::string name;
            bool answered = false;
            bool blocked = false;
            std::vector<void*> frames;
            std::vector<std::pair<const void*, char>> held;
            int nheld = 0;
        };

        // a thread that blocks the signal would only time out the ask
        bool blocks_dump_signal(int tid) {
            FILE* f = std::fopen(("/proc/self/task/" + std::to_string(tid) + "/status").c_str(), "r");
            if (!f) return false;
            bool blocked = false;
            char b[256];
            while (std::fgets(b, sizeof(b), f))
                if (std::strncmp(b, "SigBlk:", 7) == 0) {
                    const unsigned long long mask = std::strtoull(b + 7, nullptr, 16);
                    blocked = (mask >> (dump_signal() - 1)) & 1;
                    break;
                }
            std::fclose(f);
            return blocked;
        }

        void put(const std::string& s) {
            size_t done = 0;
            while (done < s.size()) {
                const auto w = ::write(2, s.data() + done, s.size() - done);
                if (w <= 0) break;
                done += (size_t) w;
            }
        }

        std::string hexs(uintptr_t v) {
            char b[24];
            std::snprintf(b, sizeof(b), "0x%lx", (unsigned long) v);
            return b;
        }

        bool in_self(uintptr_t a) { return self_base && self_end && a >= self_base && a < self_end; }

        // a return address points after the call, so look up the byte before it
        uintptr_t lookup_addr(const thread_seen& t, size_t i) {
            const auto a = (uintptr_t) t.frames[i];
            return i > 0 && a ? a - 1 : a;
        }

        // addr2line over every barchd frame of every thread at once, since loading
        // the debug info is the slow part. One block of lines per address, in order;
        // empty when there's no addr2line
        std::vector<std::string> symbolize(const std::vector<thread_seen>& threads) {
            // by pid: in addr2line, /proc/self is addr2line
            std::string cmd = "addr2line -C -f -i -p -a -e /proc/" + std::to_string(getpid()) + "/exe";
            size_t n = 0;
            for (const auto& t : threads)
                for (size_t i = 0; i < t.frames.size(); ++i)
                    if (in_self(lookup_addr(t, i))) {
                        cmd += " " + hexs(lookup_addr(t, i) - self_base);
                        ++n;
                    }
            std::vector<std::string> blocks;
            if (n == 0) return blocks;
            cmd += " 2>/dev/null";
            FILE* f = popen(cmd.c_str(), "r");
            if (!f) return blocks;
            char buf[4096];
            while (std::fgets(buf, sizeof(buf), f)) {
                std::string ln(buf);
                if (ln.rfind("0x", 0) == 0) {
                    // "0x...: func at file:line", the address dropped
                    auto c = ln.find(": ");
                    blocks.push_back(c == std::string::npos ? ln : ln.substr(c + 2));
                } else if (!blocks.empty()) {
                    blocks.back() += "        " + ln;   // " (inlined by) ..."
                }
            }
            pclose(f);
            if (blocks.size() != n) blocks.clear();     // can't line them up
            return blocks;
        }

        std::string frame_text(uintptr_t a) {
            std::string s = hexs(a);
            if (in_self(a)) return s + " barchd+" + hexs(a - self_base) + "\n";
            Dl_info info{};
            if (dladdr((void*) a, &info) && info.dli_fname) {
                const char* base = info.dli_fname;
                for (const char* p = info.dli_fname; *p; ++p)
                    if (*p == '/') base = p + 1;
                s += std::string(" ") + base;
                if (info.dli_sname)
                    s += std::string(" ") + info.dli_sname + "+" + hexs(a - (uintptr_t) info.dli_saddr);
                else if (info.dli_fbase)
                    s += "+" + hexs(a - (uintptr_t) info.dli_fbase);
            }
            return s + "\n";
        }
    }

    void dump_threads(const char* why, const void* latch, held_latches_fn held) {
        static std::mutex one;
        static std::atomic<int64_t> last{0};
        std::unique_lock<std::mutex> only(one, std::try_to_lock);
        if (!only.owns_lock()) return;      // another waiter is writing one now
        const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        const int me = gettid_();
        if (last.load() && now - last.load() < 30000) {
            put(std::string("barchd thread dump: ") + why + " on thread " + std::to_string(me) +
                ", latch " + hexs((uintptr_t) latch) + "; skipped, one was written " +
                std::to_string((now - last.load()) / 1000) + " s ago\n");
            return;
        }
        last = now;

        static std::once_flag installed;
        std::call_once(installed, [] {
            struct sigaction sa{};
            sa.sa_handler = on_dump;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = SA_RESTART;
            sigaction(dump_signal(), &sa, nullptr);
        });
        asked_held = held;

        std::vector<int> tids;
        // std::filesystem, not dirent.h: something in barchd's precompiled header
        // is already called DIR
        std::error_code ec;
        for (const auto& e : std::filesystem::directory_iterator("/proc/self/task", ec))
            tids.push_back(std::atoi(e.path().filename().c_str()));

        const pid_t pid = getpid();
        std::vector<thread_seen> threads;
        for (int tid : tids) {
            thread_seen t;
            t.tid = tid;
            if (FILE* f = std::fopen(("/proc/self/task/" + std::to_string(tid) + "/comm").c_str(), "r")) {
                char b[64] = {};
                if (std::fgets(b, sizeof(b), f)) {
                    t.name = b;
                    if (!t.name.empty() && t.name.back() == '\n') t.name.pop_back();
                }
                std::fclose(f);
            }
            if (tid != me && blocks_dump_signal(tid)) {
                t.blocked = true;
                threads.push_back(std::move(t));
                continue;
            }
            asked.done = false;
            asked.claim = tid;
            if (tid == me) {
                on_dump(0);
            } else if (syscall(SYS_tgkill, pid, tid, dump_signal()) != 0) {
                asked.claim = 0;
                continue;                   // gone already
            }
            // a thread in a blocking call answers at once; under TSan one that is
            // running answers at its next call into the runtime
            auto give_up = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (!asked.done.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < give_up)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (!asked.done.load(std::memory_order_acquire)) {
                int expect = tid;
                if (!asked.claim.compare_exchange_strong(expect, 0)) {
                    // it took the ask just now, so it is writing; let it finish
                    while (!asked.done.load(std::memory_order_acquire))
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            if (asked.done.load(std::memory_order_acquire)) {
                t.answered = true;
                t.frames.assign(asked.frames, asked.frames + asked.nframes);
                t.nheld = asked.nheld;
                for (int i = 0; i < asked.nheld && i < dump_held; ++i)
                    t.held.emplace_back(asked.lks[i], asked.modes[i]);
            }
            threads.push_back(std::move(t));
        }
        asked.claim = 0;

        const auto syms = symbolize(threads);
        size_t sym = 0;
        std::vector<std::string> holders;
        for (const auto& t : threads)
            for (const auto& [lk, mode] : t.held)
                if (lk == latch)
                    holders.push_back(std::to_string(t.tid) + " (" + mode + ")");

        std::string out = "\n==== barchd thread dump: " + std::string(why) + " on thread " +
            std::to_string(me) + ", latch " + hexs((uintptr_t) latch) + ", " +
            std::to_string(threads.size()) + " threads\n";
        out += "held by: ";
        if (holders.empty()) out += "no thread lists it (a hold past the first " +
                                    std::to_string(dump_held) + " isn't shown)";
        for (size_t i = 0; i < holders.size(); ++i) out += (i ? ", " : "") + holders[i];
        out += "\n";
        put(out);
        for (const auto& t : threads) {
            out = "\n-- thread " + std::to_string(t.tid) + " (" + t.name + ")";
            if (t.tid == me) out += " the waiter";
            if (!t.answered) {
                put(out + (t.blocked ? ": blocks the dump signal\n" : ": didn't answer\n"));
                continue;
            }
            out += "\n   holds:";
            if (t.nheld == 0) out += " nothing";
            for (const auto& [lk, mode] : t.held)
                out += std::string(" ") + mode + " " + hexs((uintptr_t) lk) + (lk == latch ? " <- THIS" : "");
            if (t.nheld > (int) t.held.size())
                out += " and " + std::to_string(t.nheld - (int) t.held.size()) + " more";
            out += "\n";
            for (size_t i = 0; i < t.frames.size(); ++i) {
                const auto a = lookup_addr(t, i);
                out += "   #" + std::to_string(i) + " ";
                if (in_self(a) && !syms.empty())
                    out += syms[sym++];
                else
                    out += frame_text((uintptr_t) t.frames[i]);
                if (!out.empty() && out.back() != '\n') out += "\n";
            }
            put(out);
        }
        put("==== end of thread dump\n");
    }
#else
    void dump_threads(const char* why, const void* latch, held_latches_fn) {
        line l;
        l.text("barchd thread dump: ").text(why).text(", latch ").hex((uint64_t) (uintptr_t) latch)
         .text(" (no thread dump on this platform)");
        l.out();
    }
#endif
}
