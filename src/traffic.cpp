//
// See traffic.h for the file layout, the per thread rule, and what is
// deliberately not recorded.
//
#include "traffic.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <vector>

#include "configuration.h"
#include "lzr_log.h"

namespace barch::traffic {
    namespace {
        constexpr const char* file_magic = "barch-traffic-file-1\n";
        /** what a secret argument is written as instead of itself - TODO 328 */
        constexpr const char* redacted = "<redacted>";
        /*
         * A megabyte of stdio buffer per thread, so a record costs a memcpy and a
         * syscall happens once every few thousand commands. One fwrite per record
         * rather than one per field: fwrite takes the FILE's own lock, so a whole
         * record written in one call cannot interleave, and two calls could. With
         * a file per thread that lock is never contended - but it is what
         * capture_changed borrows to flush a file its owner is still writing, so
         * it still matters that it exists.
         */
        constexpr size_t buffer_bytes = 1024 * 1024;
        /*
         * How many records a thread writes before it adds its bytes to the shared
         * total. The total is only read to compare against traffic_max_bytes, and
         * a shared counter touched per record is exactly the cache line ping this
         * change exists to remove.
         */
        constexpr uint64_t batch_records = 64;

        std::atomic<uint64_t> writes{0};
        std::atomic<uint64_t> drops{0};
        std::atomic<uint64_t> written{0};
        /** what `written` stood at when capture was last turned on or repointed */
        std::atomic<uint64_t> baseline{0};
        std::atomic<uint64_t> next_index{0};
        /*
         * Bumped whenever capture is turned off or pointed somewhere else. A
         * thread compares it against what it opened with and reopens when they
         * differ, which is how a file set change reaches threads that are not
         * asking about the configuration on every record.
         */
        std::atomic<uint64_t> generation{0};

        struct sink;
        std::mutex registry_lock{};
        std::vector<sink*> registry{};

        /** one thread's file */
        struct sink {
            FILE* f{nullptr};
            std::string path{};
            uint64_t gen{0};
            /*
             * This thread's number in the file set, taken once and kept. Taken
             * per open instead, a recording stopped and started would rename
             * every file and leave the old ones beside the new - eight threads
             * over ten bursts would be eighty files rather than eight.
             */
            uint64_t index{0};
            bool have_index{false};
            uint64_t pending{0};        // bytes not yet added to `written`
            uint64_t since_batch{0};
            bool listed{false};

            void publish() {
                if (!pending) return;
                written.fetch_add(pending, std::memory_order_relaxed);
                pending = 0;
                since_batch = 0;
            }
            /*
             * `f` is opened and closed under registry_lock, and only ever by the
             * thread that owns this sink. That is what makes capture_changed safe:
             * it holds the same lock while it flushes, so it cannot be looking at
             * a FILE that is being closed underneath it. The owner reads `f` on
             * the write path without the lock, which is its own field.
             */
            void close_locked() {
                if (!f) return;
                publish();
                std::fclose(f);
                f = nullptr;
                path.clear();
            }
            void close() {
                std::unique_lock l(registry_lock);
                close_locked();
            }
            ~sink() {
                std::unique_lock l(registry_lock);
                close_locked();
                if (listed) {
                    for (auto it = registry.begin(); it != registry.end(); ++it) {
                        if (*it == this) {
                            registry.erase(it);
                            break;
                        }
                    }
                    listed = false;
                }
            }
        };
        thread_local sink me{};

        uint64_t wall_nanos() {
            return (uint64_t) std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }

        /** `barch_traffic.dat` and 3 -> `barch_traffic.3.dat` */
        std::string file_for(const std::string& base, uint64_t index) {
            const size_t dot = base.find_last_of('.');
            const size_t slash = base.find_last_of('/');
            const bool has_suffix = dot != std::string::npos
                                    && (slash == std::string::npos || dot > slash);
            const std::string n = std::to_string(index);
            if (!has_suffix)
                return base + "." + n;
            return base.substr(0, dot) + "." + n + base.substr(dot);
        }

        /** this thread's file, opening it if it has not got one yet */
        FILE* target() {
            const uint64_t gen = generation.load(std::memory_order_acquire);
            if (me.f && me.gen == gen)
                return me.f;
            const std::string base = barch::get_traffic_file();
            // everything that opens or closes a file holds the registry lock, so
            // that capture_changed never flushes one that is going away. Once per
            // thread per generation, so the cost of holding it here is nothing
            std::unique_lock l(registry_lock);
            me.close_locked();                // a stale generation, or nothing yet
            if (base.empty())
                return nullptr;
            if (!me.have_index) {
                me.index = next_index.fetch_add(1, std::memory_order_relaxed);
                me.have_index = true;
            }
            const std::string want = file_for(base, me.index);
            const bool fresh = [&] {
                FILE* probe = std::fopen(want.c_str(), "rb");
                if (!probe) return true;
                std::fclose(probe);
                return false;
            }();
            // append, because a recording is meant to be taken in bursts and a
            // second burst should not silently lose the first
            FILE* opened = std::fopen(want.c_str(), "ab");
            if (!opened) {
                barch::err({"traffic capture cannot open", want});
                return nullptr;
            }
            std::setvbuf(opened, nullptr, _IOFBF, buffer_bytes);
            /*
             * What is already in the file is not counted - TODO 328.
             *
             * It used to be, so that appending to a recording counted what was in
             * it. That double counted: the bytes this process appended were
             * published once when they were written and again as file size on the
             * next reopen, so the cap tripped early. `traffic_max_bytes` is now
             * plainly "how much this recording may write", which is also the
             * question an operator is asking.
             */
            if (fresh) {
                std::fwrite(file_magic, 1, strlen(file_magic), opened);
                written.fetch_add(strlen(file_magic), std::memory_order_relaxed);
            }
            me.f = opened;
            me.path = want;
            me.gen = gen;
            if (!me.listed) {
                registry.push_back(&me);
                me.listed = true;
            }
            barch::log({"traffic capture writing to", want});
            return me.f;
        }

        /** decimal, straight onto the end, without std::to_string's allocation */
        void append_uint(std::string& out, uint64_t v) {
            char tmp[20];
            char* end = tmp + sizeof(tmp);
            char* p = end;
            do {
                *--p = (char) ('0' + (v % 10));
                v /= 10;
            } while (v);
            out.append(p, (size_t) (end - p));
        }

        /**
         * The command name as the filters below have to see it.
         *
         * Two things arrive in `args[0]` that are not the name: the case the
         * client happened to use, and a `<space>:` prefix. The dispatcher strips
         * the prefix into a copy of its own and leaves `params[0]` alone, so a
         * filter that reads `params[0]` sees `shop:CONFIG` - eleven characters
         * where a bare CONFIG is six. Filtering on that missed every prefixed
         * form, and a replay reissues a prefixed name unchanged, so
         * `shop:CONFIG SET traffic_capture on` was recorded and would have
         * reconfigured the target. See TODO 328.
         */
        std::string bare_name(std::string_view raw) {
            const size_t colon = raw.find_last_of(':');
            if (colon != std::string_view::npos && colon + 1 < raw.size())
                raw = raw.substr(colon + 1);
            std::string out;
            out.reserve(raw.size());
            for (char c : raw) out.push_back((char) toupper((unsigned char) c));
            return out;
        }

        /** is this command one we refuse to record - see the note in traffic.h */
        bool skipped(const std::string& name) {
            return name == "CONFIG";
        }

        /**
         * Commands whose arguments are secrets, and how many of the leading ones
         * are not - see TODO 328.
         *
         * The recorder runs before authorization, so it sees every `AUTH` that
         * arrives whether or not it was accepted. Recording those verbatim put
         * plaintext passwords in a file that is meant to be copied around and
         * replayed, which is the last place they should be. The command is still
         * recorded - a replay that silently dropped the AUTH would produce a
         * session that is not the one that ran - with the secret arguments
         * replaced by a marker, so what comes back is honest about what it is.
         *
         * `keep` is how many arguments after the name survive: HELLO's protocol
         * version and the `AUTH` word are structure, the rest is the credential.
         */
        struct secret_command {
            const char* name;
            size_t keep;
        };
        constexpr secret_command secret_commands[] = {
            {"HELLO", 1},       // HELLO <ver> [AUTH <user> <password>] - keep <ver>
            {"ACL", 2},         // ACL SETUSER <name> >password ... - keep both words
        };
        /**
         * How many arguments to keep, or npos when nothing here is a secret.
         *
         * AUTH depends on its arity, which is why it is not in the table: the one
         * argument form *is* the password, while in the two argument form the
         * first is a username and worth keeping - a recording that says who tried
         * to authenticate is more use than one that says somebody did.
         */
        size_t secret_keep(const std::string& name, size_t argc) {
            if (name == "AUTH")
                return argc >= 3 ? 1u : 0u;
            for (const auto& c : secret_commands) {
                if (name == c.name) return c.keep;
            }
            return std::string::npos;
        }
    }

    bool capturing() {
        return barch::get_traffic_capture();
    }

    uint64_t recorded() { return writes.load(std::memory_order_relaxed); }
    uint64_t dropped() { return drops.load(std::memory_order_relaxed); }
    uint64_t bytes_written() { return written.load(std::memory_order_relaxed); }

    uint64_t files() {
        std::unique_lock l(registry_lock);
        return registry.size();
    }

    void capture_changed() {
        /*
         * Flush every thread's file, under the FILE's own lock so that a flush
         * cannot land in the middle of an fwrite the owner is doing. Not closed:
         * a file belongs to its writer, and fclose here would free a FILE another
         * thread is about to use. The generation bump tells each owner to close
         * its own at its next record.
         */
        /*
         * The cap counts from here, and `written` is never reset - see TODO 328.
         *
         * It used to be `written.store(0)`, which races with the writers on
         * purpose: they add their bytes without the registry lock, so an add
         * that lands after the store is simply lost, and the cap then measures a
         * recording smaller than the one on disk. Taking a baseline instead
         * means every write only ever adds, and what the cap compares is
         * `written - baseline`. The baseline is set before the generation bump so
         * that a thread which reopens because the generation changed cannot read
         * a baseline from before its own bytes were counted.
         */
        baseline.store(written.load(std::memory_order_relaxed), std::memory_order_relaxed);
        generation.fetch_add(1, std::memory_order_release);
        std::unique_lock l(registry_lock);
        for (sink* s : registry) {
            if (!s->f) continue;
            flockfile(s->f);
            std::fflush(s->f);
            funlockfile(s->f);
        }
    }

    void record(uint64_t conn, std::string_view space, const std::vector<std::string_view>& args) {
        if (args.empty()) return;
        // the name without its `<space>:` prefix and folded, which is what both
        // filters below have to see - TODO 328
        const std::string name = bare_name(args[0]);
        if (skipped(name)) return;
        const size_t keep = secret_keep(name, args.size());
        try {
            FILE* f = target();
            if (!f) {
                if (drops.fetch_add(1, std::memory_order_relaxed) == 0)
                    barch::err({"traffic capture has nowhere to write"});
                return;
            }
            const uint64_t max_bytes = barch::get_traffic_max_bytes();
            // what this recording has written, which is everything since the
            // baseline capture_changed left behind
            const uint64_t since = written.load(std::memory_order_relaxed)
                                   - baseline.load(std::memory_order_relaxed) + me.pending;
            if (max_bytes && since >= max_bytes) {
                // stop rather than fill the disk. Said once, because it stays true
                if (drops.fetch_add(1, std::memory_order_relaxed) == 0)
                    barch::err({"traffic capture stopped at traffic_max_bytes",
                                std::to_string(max_bytes)});
                return;
            }

            /*
             * Built in a buffer this thread keeps, so a record costs a memcpy and
             * no allocation once the buffer has grown. The length goes in front of
             * the body, and the body's length is only known once it is built, so
             * the first `header_room` bytes are left empty and the digits written
             * backwards into them - which keeps the whole record contiguous, and
             * the record has to be contiguous to go out in one fwrite.
             */
            static thread_local std::string buf;
            constexpr size_t header_room = 24;
            buf.clear();
            buf.resize(header_room);
            append_uint(buf, wall_nanos());
            buf.push_back('\n');
            append_uint(buf, conn);
            buf.push_back('\n');
            buf.append(space);
            buf.push_back('\n');
            append_uint(buf, args.size());
            buf.push_back('\n');
            size_t arg_at = 0;
            for (const auto& a : args) {
                /*
                 * A credential is recorded as a marker, not as itself - see the
                 * note on `secret_commands` and TODO 328. The argument is still
                 * there, so a reader can see that an AUTH happened and a replay
                 * can reissue one; what is not there is the password.
                 */
                const bool secret = keep != std::string::npos && arg_at > keep;
                const std::string_view out = secret ? std::string_view(redacted) : a;
                append_uint(buf, out.size());
                buf.push_back('\n');
                buf.append(out);
                buf.push_back('\n');
                ++arg_at;
            }

            char* p = buf.data() + header_room;
            *--p = '\n';
            uint64_t body = buf.size() - header_room;
            do {
                *--p = (char) ('0' + (body % 10));
                body /= 10;
            } while (body);
            const size_t at = (size_t) (p - buf.data());

            // one call, so the record stays whole - and on this thread's own file,
            // so the lock it takes is never held by anybody else
            const size_t bytes = buf.size() - at;
            const size_t n = std::fwrite(buf.data() + at, 1, bytes, f);
            if (n != bytes) {
                if (drops.fetch_add(1, std::memory_order_relaxed) == 0)
                    barch::err({"traffic capture short write", me.path});
                return;
            }
            me.pending += n;
            writes.fetch_add(1, std::memory_order_relaxed);
            if (++me.since_batch >= batch_records)
                me.publish();
        } catch (std::exception& e) {
            // out of memory, anything: the command being recorded still has to
            // run. Said once rather than per command, because whatever this is
            // will be true for the next one too
            if (drops.fetch_add(1, std::memory_order_relaxed) == 0)
                barch::err({"traffic capture dropping records", e.what()});
        }
    }
}
