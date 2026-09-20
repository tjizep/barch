//
// Copyright (C) 2010 Square, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// A modified derivative work: Square's Tape QueueFile translated to C++ for
// barch. See queue_file.h and TODO 351 for what was changed and why.
//
#include "queue_file.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <cstdio>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace queue_file_anon {
    /** the leading bit says "versioned", and the version is 1 */
    constexpr uint32_t versioned_header = 0x80000001u;

    [[noreturn]] void qf_fail(const std::string& what, const std::string& path) {
        throw std::runtime_error(what + " [" + path + "]: " + std::strerror(errno));
    }
    [[noreturn]] void qf_fail_plain(const std::string& what) {
        throw std::runtime_error(what);
    }

    /*
     * Big endian on disk, byte by byte, because that is what the Java writes and
     * the point of the port is that either can read the other's files. Not
     * htobe64: this has to be exact, not merely consistent.
     */
    void qf_put_u32(uint8_t* b, uint32_t v) {
        b[0] = (uint8_t) (v >> 24); b[1] = (uint8_t) (v >> 16);
        b[2] = (uint8_t) (v >> 8);  b[3] = (uint8_t) v;
    }
    uint32_t qf_get_u32(const uint8_t* b) {
        return ((uint32_t) b[0] << 24) | ((uint32_t) b[1] << 16)
             | ((uint32_t) b[2] << 8)  | (uint32_t) b[3];
    }
    void qf_put_u64(uint8_t* b, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            b[i] = (uint8_t) (v >> (56 - 8 * i));
    }
    uint64_t qf_get_u64(const uint8_t* b) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v = (v << 8) | (uint64_t) b[i];
        return v;
    }
} // namespace queue_file_anon

using namespace queue_file_anon;

namespace barch {

    queue_file::queue_file(const std::string& path, sync_policy sync, bool zero_removed,
                           bool force_legacy)
        : path(path), policy(sync), zero_removed(zero_removed) {
        // only `each_add` needs the file itself to be synchronous; the others
        // sync when they are told to, or when enough has piled up - TODO 352
        const int sync_flag = policy.when == sync_when::each_add ? O_DSYNC : 0;

        /*
         * A new file is built under a temporary name and renamed into place, so
         * an interrupted creation cannot leave a half-written header that the
         * next open would try to read. The rename is the atomic step.
         */
        if (::access(path.c_str(), F_OK) != 0) {
            const std::string tmp = path + ".tmp";
            const int t = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC | sync_flag, 0644);
            if (t < 0)
                qf_fail("could not create the queue file", tmp);
            uint8_t head[32]{};
            uint32_t head_len = 0;
            if (force_legacy) {
                qf_put_u32(head, (uint32_t) initial_length);   // leading bit clear: legacy
                head_len = 16;
            } else {
                qf_put_u32(head, versioned_header);
                qf_put_u64(head + 4, initial_length);
                head_len = 32;
            }
            bool ok = ::ftruncate(t, (off_t) initial_length) == 0
                      && ::pwrite(t, head, head_len, 0) == (ssize_t) head_len
                      && ::fsync(t) == 0;
            ::close(t);
            if (!ok) {
                ::unlink(tmp.c_str());
                qf_fail("could not initialise the queue file", tmp);
            }
            if (::rename(tmp.c_str(), path.c_str()) != 0) {
                ::unlink(tmp.c_str());
                qf_fail("could not rename the new queue file into place", tmp);
            }
        }

        fd = ::open(path.c_str(), O_RDWR | sync_flag);
        if (fd < 0)
            qf_fail("could not open the queue file", path);

        uint8_t buffer[32]{};
        read_at(0, buffer, 32);

        versioned = !force_legacy && (buffer[0] & 0x80) != 0;
        uint64_t first_offset = 0;
        uint64_t last_offset = 0;
        if (versioned) {
            header_length = 32;
            const uint32_t version = qf_get_u32(buffer) & 0x7FFFFFFFu;
            if (version != 1) {
                ::close(fd);
                fd = -1;
                qf_fail_plain("cannot read queue file version " + std::to_string(version)
                           + " [" + path + "]: only version 1 and legacy are understood");
            }
            file_length = qf_get_u64(buffer + 4);
            element_count = qf_get_u32(buffer + 12);
            first_offset = qf_get_u64(buffer + 16);
            last_offset = qf_get_u64(buffer + 24);
        } else {
            header_length = 16;
            file_length = qf_get_u32(buffer);
            element_count = qf_get_u32(buffer + 4);
            first_offset = qf_get_u32(buffer + 8);
            last_offset = qf_get_u32(buffer + 12);
        }

        /*
         * Two sanity checks that turn a corrupt file into a refusal rather than
         * into wild reads. The length in the header is the authority on how big
         * the ring is, so a file shorter than it says has lost its tail, and one
         * that claims to be smaller than its own header is nonsense.
         */
        struct stat st{};
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            fd = -1;
            qf_fail("could not stat the queue file", path);
        }
        if (file_length > (uint64_t) st.st_size) {
            const auto actual = std::to_string((uint64_t) st.st_size);
            ::close(fd);
            fd = -1;
            qf_fail_plain("queue file is truncated [" + path + "]: header says "
                       + std::to_string(file_length) + " bytes, the file is " + actual);
        }
        if (file_length <= header_length) {
            const auto claimed = std::to_string(file_length);
            ::close(fd);
            fd = -1;
            qf_fail_plain("queue file is corrupt [" + path + "]: the length in its header ("
                       + claimed + ") is not larger than the header");
        }

        first = read_element(first_offset);
        last = read_element(last_offset);
    }

    queue_file::~queue_file() {
        if (fd >= 0)
            ::close(fd);
    }

    void queue_file::read_at(uint64_t position, uint8_t* into, uint32_t count) const {
        uint32_t done = 0;
        while (done < count) {
            const ssize_t n = ::pread(fd, into + done, count - done, (off_t) (position + done));
            if (n < 0) {
                if (errno == EINTR) continue;
                qf_fail("reading the queue file", path);
            }
            if (n == 0)
                qf_fail_plain("queue file ended early [" + path + "] reading "
                           + std::to_string(count) + " bytes at " + std::to_string(position));
            done += (uint32_t) n;
        }
    }

    void queue_file::write_at(uint64_t position, const uint8_t* from, uint32_t count) const {
        uint32_t done = 0;
        while (done < count) {
            const ssize_t n = ::pwrite(fd, from + done, count - done, (off_t) (position + done));
            if (n < 0) {
                if (errno == EINTR) continue;
                qf_fail("writing the queue file", path);
            }
            done += (uint32_t) n;
        }
    }

    uint64_t queue_file::wrap_position(uint64_t position) const {
        return position < file_length ? position : header_length + position - file_length;
    }

    void queue_file::ring_read(uint64_t position, uint8_t* into, uint32_t count) const {
        position = wrap_position(position);
        if (position + count <= file_length) {
            read_at(position, into, count);
            return;
        }
        // the read runs off the end, so it comes back round to just after the header
        const auto before_end = (uint32_t) (file_length - position);
        read_at(position, into, before_end);
        read_at(header_length, into + before_end, count - before_end);
    }

    void queue_file::ring_write(uint64_t position, const uint8_t* from, uint32_t count) const {
        position = wrap_position(position);
        if (position + count <= file_length) {
            write_at(position, from, count);
            return;
        }
        const auto before_end = (uint32_t) (file_length - position);
        write_at(position, from, before_end);
        write_at(header_length, from + before_end, count - before_end);
    }

    void queue_file::ring_erase(uint64_t position, uint64_t length) const {
        static const std::vector<uint8_t> zeroes(initial_length, 0);
        while (length > 0) {
            const auto chunk = (uint32_t) std::min<uint64_t>(length, zeroes.size());
            ring_write(position, zeroes.data(), chunk);
            length -= chunk;
            position += chunk;
        }
    }

    queue_file::element queue_file::read_element(uint64_t position) const {
        if (position == 0)
            return element{};
        uint8_t head[element_header_length]{};
        ring_read(position, head, element_header_length);
        const uint32_t length = qf_get_u32(head);
        /*
         * A length no ring this size could hold is refused here rather than
         * further on - TODO 363.
         *
         * This is the state the durability note in the header describes: below
         * `each_add` nothing orders the element's bytes against the header that
         * points at them, so a crash can leave a header counting an element
         * whose first four bytes never arrived. Those four bytes are the
         * length, so what comes back is whatever was in that space.
         *
         * It was caught anyway, one read later, by the file ending early. The
         * reason to catch it here is that the caller sizes a buffer from this
         * first: a 0xFFFFFFFF length meant asking for four gigabytes and then
         * failing, which on a machine that overcommits is a real allocation of
         * a real four gigabytes before the error arrives.
         */
        // saturating, because a corrupt header can claim a length barely over the
        // header's own and the subtraction would wrap into a number that refuses
        // nothing - the open time check only guarantees file_length > header_length
        const uint64_t ring = file_length - header_length;
        if (const uint64_t room = ring > element_header_length
                                  ? ring - element_header_length : 0;
            length > room) {
            qf_fail_plain("queue file element is not possible [" + path + "]: the element at "
                       + std::to_string(position) + " says it is " + std::to_string(length)
                       + " bytes and the whole ring holds " + std::to_string(room));
        }
        return element{position, length};
    }

    void queue_file::write_header(uint64_t length, uint32_t count, uint64_t first_position,
                                  uint64_t last_position) const {
        uint8_t buffer[32]{};
        if (versioned) {
            qf_put_u32(buffer, versioned_header);
            qf_put_u64(buffer + 4, length);
            qf_put_u32(buffer + 12, count);
            qf_put_u64(buffer + 16, first_position);
            qf_put_u64(buffer + 24, last_position);
            write_at(0, buffer, 32);
            return;
        }
        qf_put_u32(buffer, (uint32_t) length);     // signed in Java, so the top bit is clear
        qf_put_u32(buffer + 4, count);
        qf_put_u32(buffer + 8, (uint32_t) first_position);
        qf_put_u32(buffer + 12, (uint32_t) last_position);
        write_at(0, buffer, 16);
    }

    void queue_file::set_file_length(uint64_t new_length) const {
        if (::ftruncate(fd, (off_t) new_length) != 0)
            qf_fail("could not resize the queue file", path);
        // the length is metadata, so it needs its own sync: O_DSYNC does not cover it
        if (::fsync(fd) != 0)
            qf_fail("could not sync the queue file's new length", path);
    }

    uint64_t queue_file::used_bytes() const {
        if (element_count == 0)
            return header_length;
        if (last.position >= first.position) {
            // one contiguous run
            return (last.position - first.position)
                   + element_header_length + last.length
                   + header_length;
        }
        // wrapped: the tail is before the head in the file
        return last.position
               + element_header_length + last.length
               + file_length - first.position;
    }

    uint64_t queue_file::remaining_bytes() const {
        return file_length - used_bytes();
    }

    void queue_file::expand_if_necessary(uint64_t data_length) {
        const uint64_t element_length = element_header_length + data_length;
        uint64_t remaining = remaining_bytes();
        if (remaining >= element_length)
            return;

        // double until it fits, which keeps the length a power of two
        uint64_t previous = file_length;
        uint64_t new_length = previous;
        do {
            remaining += previous;
            new_length = previous << 1;
            previous = new_length;
        } while (remaining < element_length);

        set_file_length(new_length);

        /*
         * If the ring wrapped, the part that sits at the front of the file now
         * has room after the old end, so it is copied there and the queue
         * becomes one contiguous run again. Source and destination cannot
         * overlap: the copy goes to the old end, and what is copied is at most
         * everything before it.
         */
        const uint64_t end_of_last = wrap_position(last.position + element_header_length
                                                   + last.length);
        uint64_t copied = 0;
        if (end_of_last <= first.position) {
            const uint64_t count = end_of_last - header_length;
            std::vector<uint8_t> chunk(std::min<uint64_t>(count, initial_length));
            uint64_t moved = 0;
            while (moved < count) {
                const auto n = (uint32_t) std::min<uint64_t>(count - moved, chunk.size());
                read_at(header_length + moved, chunk.data(), n);
                write_at(file_length + moved, chunk.data(), n);
                moved += n;
            }
            copied = count;
        }

        // commit: the header is what makes the new length and positions real
        if (last.position < first.position) {
            const uint64_t new_last = file_length + last.position - header_length;
            write_header(new_length, element_count, first.position, new_last);
            last = element{new_last, last.length};
        } else {
            write_header(new_length, element_count, first.position, last.position);
        }

        file_length = new_length;

        if (zero_removed && copied > 0)
            ring_erase(header_length, copied);
    }

    void queue_file::add(const uint8_t* data, uint32_t count) {
        if (data == nullptr && count > 0)
            qf_fail_plain("queue_file::add given no data [" + path + "]");
        if (count > max_element_length)
            qf_fail_plain("element of " + std::to_string(count) + " bytes is larger than this"
                       " format can address [" + path + "]");

        expand_if_necessary(count);

        const bool was_empty = empty();
        const uint64_t position = was_empty
            ? header_length
            : wrap_position(last.position + element_header_length + last.length);

        uint8_t head[element_header_length]{};
        qf_put_u32(head, count);
        ring_write(position, head, element_header_length);
        if (count > 0)
            ring_write(position + element_header_length, data, count);

        /*
         * The header last, which is what makes this atomic: until it is written
         * the element is bytes nobody is looking at, and after it the element is
         * in the queue. There is no third state.
         */
        const uint64_t first_position = was_empty ? position : first.position;
        write_header(file_length, element_count + 1, first_position, position);

        last = element{position, count};
        ++element_count;
        if (was_empty)
            first = last;

        if (policy.when == sync_when::after_bytes) {
            unsynced += element_header_length + count;
            if (policy.bytes > 0 && unsynced >= policy.bytes)
                sync();
        } else if (policy.when != sync_when::each_add) {
            unsynced += element_header_length + count;
        }
    }

    void queue_file::add(const std::string& data) {
        add(reinterpret_cast<const uint8_t*>(data.data()), (uint32_t) data.size());
    }

    void queue_file::add(const std::vector<uint8_t>& data) {
        add(data.data(), (uint32_t) data.size());
    }

    bool queue_file::peek(std::vector<uint8_t>& into) const {
        if (empty())
            return false;
        into.resize(first.length);
        if (first.length > 0)
            ring_read(first.position + element_header_length, into.data(), first.length);
        return true;
    }

    void queue_file::for_each(const std::function<bool(const uint8_t*, uint32_t)>& fn) const {
        uint64_t position = first.position;
        std::vector<uint8_t> data;
        for (uint32_t i = 0; i < element_count; ++i) {
            const element current = read_element(position);
            data.resize(current.length);
            if (current.length > 0)
                ring_read(wrap_position(current.position + element_header_length),
                          data.data(), current.length);
            if (!fn(data.data(), current.length))
                return;
            position = wrap_position(current.position + element_header_length + current.length);
        }
    }

    void queue_file::remove() {
        remove(1);
    }

    void queue_file::remove(uint32_t n) {
        if (n == 0)
            return;
        if (empty())
            qf_fail_plain("cannot remove from an empty queue file [" + path + "]");
        if (n > element_count)
            qf_fail_plain("cannot remove " + std::to_string(n) + " elements from a queue file"
                       " holding " + std::to_string(element_count) + " [" + path + "]");
        if (n == element_count) {
            clear();
            return;
        }

        const uint64_t erase_from = first.position;
        uint64_t erase_length = 0;

        // walk n elements forward to find the new head
        uint64_t new_first = first.position;
        uint32_t new_first_length = first.length;
        uint8_t head[element_header_length]{};
        for (uint32_t i = 0; i < n; ++i) {
            erase_length += element_header_length + new_first_length;
            new_first = wrap_position(new_first + element_header_length + new_first_length);
            ring_read(new_first, head, element_header_length);
            new_first_length = qf_get_u32(head);
        }

        write_header(file_length, element_count - n, new_first, last.position);
        element_count -= n;
        first = element{new_first, new_first_length};

        if (zero_removed)
            ring_erase(erase_from, erase_length);
    }

    void queue_file::sync() const {
        if (policy.when == sync_when::each_add) {
            unsynced = 0;               // O_DSYNC already put it there
            return;
        }
        if (::fdatasync(fd) != 0)
            qf_fail("could not sync the queue file", path);
        unsynced = 0;
    }

    void queue_file::clear() {
        write_header(initial_length, 0, 0, 0);

        if (zero_removed) {
            const std::vector<uint8_t> zeroes(initial_length - header_length, 0);
            write_at(header_length, zeroes.data(), (uint32_t) zeroes.size());
        }

        element_count = 0;
        first = element{};
        last = element{};
        if (file_length > initial_length)
            set_file_length(initial_length);
        file_length = initial_length;
    }
}
