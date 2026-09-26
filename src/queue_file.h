//
// A reliable, file-based FIFO queue - a port of Square's Tape QueueFile.
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
// This file is a modified derivative work: translated to C++ for barch from
// tape/src/main/java/com/squareup/tape2/QueueFile.java, with the changes noted
// in TODO 351. The original is by Bob Lee (bob@squareup.com).
//
#ifndef BARCH_QUEUE_FILE_H
#define BARCH_QUEUE_FILE_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace barch {
    /**
     * A file-based FIFO queue where adding and removing are O(1) and both are
     * atomic - the file survives the process dying mid-operation.
     *
     * How that works is worth knowing before relying on it. An addition writes
     * the element into the ring buffer first and the header last. The header is
     * one small write at offset 0, so a crash either lands before it - leaving
     * the file exactly as it was, minus an element nobody was told about - or
     * after it, leaving the element. There is no state in between that a reader
     * can see. The file length lives in the header for the same reason: an
     * expansion that grew the file but died before copying can be recognised.
     *
     * The caveat the original states and this port inherits: this assumes the
     * header write reaches the device whole. On a filesystem that can tear a
     * 32 byte write at offset 0 the header can be garbage after a power cut.
     * Journalled filesystems in their default modes do not tear a single
     * sector-aligned write, but this is an assumption and not a guarantee.
     *
     * Not thread safe, as in the original. One writer at a time, and readers
     * excluded from it - barch's own latches are the obvious way to do that.
     *
     * `peek` and `remove` are separate on purpose: read the head, do something
     * with it, and only then drop it. A crash in between leaves the element to
     * be processed again, which is the behaviour a log replay wants.
     */
    /** when a queue file pushes what it has written to the device - TODO 352 */
    enum class sync_when {
        never,       // barch never syncs; the page cache decides
        on_demand,   // only when sync() is called, e.g. from a timer
        each_add,    // every addition, before add() returns
        after_bytes  // once this many bytes have been added since the last sync
    };

    struct sync_policy {
        sync_when when{sync_when::each_add};
        uint64_t bytes{0};      // only read when `when` is after_bytes
    };

    class queue_file {
    public:
        /** the on-disk length of an element's own header: a 4 byte length */
        static constexpr uint32_t element_header_length = 4;
        /** what a new file is created as, and what clear() truncates back to */
        static constexpr uint64_t initial_length = 4096;
        /** the largest element this format can address */
        static constexpr uint32_t max_element_length = 0x7FFFFFFFu;

        /**
         * Open `path`, creating it if it is not there.
         *
         * `sync` decides when what has been written reaches the device.
         * `each_add` opens the file O_DSYNC, which is Java's "rwd" and what the
         * crash behaviour described above depends on; it costs a device round
         * trip per operation.
         *
         * The weaker policies are not merely less recent, and this is the part
         * worth understanding. An addition is atomic because the element's bytes
         * reach the device before the header that points at them, and O_DSYNC is
         * what orders those two writes. Without it the kernel may put the header
         * down first, so a crash can leave a queue whose header describes an
         * element whose bytes never arrived - and the first four of those bytes
         * are the element's length, so a reader can take a wild length and walk
         * off the end of the ring. Below `each_add`, whatever writes the records
         * has to be able to tell a good record from a bad one - a checksum per
         * record, and a reader that stops at the first that fails. See TODO 352.
         *
         * `zero_removed` overwrites the bytes of removed elements, which costs a
         * write per removal and means a deleted record is really gone.
         *
         * `force_legacy` writes the 16 byte header instead of the 32 byte one,
         * for compatibility with files written by old Tape versions. Legacy
         * files are read either way; this only affects creation.
         */
        explicit queue_file(const std::string& path, sync_policy sync = {},
                            bool zero_removed = false, bool force_legacy = false);
        ~queue_file();

        queue_file(const queue_file&) = delete;
        queue_file& operator=(const queue_file&) = delete;
        queue_file(queue_file&&) = delete;
        queue_file& operator=(queue_file&&) = delete;

        /**
         * add to the tail. throws on an element larger than max_element_length.
         *
         * `from_held` is how much of this element, its header included, comes
         * out of room reserve() is holding. Everything else held stays free: an
         * add that would need it grows the file or fails - TODO 471.
         */
        void add(const uint8_t* data, uint32_t count, uint64_t from_held = 0);
        void add(const std::string& data);
        void add(const std::vector<uint8_t>& data, uint64_t from_held = 0);

        /**
         * Hold `bytes` more of room for elements, their headers included, until
         * release() or an add that draws on it. The file grows now if it has to,
         * and the free part of it gets real blocks, so neither a size limit nor
         * a full disk can take the room back. Throws, holding nothing more, when
         * either can't be done. For a caller about to add several that would
         * rather fail before the first than partway - TODO 470, TODO 471.
         */
        void reserve(uint64_t bytes);
        /** let go of up to `bytes` of what reserve() is holding */
        void release(uint64_t bytes);
        [[nodiscard]] uint64_t held_bytes() const { return held; }

        /** the eldest element, or false when the queue is empty */
        [[nodiscard]] bool peek(std::vector<uint8_t>& into) const;

        /** drop the eldest element, or the eldest n. throws if n exceeds size() */
        void remove();
        void remove(uint32_t n);

        /** drop everything and truncate back to initial_length */
        void clear();
        /**
         * Keep the eldest `keep` elements and drop the rest - the other end from
         * remove(). For a log whose tail didn't verify: what's past the last good
         * element goes, so the next add lands where it can be read. Nothing
         * happens when `keep` is size() or more. Like remove(), one header write,
         * and it isn't synced here.
         */
        void truncate(uint32_t keep);

        /**
         * Push everything written so far to the device, whatever the policy says.
         *
         * Unconditional on purpose: a timer calls this, and so should anything
         * that has just written something it does not want to lose - a
         * checkpoint record, or a shutdown. Does nothing when the file is
         * already synced, which `each_add` always is.
         */
        void sync() const;

        /** bytes added since the last sync, 0 under `each_add` */
        [[nodiscard]] uint64_t unsynced_bytes() const { return unsynced; }

        /**
         * Walk from eldest to newest, stopping early if `fn` returns false.
         *
         * This is what a replay uses. It does not remove anything and it does
         * not mind being stopped part way: the queue is untouched either way, so
         * a replay that fails half way through can be started again.
         */
        void for_each(const std::function<bool(const uint8_t*, uint32_t)>& fn) const;

        [[nodiscard]] uint32_t size() const { return element_count; }
        [[nodiscard]] bool empty() const { return element_count == 0; }
        /** the file's length in bytes, header and unused ring space included */
        [[nodiscard]] uint64_t file_bytes() const { return file_length; }
        /** what the elements and their headers actually occupy */
        [[nodiscard]] uint64_t used_bytes() const;
        [[nodiscard]] const std::string& name() const { return path; }
        /** true when this file uses the 32 byte header */
        [[nodiscard]] bool is_versioned() const { return versioned; }

    private:
        /** a pointer to an element: where it is and how long its data is */
        struct element {
            uint64_t position{0};
            uint32_t length{0};
            [[nodiscard]] bool null() const { return position == 0; }
        };

        std::string path;
        int fd{-1};
        sync_policy policy{};
        mutable uint64_t unsynced{0};
        bool versioned{true};
        bool zero_removed{false};
        uint32_t header_length{32};
        uint64_t file_length{0};
        /** room reserve() is holding, which only a drawing add may use */
        uint64_t held{0};
        uint32_t element_count{0};
        element first{};
        element last{};

        void read_at(uint64_t position, uint8_t* into, uint32_t count) const;
        void write_at(uint64_t position, const uint8_t* from, uint32_t count) const;
        /** the same two, but wrapping at the end of the ring */
        void ring_read(uint64_t position, uint8_t* into, uint32_t count) const;
        void ring_write(uint64_t position, const uint8_t* from, uint32_t count) const;
        void ring_erase(uint64_t position, uint64_t length) const;

        [[nodiscard]] uint64_t wrap_position(uint64_t position) const;
        [[nodiscard]] element read_element(uint64_t position) const;
        void write_header(uint64_t length, uint32_t count, uint64_t first_position,
                          uint64_t last_position) const;
        void set_file_length(uint64_t new_length) const;
        void expand_if_necessary(uint64_t data_length);
        [[nodiscard]] uint64_t remaining_bytes() const;
    };
}

#endif //BARCH_QUEUE_FILE_H
