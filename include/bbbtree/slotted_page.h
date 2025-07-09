#pragma once

#include "bbbtree/types.h"

#include <cstddef>

namespace bbbtree
{
    /// @brief A SlottedPage contains Records identified through Tuple IDs (`TID`).
    /// Slots grow from the front of the page, after the heading. Data grow from the end.
    /// The page is full when slots and data meets.
    /// TODO: Does not handle redirect tuples. No deletions, compactification & relocation for now. Append-Only.
    struct SlottedPage
    {
        struct Header
        {
            /// Constructor
            explicit Header(uint32_t page_size);

            /// Number of currently used slots. Indicates the next free slot due to append-only design.
            uint16_t slot_count;
            /// Upper end of the data. Where new data can be prepended.
            uint32_t data_start;
        };

        /// @brief A slot indicates the offset and length of the corresponding tuple. No redirects for now.
        struct Slot
        {
            /// Constructor
            Slot() = default;

            /// Clear the slot.
            void clear() { value = 0; }
            /// Get the size.
            [[nodiscard]] uint32_t get_size() const { return value & 0xFFFFFFull; }
            /// Get the offset.
            [[nodiscard]] uint32_t get_offset() const { return (value >> 24) & 0xFFFFFFull; }
            /// Is empty?
            [[nodiscard]] bool is_empty() const { return value == 0; }

            /// Set the slot.
            void set_slot(uint32_t offset, uint32_t size)
            {
                value = 0;
                value ^= size & 0xFFFFFFull;
                value ^= (offset & 0xFFFFFFull) << 24;
            }

        private:
            /// The slot value: T (1 byte) | S (1 byte) | O (3 byte) | L (3 byte)
            /// T: If != 0xFF, the slot points to another record. The value is the TID.
            /// S: If = 0, the tuple is at offset O with length L.
            /// Otherwise, the tuple was moved from another page, placed at offset O with length L. The first 8 bytes contain the original TID.
            uint64_t value{0};
        };

        /// Constructor. Initializes the header and sets the rest to zero.
        /// @param[in] page_size    The size of the page.
        explicit SlottedPage(uint32_t page_size);

        /// Get data.
        std::byte *get_data() { return reinterpret_cast<std::byte *>(this); }
        /// Get constant data.
        [[nodiscard]] const std::byte *get_data() const { return reinterpret_cast<const std::byte *>(this); }

        /// Get slots.
        Slot *get_slots() { return reinterpret_cast<SlottedPage::Slot *>(get_data() + sizeof(SlottedPage)); }
        /// Get constant slots.
        [[nodiscard]] const Slot *get_slots() const { return reinterpret_cast<const SlottedPage::Slot *>(get_data() + sizeof(SlottedPage)); }

        /// Get free space.
        size_t get_free_space() { return header.data_start - sizeof(SlottedPage) - header.slot_count * sizeof(Slot); };

        static size_t get_initial_free_space(uint32_t page_size) { return page_size - sizeof(SlottedPage); }

        // Allocate a slot. Throws if not enough space left on page.
        /// @param[in] data_size    The slot that should be allocated.
        /// @param[in] page_size    The new size of a slot.
        /// @return                 The new slot's ID.
        SlotID allocate(uint32_t data_size, uint32_t page_size);

        /// Erase a slot. Throws if `slot_id` invalid.
        /// @param[in] slot_id      The slot that should be erased
        void erase(SlotID slot_id);

        /// The header.
        /// DO NOT allocate heap objects for a slotted page but instead reinterpret_cast BufferFrame.get_data()!
        Header header;
    };

    static_assert(sizeof(SlottedPage) == sizeof(SlottedPage::Header), "An empty slotted page must only contain the header");
} // namespace bbbtree
