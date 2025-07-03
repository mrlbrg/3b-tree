#pragma once

#include <cstdint>

namespace bbbtree
{
    /// A SegmentID corresponds to a file.
    using SegmentID = uint16_t;
    /// A Page within a Segment.
    using PageID = uint64_t;
    /// A Slot within a Page.
    using SlotID = uint16_t;

    /// The Segment and Page ID within one int.
    struct SegmentPageID
    {
        /// Constructor.
        SegmentPageID(SegmentID segment_id, PageID page_id) : value(static_cast<uint64_t>(segment_id) << 48 | page_id) {}

        /// Extract the segment ID from the value.
        SegmentID get_segment_id() { return value >> 48; }

        /// Extract the page ID from the value.
        PageID get_segment_page_id() { return value & ((1ull << 48) - 1); }

        /// Segment ID (16 bit) | Page ID (48 bit)
        uint64_t value;
    };
}
