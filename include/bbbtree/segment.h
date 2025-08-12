#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/slotted_page.h"

#include <optional>

namespace bbbtree {
/// A segment manages a collection of corresponding pages, e.g. a collection of
/// slotted pages. It currently maps to a single file.
class Segment {
  public:
	/// Constructor.
	Segment(SegmentID segment_id, BufferManager &buffer_manager)
		: segment_id(segment_id), buffer_manager(buffer_manager) {}

	/// The segment id.
	SegmentID segment_id;

  protected:
	/// The buffer manager.
	BufferManager &buffer_manager;
};

/// Encodes the free space left in a slotted page segment. Tracks the amount of
/// existing slotted pages. Caller must ensure that a new page's bytes are set
/// to 0. Otherwise its undefined behaviour.
/// TODO: For now, we append-only. Therefore we merely track the
///       last slotted page's space and the total amount of allocated pages.
/// TODO: Only a dummy, append-only FSI Segment. Do not use it for index
/// bootstrapping. Do not maintain it in its own file.
class FSISegment : public Segment {
  public:
	/// Constructor. TODO: When segment (file) was not allocated so far, set the
	/// header of the first page to 0.
	FSISegment(SegmentID segment_id, BufferManager &buffer_manager);
	/// Find a free page. Optionally returns the page ID.
	std::optional<PageID> find(uint32_t required_space);
	/// Updates the amount of free space on the target page. Updated by the
	/// Slotted Pages Segment.
	void update(PageID target_page, uint32_t free_space);
	/// Creates a new page in this inventory. Returns the new page's ID.
	PageID create_new_page(size_t initial_free_space);

  private:
	struct Header {
		/// The number of allocated slotted pages so far.
		size_t allocated_pages;
		/// The free space on the last slotted page.
		size_t free_space;
	};
};

/// A segment (here equivalent to a file) containing all the slotted pages.
class SPSegment : public Segment {
  public:
	/// Constructor. The segment_id is also the name of the file containing the
	/// pages.
	SPSegment(SegmentID segment_id, BufferManager &buffer_manager,
			  FSISegment &fsi)
		: Segment(segment_id, buffer_manager), space_inventory(fsi) {}

	/// Uses the free-space inventory to allocate space for a new tuple on a
	/// page.
	TID allocate(uint32_t size);
	/// Reads data of the tuple into the buffer.
	uint32_t read(TID tid, std::byte *record, uint32_t capacity) const;
	/// Writes a tuple into the given tuple id.
	/// Only allows writes of the exact size allocated for this tuple id.
	uint32_t write(TID tid, const std::byte *record, uint32_t record_size);
	/// TODO: Resizes a tuple.
	void resize(TID tid, uint32_t new_length);
	/// Erases a tuple from the slotted page.
	void erase(TID tid);

  private:
	// Cast a Page to a SlottedPage
	static SlottedPage &get_slotted_page(BufferFrame &buffer_frame) {
		return *(reinterpret_cast<SlottedPage *>(buffer_frame.get_data()));
	}

	/// Free space inventory
	FSISegment &space_inventory;
};
} // namespace bbbtree
