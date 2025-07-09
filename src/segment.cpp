#include "bbbtree/segment.h"

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace bbbtree {
FSISegment::FSISegment(SegmentID segment_id, BufferManager &buffer_manager)
	: Segment(segment_id, buffer_manager) {}

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
	auto &frame = buffer_manager.fix_page(segment_id, 0, false);
	auto &header = *(reinterpret_cast<FSISegment::Header *>(frame.get_data()));

	// Enough space?
	if (header.free_space < required_space) {
		buffer_manager.unfix_page(frame, false);
		return {};
	}

	auto page_id = header.allocated_pages - 1;
	buffer_manager.unfix_page(frame, false);

	return {page_id};
}

void FSISegment::update(uint64_t target_page, uint32_t new_free_space) {
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &header = *(reinterpret_cast<FSISegment::Header *>(frame.get_data()));

	if (target_page != (header.allocated_pages - 1)) {
		buffer_manager.unfix_page(frame, true);
		throw std::logic_error("FSISegment::update(): Cannot update a page's "
							   "free space which is not the last.");
	}

	if (header.free_space < new_free_space) {
		buffer_manager.unfix_page(frame, true);
		throw std::logic_error("FSISegment::update(): Free space on a slotted "
							   "page can only shrink.");
	}

	header.free_space = new_free_space;
	buffer_manager.unfix_page(frame, true);
}

PageID FSISegment::create_new_page(size_t initial_free_space) {
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &header = *(reinterpret_cast<FSISegment::Header *>(frame.get_data()));

	// TODO: Synchronize this when multi-threading.
	++header.allocated_pages;
	header.free_space = initial_free_space;
	auto page_id = header.allocated_pages - 1;
	buffer_manager.unfix_page(frame, true);

	return page_id;
}

TID SPSegment::allocate(uint32_t size) {
	auto create_new_slotted_page = [&]() {
		// Create new page in Free-Space Inventory
		auto new_page_id =
			space_inventory.create_new_page(SlottedPage::get_initial_free_space(
				buffer_manager.get_page_size()));
		// Create new Slotted Page
		auto &frame = buffer_manager.fix_page(segment_id, new_page_id, true);
		auto &slotted_page = this->get_slotted_page(frame);
		slotted_page.header = SlottedPage::Header{
			static_cast<uint32_t>(buffer_manager.get_page_size())};
		buffer_manager.unfix_page(frame, true);

		return new_page_id;
	};

	// Tuple must be smaller than page
	auto max_size = buffer_manager.get_page_size() - sizeof(SlottedPage::Slot) -
					sizeof(SlottedPage::Header);
	if (size > max_size)
		throw std::logic_error("SPSegment::allocate(): Cannot allocate tuples "
							   "bigger than the page.");

	// Find page with enough space (last page or new page)
	auto optional_page_id =
		space_inventory.find(size + sizeof(SlottedPage::Slot));
	PageID page_id = optional_page_id.has_value() ? optional_page_id.value()
												  : create_new_slotted_page();
	auto &page = buffer_manager.fix_page(segment_id, page_id, true);
	auto &slotted_page = get_slotted_page(page);

	// Allocate a new slot on that page
	auto slot_id = slotted_page.allocate(size, buffer_manager.get_page_size());
	space_inventory.update(page_id, slotted_page.get_free_space());

	buffer_manager.unfix_page(page, true);

	return TID{page_id, slot_id};
}

uint32_t SPSegment::read(TID tid, std::byte *record, uint32_t capacity) const {
	// Get Slot
	auto &frame = buffer_manager.fix_page(segment_id, tid.get_page_id(), false);
	auto &slotted_page = get_slotted_page(frame);
	auto &slot = *(slotted_page.get_slots() + tid.get_slot_id());

	// TODO: Allow redirects

	// Read
	const auto *data = slotted_page.get_data() + slot.get_offset();
	uint32_t bytes_read = std::min(capacity, slot.get_size());
	std::memcpy(record, data, bytes_read);

	buffer_manager.unfix_page(frame, false);

	return bytes_read;
}

uint32_t SPSegment::write(TID tid, const std::byte *record,
						  uint32_t record_size) {
	// Get Slot
	auto &frame = buffer_manager.fix_page(segment_id, tid.get_page_id(), true);
	auto &slotted_page = get_slotted_page(frame);
	auto &slot = *(slotted_page.get_slots() + tid.get_slot_id());

	// TODO: Allow redirects

	// Enough size allocated?
	// TODO: Allow Resize
	if (slot.get_size() != record_size) {
		buffer_manager.unfix_page(frame, false);
		throw std::logic_error(
			"SPSegment::write(): Size of record to be written must be the size "
			"allocated for the given tuple ID (TID).");
	}

	// Write
	auto *data = slotted_page.get_data() + slot.get_offset();
	std::memcpy(data, record, record_size);
	buffer_manager.unfix_page(frame, true);

	return record_size;
}

void SPSegment::erase(TID tid) {
	// TODO
	throw std::logic_error("Database::erase(): Not implemented yet.");
}
} // namespace bbbtree