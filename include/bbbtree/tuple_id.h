#pragma once

#include "bbbtree/types.h"

#include <iostream>

namespace bbbtree {
/// @brief A TID consists of page ID (48 bit) | slot ID (16 bit).
struct TID {
	/// Constructor from raw tuple ID.
	TID(uint64_t tuple_id) : value(tuple_id) {}
	/// Constructor from page and slot ID.
	TID(PageID page_id, SlotID slot_id)
		: value((page_id << 16) ^ (slot_id & 0xFFFF)) {}

	/// Get the page ID.
	[[nodiscard]] PageID get_page_id() const { return value >> 16; }
	/// Get the slot ID.
	[[nodiscard]] SlotID get_slot_id() const { return value & 0xFFFF; }

	auto operator<=>(const TID &) const = default;

	friend std::ostream &operator<<(std::ostream &os, const TID &tid);

  private:
	/// The TID value.
	uint64_t value;
};
} // namespace bbbtree