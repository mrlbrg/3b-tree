#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>

namespace bbbtree {
// -----------------------------------------------------------------
/// A SegmentID corresponds to a file.
using SegmentID = uint16_t;
/// A Page within a Segment.
using PageID = uint64_t;
/// A Slot within a Page.
using SlotID = uint16_t;
// -----------------------------------------------------------------
struct UInt64 {
	/// Default Constructor.
	UInt64() = default;
	/// Constructor.
	UInt64(uint64_t value) : value(value) {};

	/// Size of the serialized value.
	static constexpr uint16_t size() { return sizeof(value); }
	/// Serialize the value into bytes to store on pages.
	void serialize(std::byte *dst) const { std::memcpy(dst, &value, size()); }
	/// Deserializes the bytes into the type.
	static UInt64 deserialize(const std::byte *data, uint16_t num_bytes) {
		assert(num_bytes == size());
		return UInt64(*reinterpret_cast<const uint64_t *>(data));
	}

	/// Spaceship operator.
	auto operator<=>(const UInt64 &) const = default;
	/// Prints out value.
	friend std::ostream &operator<<(std::ostream &os, const UInt64 &value);
	/// Hashes the value.
	friend struct std::hash<UInt64>;

  private:
	uint64_t value;
};
// -----------------------------------------------------------------
/// A TID consists of page ID (48 bit) | slot ID (16 bit).
struct TID {
	/// Default Constructor.
	TID() = default;
	/// Constructor from raw tuple ID.
	TID(uint64_t tuple_id) : value(tuple_id) {}
	/// Constructor from page and slot ID.
	TID(PageID page_id, SlotID slot_id)
		: value((page_id << 16) ^ (slot_id & 0xFFFF)) {}

	/// Get the page ID.
	[[nodiscard]] PageID get_page_id() const { return value >> 16; }
	/// Get the slot ID.
	[[nodiscard]] SlotID get_slot_id() const { return value & 0xFFFF; }

	/// Size of the serialized value.
	static constexpr uint16_t size() { return sizeof(value); }
	/// Serialize the value into bytes to store on pages.
	void serialize(std::byte *dst) const { std::memcpy(dst, &value, size()); }
	/// Deserializes the bytes into the type.
	static TID deserialize(const std::byte *data, uint16_t num_bytes) {
		assert(num_bytes == size());
		return TID(*reinterpret_cast<const uint64_t *>(data));
	}

	/// Spaceship operator.
	auto operator<=>(const TID &) const = default;
	/// Prints out value.
	friend std::ostream &operator<<(std::ostream &os, const TID &value);
	/// Hashes the value.
	friend struct std::hash<TID>;

  private:
	uint64_t value;
};
// -----------------------------------------------------------------
/// Beware that this object is not the owner of its buffer. Must manage lifetime
/// of the data its pointing to. TODO: Must update this when
/// multi-threading/when objects on nodes become mutable.
struct String {
	/// Default Constructor.
	String() = default;
	/// Constructor.
	String(std::string_view view) : view(view) {};

	/// Size of the wrapped value.
	uint16_t size() const { return view.size(); }
	/// Serializes this type into bytes to store on pages.
	void serialize(std::byte *dst) const {
		std::memcpy(dst, view.data(), view.size());
	}
	/// Deserializes the bytes into the type.
	static String deserialize(const std::byte *data, uint16_t num_bytes) {
		return String({reinterpret_cast<const char *>(data), num_bytes});
	}

	/// Spaceship operator.
	auto operator<=>(const String &) const = default;
	/// Prints out value.
	friend std::ostream &operator<<(std::ostream &os, const String &value);
	/// Hashes the value.
	friend struct std::hash<String>;

  private:
	std::string_view view;
};
// -----------------------------------------------------------------
} // namespace bbbtree
// -----------------------------------------------------------------
namespace std {
template <> struct hash<bbbtree::UInt64> {
	size_t operator()(const bbbtree::UInt64 &type) const {
		return std::hash<uint64_t>()(type.value);
	}
};
template <> struct hash<bbbtree::TID> {
	size_t operator()(const bbbtree::TID &type) const {
		return std::hash<uint64_t>()(type.value);
	}
};
template <> struct hash<bbbtree::String> {
	size_t operator()(const bbbtree::String &type) const {
		return std::hash<std::string_view>()(type.view);
	}
};
} // namespace std