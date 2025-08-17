#pragma once

#include "bbbtree/btree.h"
#include "bbbtree/types.h"

namespace bbbtree {
// -----------------------------------------------------------------
/// An indexable Page ID that can be used as a key in the BTree to identify
/// changes on pages.
using PID = UInt64;
// -----------------------------------------------------------------
/// Forward declarations.
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Delta;
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Deltas;
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Delta<KeyT, ValueT> &type);
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Deltas<KeyT, ValueT> &type);
// -----------------------------------------------------------------
/// Types of operations performed on the index.
enum class OperationType : uint8_t { Insert = 0, Update = 1, Delete = 2 };
// -----------------------------------------------------------------
/// A Delta is a change that was applied to an entry.
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Delta {
	/// An entry that was changed in the index.
	struct Entry {
		KeyT key;
		ValueT value;

		/// Default Constructor.
		Entry() = default;
		/// Constructor.
		Entry(KeyT key, ValueT value)
			: key(std::move(key)), value(std::move(value)) {}

		/// Spaceship operator.
		auto operator<=>(const Entry &) const = default;
	};

	/// Default Constructor.
	Delta() = default;

	/// Constructor.
	Delta(OperationType op, KeyT key, ValueT value)
		: entry{std::move(key), std::move(value)}, op(op) {}

	/// The entry that was changed.
	Entry entry;
	/// The type of operation.
	OperationType op;

	/// Size of the serialized value.
	uint16_t size() const;
	/// Serializes this type into bytes to store on pages.
	void serialize(std::byte *dst) const;
	/// Deserializes the bytes on a page into the type.
	/// @max_bytes: the maximum number of bytes to read from the source.
	static Delta deserialize(const std::byte *src);

	/// Spaceship operator.
	auto operator<=>(const Delta &) const = default;
	/// Prints the delta.
	friend std::ostream &operator<< <>(std::ostream &,
									   const Delta<KeyT, ValueT> &);
};
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Deltas {
  public:
	/// Constructor.
	Deltas(std::vector<Delta<KeyT, ValueT>> deltas)
		: deltas(std::move(deltas)) {
		cached_size = sizeof(uint16_t); // Number of deltas.
		// Calculate the size of the deltas.
		for (const auto &delta : this->deltas)
			cached_size += delta.size();
	}
	/// Constuctor with known size.
	Deltas(std::vector<Delta<KeyT, ValueT>> deltas, uint16_t size)
		: deltas(std::move(deltas)), cached_size(size) {}

	/// Size of the serialized values.
	uint16_t size() const { return cached_size; }
	/// Serializes this type into bytes to store on pages.
	void serialize(std::byte *dst) const;
	/// Deserializes the bytes on a page into the type.
	static Deltas deserialize(const std::byte *src, uint16_t n);

	/// Spaceship operator.
	auto operator<=>(const Deltas &) const = default;
	/// Prints the deltas.
	friend std::ostream &operator<< <>(std::ostream &os,
									   const Deltas<KeyT, ValueT> &);

  private:
	/// The deltas that were applied to the page.
	const std::vector<Delta<KeyT, ValueT>> deltas;
	/// The number of bytes needed to serialize the deltas.
	uint16_t cached_size = 0;
};
// -----------------------------------------------------------------
} // namespace bbbtree