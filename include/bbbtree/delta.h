#pragma once

#include "bbbtree/btree.h"
#include "bbbtree/types.h"

#include <cstdint>
#include <variant>

namespace bbbtree {
// -----------------------------------------------------------------
/// An indexable Page ID that can be used as a key in the BTree to identify
/// changes on pages.
using PID = UInt64;
// -----------------------------------------------------------------
/// Forward declarations.
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Delta;
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Deltas;
template <KeyIndexable KeyT, ValueIndexable ValueT> class DeltaTree;
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Delta<KeyT, ValueT> &type);
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Deltas<KeyT, ValueT> &type);
// -----------------------------------------------------------------
/// A Delta is a change that was applied to an entry.
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Delta {
	/// An entry that was changed in the index.
	struct Entry {
		KeyT key;
		ValueT value;

		/// Default Constructor.
		Entry() = default;
		/// Constructor for leaf entries.
		Entry(KeyT key, ValueT value)
			: key(std::move(key)), value(std::move(value)) {}

		/// Spaceship operator.
		auto operator<=>(const Entry &) const = default;
	};

	/// Default Constructor.
	Delta() = default;
	/// Constructor for leaf deltas.
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
	void deserialize(const std::byte *src);

	/// Spaceship operator.
	auto operator<=>(const Delta &) const = default;
	/// Prints the delta.
	friend std::ostream &operator<< <>(std::ostream &,
									   const Delta<KeyT, ValueT> &);
};
// -----------------------------------------------------------------
/// Deltas to be stored as values in a delta BTree.
template <KeyIndexable KeyT, ValueIndexable ValueT> struct Deltas {
	using LeafDelta = Delta<KeyT, ValueT>;
	using InnerNodeDelta = Delta<KeyT, PID>;
	using LeafDeltas = std::vector<LeafDelta>;
	using InnerNodeDeltas = std::vector<InnerNodeDelta>;

  public:
	/// Constructor for leaf deltas.
	Deltas(LeafDeltas &&deltas, uint16_t slot_count);
	/// Constructor for leaf deltas with known size.
	Deltas(LeafDeltas &&deltas, uint16_t slot_count, uint16_t size);
	/// Constructor for inner node deltas.
	Deltas(InnerNodeDeltas &&deltas, PageID upper, uint16_t slot_count);
	/// Constructor for inner node deltas with known size.
	Deltas(InnerNodeDeltas &&deltas, PageID upper, uint16_t slot_count,
		   uint16_t size);

	/// Number of bytes of the serialized object.
	uint16_t size() const { return cached_size; }
	/// Serializes this type into bytes to store on pages. The caller must
	/// ensure that `dst` has enough space for `cached_size`.
	void serialize(std::byte *dst) const;
	/// Deserializes the bytes on a page into the type.
	static Deltas deserialize(const std::byte *src, uint16_t n);

	bool is_leaf() const { return std::holds_alternative<LeafDeltas>(deltas); }

	/// Spaceship operator.
	auto operator<=>(const Deltas &) const = default;
	/// Prints the deltas.
	friend std::ostream &operator<< <>(std::ostream &os,
									   const Deltas<KeyT, ValueT> &);

	friend DeltaTree<KeyT, ValueT>;

  private:
	/// Returns the number of deltas.
	uint16_t num_deltas() const;

	/// The deltas extracted from BTree nodes. May be from leaf or inner nodes.
	/// Leaf nodes store keys and values, inner nodes store keys and PIDs.
	const std::variant<LeafDeltas, InnerNodeDeltas> deltas;
	/// Inner nodes store the upper page ID.
	PageID upper{0};
	/// The number of slots in the node at evict time.
	uint16_t slot_count;

	/// Cached size of the serialized deltas.
	uint16_t cached_size;
};
// -----------------------------------------------------------------
} // namespace bbbtree