#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"
#include "bbbtree/types.h"

#include <concepts>
#include <cstdint>
#include <vector>

// TODO: Rethink `logic_errors` and which error makes more sense to throw.
// TODO: Maybe store tuples directly in B-Tree and do not redirect via TID.
// TODO: Create a metadata page that stores all state for all segments. That
// way, not each BTree has to store its state on its own page, wasting space in
// the buffer.

namespace bbbtree {
// We use only a single type of value for this database.
using ValueT = uint64_t;
// Buffer Configs
static const constexpr size_t PAGE_SIZE = 1024;
static const constexpr size_t NUM_PAGES = 10;
// File Configs
static const constexpr SegmentID FSI_SEGMENT_ID = 0;
static const constexpr SegmentID SP_SEGMENT_ID = 1;
static const constexpr SegmentID INDEX_SEGMENT_ID = 2;
static const constexpr SegmentID DELTA_SEGMENT_ID = 3;

/// A concept that requires some member functions from an index mapping a key to
/// TIDs.
template <template <typename, typename, bool = false> class IndexT,
		  typename KeyT>
concept IndexInterface =
	requires(IndexT<KeyT, TID> index, const KeyT &key, const TID &value) {
		{ index.lookup(key) } -> std::same_as<std::optional<TID>>;
		{ index.erase(key) } -> std::same_as<void>;
		{ index.insert(key, value) } -> std::same_as<bool>;
	};

/// A Database maintains a single table of keys and values. The schema is
/// fixated at compile-time. It is templated on its index, which maps `KeyT` to
/// TIDs. The value type is always the same.
template <template <typename, typename, bool = false> typename IndexT,
		  typename KeyT>
	requires IndexInterface<IndexT, KeyT>
class Database {
  public:
	/// A Tuple with key and value that are stored in the database.
	/// TODO: Make keys variable-sized.
	struct Tuple {
		KeyT key;
		ValueT value;

		/// Spaceship operator.
		auto operator<=>(const Tuple &) const = default;
	};

	/// Constructor.
	Database(size_t page_size = PAGE_SIZE, size_t num_pages = NUM_PAGES,
			 bool reset = false);

	/// Inserts a tuple into the database.
	/// TODO: Allow insert of variable sized tuples.
	void insert(const Tuple &tuple);
	/// Inserts tuples into the database.
	/// Tuple keys must not be already present in database.
	void insert(const std::vector<Tuple> &tuples);
	/// Reads a value by key from the database.
	Tuple get(const KeyT &key);
	/// Deletes a tuple by key from the database.
	/// TODO: Implement erase.
	void erase(const KeyT &key);
	/// Returns the number of tuples stored in the database.
	size_t size() { return index.size(); }

  private:
	/// The buffer manager. Note that the buffer manager must be the first
	/// member to ensure that it is destructed last. Other members might have
	/// pages to persist in their destructor.
	BufferManager buffer_manager;
	/// The Free-Space-Inventory segment.
	FSISegment space_inventory;
	/// The Slotted Pages segment.
	SPSegment records;
	/// The access path. Maps keys to their tuple IDs (`TID`).
	IndexT<KeyT, TID> index;
};

} // namespace bbbtree
