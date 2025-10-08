#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"

#include <unordered_map>

namespace bbbtree {

// An in-memory map.
template <typename KeyT, typename ValueT, bool UseDeltaTree = false> class Map {
	static_assert(!UseDeltaTree,
				  "Map does not support delta trees. Use BBBTree instead.");

  public:
	/// Constructor.
	Map() : map() {}
	/// Constructor for compatibility with Database template.
	Map(SegmentID /*segment_id*/, BufferManager & /*buffer_manager*/,
		uint16_t /*wa_threshold*/) {}

	/// Lookup an entry in the tree. Returns `nullopt` if key was not
	/// found.
	std::optional<ValueT> lookup(const KeyT &key);
	/// Erase an entry in the tree.
	void erase(const KeyT &key, size_t page_size);
	/// Inserts a new entry into the tree. Returns false if key already exists.
	[[nodiscard]] bool insert(const KeyT &key, const ValueT &value);
	/// The number of values stored in the map.
	size_t size() { return map.size(); }
	/// Clears the map.
	void clear() { map.clear(); }
	/// Sets the height in the stats. Does nothing for a map.
	void set_height() { stats.b_tree_height = 0; }

  private:
	/// The map with the values.
	std::unordered_map<KeyT, ValueT> map;
};
} // namespace bbbtree