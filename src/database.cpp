#include "bbbtree/database.h"
#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/map.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace bbbtree {
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
Database<IndexT, KeyT>::Database(size_t page_size, size_t num_pages,
								 uint16_t wa_threshold, bool reset)
	: buffer_manager(page_size, num_pages, reset),
	  space_inventory(FSI_SEGMENT_ID, buffer_manager),
	  records(SP_SEGMENT_ID, buffer_manager, space_inventory),
	  index(INDEX_SEGMENT_ID, buffer_manager, wa_threshold) {}
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
void Database<IndexT, KeyT>::insert(const Tuple &tuple) {
	// Get a new TID
	auto tid = records.allocate(tuple.size());
	// Add TID to index
	auto success = index.insert(tuple.key, tid);
	if (!success) {
		records.erase(tid);
		throw std::logic_error(
			"Database<IndexT>::insert(): Key already in database.");
	}
	// Insert tuple in records
	records.write(tid, reinterpret_cast<const std::byte *>(&tuple),
				  tuple.size());
	stats.num_insertions_db++;
}
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
void Database<IndexT, KeyT>::insert(const std::vector<Tuple> &tuples) {
	// TODO: Detect sequential inserts?
	for (auto &tuple : tuples)
		insert(tuple);
}
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
Database<IndexT, KeyT>::Tuple Database<IndexT, KeyT>::get(const KeyT &key) {
	stats.num_lookups_db++;

	// Get TID for key
	auto maybe_tid = index.lookup(key);
	if (!maybe_tid.has_value())
		throw std::logic_error("Database::get(): Key not found.");
	auto tid = maybe_tid.value();

	// Get Tuple
	Tuple tuple{};
	auto bytes_read =
		records.read(tid, reinterpret_cast<std::byte *>(&tuple), tuple.size());

	if (bytes_read != tuple.size())
		throw std::logic_error("Database<IndexT>::get(): Read corrupted.");

	return tuple;
}
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
void Database<IndexT, KeyT>::update(const Tuple &tuple) {
	// Get TID for key
	auto maybe_tid = index.lookup(tuple.key);
	if (!maybe_tid.has_value())
		throw std::logic_error("Database::update(): Key not found.");
	auto tid = maybe_tid.value();
	// Update tuple in records
	records.write(tid, reinterpret_cast<const std::byte *>(&tuple),
				  tuple.size());
	stats.num_updates_db++;
	// Update index
	index.update(tuple.key, tid);
}
// -----------------------------------------------------------------
template <template <typename, typename, bool> typename IndexT, typename KeyT>
	requires IndexInterface<IndexT, KeyT>
void Database<IndexT, KeyT>::erase(const KeyT & /*key*/) {
	// TODO. Also update stats here.
	++stats.num_deletions_db;
	throw std::logic_error("Database<IndexT>::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
// Explicit instantiations
template class Database<BTree, UInt64>;
template class Database<BTree, String>;
template class Database<Map, UInt64>;
template class Database<Map, String>;
template class Database<BBBTree, UInt64>;
template class Database<BBBTree, String>;
} // namespace bbbtree