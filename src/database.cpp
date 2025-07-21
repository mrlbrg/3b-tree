#include "bbbtree/database.h"
#include "bbbtree/btree.h"

#include <cstddef>
#include <stdexcept>

namespace bbbtree {
// -----------------------------------------------------------------
template <template <typename, typename> typename IndexT, typename KeyT>
	requires IndexC<IndexT, KeyT>
Database<IndexT, KeyT>::Database(size_t page_size, size_t num_pages, bool reset)
	: buffer_manager(page_size, num_pages, reset),
	  space_inventory(FSI_SEGMENT_ID, buffer_manager),
	  records(SP_SEGMENT_ID, buffer_manager, space_inventory),
	  index(INDEX_SEGMENT_ID, buffer_manager) {
	index.print();
}
// -----------------------------------------------------------------
template <template <typename, typename> typename IndexT, typename KeyT>
	requires IndexC<IndexT, KeyT>
void Database<IndexT, KeyT>::insert(const Tuple &tuple) {
	// Get a new TID
	auto tid = records.allocate(sizeof(tuple));
	// Add TID to index
	auto success = index.insert(tuple.key, tid);
	if (!success) {
		records.erase(tid);
		throw std::logic_error(
			"Database<IndexT>::insert(): Key already in database.");
	}
	// Insert tuple in records
	records.write(tid, reinterpret_cast<const std::byte *>(&tuple),
				  sizeof(tuple));
}
// -----------------------------------------------------------------
template <template <typename, typename> typename IndexT, typename KeyT>
	requires IndexC<IndexT, KeyT>
void Database<IndexT, KeyT>::insert(const std::vector<Tuple> &tuples) {
	// TODO: Detect sequential inserts?
	for (auto &tuple : tuples)
		insert(tuple);
}
// -----------------------------------------------------------------
template <template <typename, typename> typename IndexT, typename KeyT>
	requires IndexC<IndexT, KeyT>
Database<IndexT, KeyT>::Tuple Database<IndexT, KeyT>::get(const KeyT &key) {
	// Get TID for key
	auto maybe_tid = index.lookup(key);
	if (!maybe_tid.has_value())
		throw std::logic_error("Database::get(): Key not found.");
	auto tid = maybe_tid.value();

	// Get Tuple
	Tuple tuple{};
	auto bytes_read =
		records.read(tid, reinterpret_cast<std::byte *>(&tuple), sizeof(tuple));

	if (bytes_read != sizeof(tuple))
		throw std::logic_error("Database<IndexT>::get(): Read corrupted.");

	return tuple;
}
// -----------------------------------------------------------------
template <template <typename, typename> typename IndexT, typename KeyT>
	requires IndexC<IndexT, KeyT>
void Database<IndexT, KeyT>::erase(const KeyT & /*key*/) {
	// TODO
	throw std::logic_error("Database<IndexT>::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
// Explicit instantiations
// template class Database<std::unordered_map, uint64_t>;
template class Database<BTree, uint64_t>;
} // namespace bbbtree