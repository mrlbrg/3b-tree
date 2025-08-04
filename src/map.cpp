#include "bbbtree/map.h"
#include "bbbtree/tuple_id.h"
#include "bbbtree/types.h"

#include <optional>

namespace bbbtree {
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT>
std::optional<ValueT> Map<KeyT, ValueT>::lookup(const KeyT &key) {
	auto it = map.find(key);
	if (it == map.end())
		return {};

	return {it->second};
}
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT>
void Map<KeyT, ValueT>::erase(const KeyT &key) {
	map.erase(key);
}
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT>
bool Map<KeyT, ValueT>::insert(const KeyT &key, const ValueT &value) {

	auto [it, success] = map.try_emplace(key, value);

	return success;
}
// -----------------------------------------------------------------
// Explicit instantiations
template class Map<UInt64, TID>;
// -----------------------------------------------------------------
} // namespace bbbtree