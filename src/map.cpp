#include "bbbtree/map.h"
#include "bbbtree/types.h"

#include <optional>

namespace bbbtree {
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT, bool UseDeltaTree>
std::optional<ValueT> Map<KeyT, ValueT, UseDeltaTree>::lookup(const KeyT &key) {
	auto it = map.find(key);
	if (it == map.end())
		return {};

	return {it->second};
}
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT, bool UseDeltaTree>
void Map<KeyT, ValueT, UseDeltaTree>::erase(const KeyT &key) {
	map.erase(key);
}
// -----------------------------------------------------------------
template <typename KeyT, typename ValueT, bool UseDeltaTree>
bool Map<KeyT, ValueT, UseDeltaTree>::insert(const KeyT &key,
											 const ValueT &value) {

	auto [it, success] = map.try_emplace(key, value);

	return success;
}
// -----------------------------------------------------------------
// Explicit instantiations
template class Map<UInt64, TID>;
template class Map<String, TID>;
// -----------------------------------------------------------------
} // namespace bbbtree