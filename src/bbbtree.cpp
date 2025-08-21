#include "bbbtree/bbbtree.h"

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
bool DeltaTree<KeyT, ValueT>::before_unload(const BufferFrame &frame) {
	// TODO: When we decide to write out because
	// write amplification is low,
	// we must clean the slots of their dirty state here.

	if (frame.is_new())
		return true;

	assert(frame.is_dirty()); // Must be dirty and not new. If
							  // its new its forced out since we
							  // need a valid header on disk.

	// Get node type: Leaf or Inner Node.
	const auto *node =
		reinterpret_cast<const BTree<KeyT, ValueT, true>::InnerNode *>(
			frame.get_data());

	if (node->is_leaf()) {
		const auto *leaf =
			reinterpret_cast<const BTree<KeyT, ValueT, true>::LeafNode *>(node);

		// TODO: Maybe outsource the following to the deltas class. Given
		// the active variant the pointer is interpreted accordingly.

		//  Scan all slots in the node. Create deltas for each dirty slot.
		LeafDeltas deltas{};
		for (const auto *slot = leaf->slots_begin(); slot < leaf->slots_end();
			 ++slot) {
			deltas.emplace_back(slot->state, slot->get_key(leaf->get_data()),
								slot->get_value(leaf->get_data()));
		}
		if (deltas.empty())
			return true;

		// Store in delta tree.
		auto success = this->insert(frame.get_page_id(), {deltas});

		if (!success)
			throw std::logic_error(
				"DeltaTree: Failed to insert deltas for page " +
				std::to_string(frame.get_page_id()) + " into delta tree.");

	} else {
		// Scan all slots in the node. Create deltas for each dirty slot.
		InnerNodeDeltas deltas{};
		for (const auto *slot = node->slots_begin(); slot < node->slots_end();
			 ++slot) {
			deltas.emplace_back(slot->state, slot->get_key(node->get_data()),
								slot->child);
		}
		if (deltas.empty())
			return true;

		// Store in delta tree.
		auto success = this->insert(frame.get_page_id(), {deltas});
		if (!success)
			throw std::logic_error(
				"DeltaTree: Failed to insert deltas for page " +
				std::to_string(frame.get_page_id()) + " into delta tree.");
	}

	return false;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::after_load(const BufferFrame &frame) {
	// Perform any necessary actions after loading the page.
	std::cout << "After loading page..." << frame.get_page_id() << std::endl;
}
template <KeyIndexable KeyT, ValueIndexable ValueT>
void BBBTree<KeyT, ValueT>::print() {
	std::cout << "B-Tree:" << std::endl;
	btree.print();
	std::cout << "Delta Tree:" << std::endl;
	delta_tree.print();
}
// -----------------------------------------------------------------
// Explicit instantiations
template class BBBTree<UInt64, TID>;
template class BBBTree<String, TID>;
} // namespace bbbtree
