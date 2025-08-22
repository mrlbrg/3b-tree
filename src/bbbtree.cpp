#include "bbbtree/bbbtree.h"

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
bool DeltaTree<KeyT, ValueT>::before_unload(BufferFrame &frame) {
	// TODO: When we return true to continue to write out because
	// write amplification is low,
	// we must clean the slots of their dirty state here.

	// Clean the slots of their dirty state when writing the node out.
	if (frame.is_new()) {
		clean_node(reinterpret_cast<Node *>(frame.get_data()));
		return true;
	}

	assert(frame.is_dirty());

	//  Scan all slots in the node and insert the deltas in the delta tree.
	store_deltas(frame.get_page_id(),
				 reinterpret_cast<const Node *>(frame.get_data()));

	return false;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::after_load(const BufferFrame &frame) {
	// Perform any necessary actions after loading the page.
	std::cout << "After loading page..." << frame.get_page_id() << std::endl;
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT>
void DeltaTree<KeyT, ValueT>::clean_node(NodeT *node) {
	for (auto *slot = node->slots_begin(); slot < node->slots_end(); ++slot)
		slot->state = OperationType::None;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::clean_node(Node *node) {
	if (node->is_leaf())
		clean_node(reinterpret_cast<LeafNode *>(node));
	else
		clean_node(reinterpret_cast<InnerNode *>(node));
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT, typename DeltasT>
DeltasT DeltaTree<KeyT, ValueT>::extract_deltas(const NodeT *node,
												DeltasT &&deltas) {
	for (const auto *slot = node->slots_begin(); slot < node->slots_end();
		 ++slot) {
		if (slot->state == OperationType::Insert)
			deltas.emplace_back(slot->state, slot->get_key(node->get_data()),
								slot->get_value(node->get_data()));
	}

	assert(deltas.empty() == false);

	return std::move(deltas);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::store_deltas(PID &&page_id, const Node *node) {
	std::variant<LeafDeltas, InnerNodeDeltas> deltas;

	if (node->is_leaf())
		deltas = extract_deltas(reinterpret_cast<const LeafNode *>(node),
								LeafDeltas{});
	else
		deltas = extract_deltas(reinterpret_cast<const InnerNode *>(node),
								InnerNodeDeltas{});

	auto success = this->insert(std::move(page_id), std::move(deltas));

	if (!success)
		throw std::logic_error(
			"DeltaTree: Failed to insert deltas for page into delta tree.");
}
// -----------------------------------------------------------------
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
// -----------------------------------------------------------------
} // namespace bbbtree
