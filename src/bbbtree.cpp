#include "bbbtree/bbbtree.h"
#include "bbbtree/buffer_manager.h"
#include <string>

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
bool DeltaTree<KeyT, ValueT>::before_unload(char *data, const State &state,
											PageID page_id) {
	// TODO: When we return true to continue to write out because
	// write amplification is low,
	// we must clean the slots of their dirty state here.

	// Clean the slots of their dirty state when writing the node out.
	if (state == State::NEW) {
		clean_node(reinterpret_cast<Node *>(data));
		return true;
	}

	assert(state == State::DIRTY);

	// TODO: When `page_id` already has an entry in the delta tree, we should
	// remove it first here.

	//  Scan all slots in the node and insert the deltas in the delta tree.
	store_deltas(page_id, reinterpret_cast<const Node *>(data));

	return false;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::after_load(char *data, PageID page_id) {
	// Load the deltas for this page
	auto maybe_deltas = this->lookup(page_id);
	// No deltas found. Do nothing.
	if (!maybe_deltas.has_value())
		return;

	// Apply the deltas to the node.
	auto deltas = maybe_deltas.value();
	auto *node = reinterpret_cast<Node *>(data);
	assert(deltas.num_deltas() > 0);

	if (node->is_leaf())
		apply_deltas(reinterpret_cast<LeafNode *>(node),
					 std::get<LeafDeltas>(deltas.deltas));
	else
		apply_deltas(reinterpret_cast<InnerNode *>(node),
					 std::get<InnerNodeDeltas>(deltas.deltas));

	// Remove the page from the delta tree.
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT>
void DeltaTree<KeyT, ValueT>::clean_node(NodeT *node) {
	for (auto *slot = node->slots_begin(); slot < node->slots_end(); ++slot)
		slot->state = OperationType::Unchanged;
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
		if (slot->state == OperationType::Inserted)
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
			"DeltaTree: Failed to insert deltas for PageID " +
			std::to_string(page_id) +
			" into delta tree. An entry already exists for this page.");
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT, typename DeltasT>
void DeltaTree<KeyT, ValueT>::apply_deltas(NodeT *node, const DeltasT &deltas) {
	for (auto &[entry, op_type] : deltas) {
		auto &[key, value] = entry;
		if (op_type == OperationType::Inserted) {
			auto success = node->insert(key, value);
			if (!success)
				throw std::logic_error(
					"DeltaTree::after_load(): Failed to apply delta to node. "
					"Key already exists.");
			continue;
		}
		throw std::logic_error("DeltaTree::after_load(): Operation "
							   "Type not implemented yet.");
	}
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
