#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/logger.h"
#include "bbbtree/stats.h"

#include <cstdint>
#include <cstring>

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::pair<bool, bool>
DeltaTree<KeyT, ValueT>::before_unload(char *data, const State &state,
									   PageID page_id, size_t page_size) {
	// TODO: When we return true to continue to write out because
	// write amplification is low,
	// we must clean the slots of their dirty state too.
	// TODO: When the page is actually written out, we need to erase it from
	// the delta tree and set the `num_bytes_changed` on the node to 0.
	// logger.log("DeltaTree::before_unload(): page " + std::to_string(page_id)
	// + 		   " state " + 		   (state == State::DIRTY 				?
	// "DIRTY" 				: (state == State::NEW ? "NEW" : "CLEAN")));
	if (is_locked)
		// throw std::logic_error("DeltaTree::before_unload(): Re-entrant
		// call");
		return {false, false}; // Do not allow unload when already
							   // locked.

	is_locked = true;

	// Clean the slots of their dirty state when writing the node out.
	auto *node = reinterpret_cast<const Node *>(data);
	// New pages are always written out.
	// Pages with many updates are always written out.
	bool has_many_updates = node->get_update_ratio(page_size) > wa_threshold;
	bool is_new = (state == State::NEW);
	bool force_write_out = is_new || has_many_updates;

	// Erase any buffered deltas for this page, since we are going to write it
	// out now.
	if (has_many_updates)
		this->erase(page_id, page_size);

	// Remove any tracking information on the node, since we are going to write
	// it out now.
	if (force_write_out) {
		clean_node(reinterpret_cast<Node *>(data));
		is_locked = false;
		return {true, true};
	}

	assert(state == State::DIRTY);
	assert(node->num_bytes_changed > 0);

	// TODO: Do not erase when there is nothing. Just allow making an insert
	// that updates the value if the key exists already i.e. upsert.
	this->erase(page_id, page_size);

	//  Scan all slots in the node and insert the deltas in the delta tree.
	store_deltas(page_id, reinterpret_cast<const Node *>(data));
	is_locked = false;

	++stats.pages_write_deferred;
	return {true, false};
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::after_load(char *data, PageID page_id) {
	// TODO: Return if after_load
	assert(!is_locked);

	is_locked = true;
	// Load the deltas for this page
	auto maybe_deltas = this->lookup(page_id);
	// No deltas found. Do nothing.
	if (!maybe_deltas.has_value()) {
		is_locked = false;
		return;
	}

	// Apply the deltas to the node.
	auto deltas = maybe_deltas.value();
	auto *node = reinterpret_cast<Node *>(data);

	if (node->is_leaf()) {
		apply_deltas(reinterpret_cast<LeafNode *>(node),
					 std::get<LeafDeltas>(deltas.deltas), deltas.slot_count);
	} else {
		auto *inner_node = reinterpret_cast<InnerNode *>(node);
		apply_deltas(inner_node, std::get<InnerNodeDeltas>(deltas.deltas),
					 deltas.slot_count);
		inner_node->upper = deltas.upper;
	}

	is_locked = false;
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT>
void DeltaTree<KeyT, ValueT>::clean_node(NodeT *node) {
	node->num_bytes_changed = 0;
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
void DeltaTree<KeyT, ValueT>::extract_deltas(const NodeT *node,
											 DeltasT &deltas) {
	for (const auto *slot = node->slots_begin(); slot < node->slots_end();
		 ++slot) {
		switch (slot->state) {
		case OperationType::Unchanged:
			continue;
		case OperationType::Inserted:
			deltas.emplace_back(slot->state, slot->get_key(node->get_data()),
								slot->get_value(node->get_data()));
			break;
		case OperationType::Updated:
			assert(!node->is_leaf()); // Not supporting leaf node updates yet.
			deltas.emplace_back(slot->state, slot->get_key(node->get_data()),
								slot->get_value(node->get_data()));
			break;
		default:
			throw std::logic_error("DeltaTree::extract_deltas(): Unknown "
								   "operation type in slot.");
		}
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::store_deltas(PageID page_id, const Node *node) {
	bool success = false;
	if (node->is_leaf()) {
		LeafDeltas deltas{};
		auto *leaf = reinterpret_cast<const LeafNode *>(node);
		extract_deltas(leaf, deltas);
		success = this->insert(std::move(page_id),
							   {std::move(deltas), leaf->slot_count});
	} else {
		InnerNodeDeltas deltas{};
		auto *inner_node = reinterpret_cast<const InnerNode *>(node);
		extract_deltas(inner_node, deltas);
		success = this->insert(
			std::move(page_id),
			{std::move(deltas), inner_node->upper, inner_node->slot_count});
	}
	assert(success);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT, typename DeltasT>
void DeltaTree<KeyT, ValueT>::apply_deltas(NodeT *node, const DeltasT &deltas,
										   uint16_t slot_count) {
	auto apply_delta = [](const auto &delta, NodeT *node) {
		auto &[entry, op_type] = delta;
		auto &[key, value] = entry;
		if (op_type == OperationType::Inserted) {
			auto success = node->insert(key, value);
			assert(success);
		} else if (op_type == OperationType::Updated) {
			assert(!node->is_leaf()); // Not supporting leaf node updates yet.
			node->update(key, value);
		} else {
			throw std::logic_error("DeltaTree::after_load(): "
								   "Operation Type not implemented "
								   "yet.");
		}
	};

	auto out_of_place_apply = [&](const size_t page_size, size_t deltas_i) {
		// Get the space we need temporarily.
		size_t insert_size = 0;
		for (auto i = deltas_i; i < deltas.size(); ++i) {
			auto &[entry, op_type] = deltas[i];
			auto &[key, value] = entry;
			insert_size += node->required_space(key, value);
		}
		// Create a buffer for the new node.
		std::vector<std::byte> buffer{page_size + insert_size};
		std::memcpy(buffer.data(), node->get_data(), buffer.size());
		auto *buffered_node = reinterpret_cast<NodeT *>(buffer.data());
		// Make space for insert deltas.
		buffered_node->compactify(buffer.size());
		// Insert deltas.
		for (auto i = deltas_i; i < deltas.size(); ++i)
			apply_delta(deltas[i], buffered_node);
		// Cut off all split slots.
		buffered_node->slot_count = slot_count;
		// Have to compactify again to make sure everything fits when shrinking.
		buffered_node->compactify(buffer.size());
		// Shrink node to original size.
		buffered_node->shrink(buffer.size(), page_size);
		// Copy back to original node.
		std::memcpy(node, buffered_node, page_size);
	};

	// Sanity Check: There must have been either inserts or node splits.
	assert(!deltas.empty() || node->slot_count != slot_count);

	// Insert deltas into the node.
	for (size_t i = 0; i < deltas.size(); ++i) {
		auto &[entry, op] = deltas[i];
		if (node->has_space(entry.key, entry.value)) {
			apply_delta(deltas[i], node);
		} else {
			// No space left on the node. Apply the rest of the deltas
			// out-of-place and compactify to fit the node again.
			out_of_place_apply(this->buffer_manager.page_size, i);
			return;
		}
	}

	// Cut off the slots that were split.
	node->slot_count = slot_count;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::ostream &operator<<(std::ostream &os,
						 const BBBTree<KeyT, ValueT, UseDeltaTree> &type) {
	os << "B-Tree:" << std::endl;
	os << type.btree << std::endl;
	os << "Delta Tree" << std::endl;
	os << type.delta_tree << std::endl;

	return os;
}
// -----------------------------------------------------------------
// Explicit instantiations
template class BBBTree<UInt64, TID>;
template class BBBTree<String, TID>;
template std::ostream &operator<<(std::ostream &, const BBBTree<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &, const BBBTree<String, TID> &);
// -----------------------------------------------------------------
} // namespace bbbtree
