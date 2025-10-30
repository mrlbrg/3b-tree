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
bool DeltaTree<KeyT, ValueT>::before_unload(char *data, const State &state,
											PageID page_id, size_t page_size) {
#ifndef NDEBUG
	logger.log("DeltaTree::before_unload(): page " + std::to_string(page_id) +
			   " state " +
			   (state == State::DIRTY
					? "DIRTY"
					: (state == State::NEW ? "NEW" : "CLEAN")));
#endif

	// Clean the slots of their dirty state when writing the node out.
	auto *node = reinterpret_cast<const Node *>(data);
	// New pages are always written out.
	// Pages with many updates are always written out.
	bool has_many_updates = node->get_update_ratio(page_size) > wa_threshold;

	bool is_new = (state == State::NEW);
	bool force_write_out =
		!this->buffering_enabled || is_new || has_many_updates || is_locked;
	// logger.log(std::to_string(wa_threshold * 100) + "," +
	// 		   std::to_string(node->get_update_ratio(page_size) * 100) + "," +
	// 		   std::to_string(page_id) + "," + (force_write_out ? "1" : "0"));

	// Erase any buffered deltas for this page, since we are going to write it
	// out now. If the tree is already locked, we cannot modify it now.
	if (is_locked) {
		deferred_deletions.push_back(page_id);
	} else if (has_many_updates) {
		is_locked = true;
		this->erase(page_id, page_size);
		is_locked = false;
	}

	// Remove any tracking information on the node, since we are going to write
	// it out now.
	if (force_write_out) {
		clean_node(reinterpret_cast<Node *>(data));
		return true;
	}

	is_locked = true;

	assert(state == State::DIRTY);
	assert(node->num_bytes_changed > 0);

	// TODO: Do not erase when there is nothing. Just allow making an insert
	// that updates the value if the key exists already i.e. upsert.
	this->erase(page_id, page_size);

	//  Scan all slots in the node and insert the deltas in the delta tree.
	store_deltas(page_id, reinterpret_cast<const Node *>(data));

	// Apply any deferred deletions now.
	erase_deferred_deletions();

	is_locked = false;

	return false;
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
		// Apply any deferred deletions now.
		erase_deferred_deletions();
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

	// Apply any deferred deletions now.
	erase_deferred_deletions();

	is_locked = false;
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
template <typename NodeT>
void DeltaTree<KeyT, ValueT>::clean_node(NodeT *node) {
	node->num_bytes_changed = 0;
	for (auto *slot = node->slots_begin(); slot < node->slots_end(); ++slot)
		slot->set_state(OperationType::Unchanged);
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
		switch (slot->get_state()) {
		case OperationType::Unchanged:
			continue;
		case OperationType::Inserted:
			deltas.emplace_back(slot->get_state(),
								slot->get_key(node->get_data()),
								slot->get_value(node->get_data()));
			break;
		case OperationType::Updated:
			deltas.emplace_back(slot->get_state(),
								slot->get_key(node->get_data()),
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
	auto apply_delta = [](const auto &delta, NodeT *node, size_t page_size) {
		auto &[entry, op_type] = delta;
		auto &[key, value] = entry;
		switch (op_type) {
		case OperationType::Inserted: {
			if (!node->has_space(key, value))
				node->compactify(page_size);
			assert(node->has_space(key, value));
			auto success = node->insert(key, value);
			assert(success);
			break;
		}
		case OperationType::Updated:
			node->update(key, value);
			break;
		default:
			throw std::logic_error("DeltaTree::after_load(): "
								   "Operation Type not implemented "
								   "yet.");
		}
	};

	// Sanity Check: There must have been either deltas or node splits.
	assert(!deltas.empty() || node->slot_count != slot_count);

	// Analyze delta stream to determine cut-off point.
	auto cut_off = slot_count;
	for (size_t i = 0; i < deltas.size(); ++i) {
		auto &[entry, op] = deltas[i];
		switch (op) {
		case OperationType::Inserted:
			--cut_off;
			break;
		default:
			break;
		}
	}

	// Remove split off slots.
	node->slot_count = cut_off;

	// Apply deltas to the node.
	for (size_t i = 0; i < deltas.size(); ++i) {
		auto &[entry, op] = deltas[i];
		apply_delta(deltas[i], node, this->buffer_manager.page_size);
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void DeltaTree<KeyT, ValueT>::erase_deferred_deletions() {
	assert(is_locked);
	while (!deferred_deletions.empty()) {
		auto page_id = deferred_deletions.back();
		deferred_deletions.pop_back();
		this->erase(page_id, this->buffer_manager.page_size);
	}
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
