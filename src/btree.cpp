#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/delta.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::BTree(SegmentID segment_id,
										 BufferManager &buffer_manager,
										 PageLogic *page_logic)
	: Segment(segment_id, buffer_manager), page_logic(page_logic) {

	// Sanity Check.
	if constexpr (UseDeltaTree) {
		assert(page_logic &&
			   "When delta tree is enabled, `page_logic` must be provided");
	}

	// TODO: Create some meta-data segment that stores information like the
	// root's page id on file.
	auto &frame = buffer_manager.fix_page(segment_id, 0, false);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
		frame.get_data()));

	// Load meta-data into memory.
	root = state.root;
	next_free_page = state.next_free_page;

	bool is_dirty = false;

	// Start a new tree?
	if (next_free_page == 0) {
		root = 1;
		next_free_page = 2;
		state.root = root;
		state.next_free_page = next_free_page;
		// Intialize root node.
		auto &root_page =
			buffer_manager.fix_page(segment_id, root, true, page_logic);
		new (root_page.get_data()) LeafNode(buffer_manager.page_size);
		buffer_manager.unfix_page(root_page, true);
		is_dirty = true;
	}

	buffer_manager.unfix_page(frame, is_dirty);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::~BTree() {}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::optional<ValueT>
BTree<KeyT, ValueT, UseDeltaTree>::lookup(const KeyT &key) {
	auto &leaf_frame = get_leaf(key, false);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// TODO: Not thread-safe.
	auto result = leaf.lookup(key);

	buffer_manager.unfix_page(leaf_frame, false);

	return result;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::erase(const KeyT & /*key*/) {
	throw std::logic_error("BTree::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BufferFrame &BTree<KeyT, ValueT, UseDeltaTree>::get_leaf(const KeyT &key,
														 bool exclusive) {
	// TODO: When multithreading, we only want the leaf to be locked
	// exclusively. Must not lock all exclusively on the path here. Start
	// with inexclusively reading root. Check if it's a leaf. If so, restart
	// but take exclusive lock and return.
	auto *frame =
		&buffer_manager.fix_page(segment_id, root, exclusive, page_logic);
	auto *node = reinterpret_cast<InnerNode *>(frame->get_data());

	while (!node->is_leaf()) {
		// Acquire child.
		auto child_id = node->lookup(key);
		auto *child_frame =
			&buffer_manager.fix_page(segment_id, child_id, exclusive);

		// Release parent.
		buffer_manager.unfix_page(*frame, false);

		frame = child_frame;
		node = reinterpret_cast<InnerNode *>(child_frame->get_data());
	}

	// Returns the leaf's locked frame.
	return *frame;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::insert(const KeyT &key,
											   const ValueT &value) {
	// Sanity check
	{
		size_t page_size = buffer_manager.page_size;
		size_t required_leaf_size = key.size() + value.size();
		size_t required_node_size = key.size();
		if ((required_leaf_size > page_size - LeafNode::min_space) ||
			(required_node_size > page_size - InnerNode::min_space))
			throw std::logic_error("BTree::insert(): Key too large.");
	}

restart:
	// TODO: This frame is not locked exclusively.
	auto &leaf_frame = get_leaf(key, true);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// Node split?
	if (!leaf.has_space(key, value)) {
		// Release locks. Split will acquire its own. Re-acquire lock after
		// split by traversing to leaf again.
		buffer_manager.unfix_page(leaf_frame, false);
		split(key, value);
		goto restart;
	}

	auto success = leaf.insert(key, value);
	buffer_manager.unfix_page(leaf_frame, true);

	if (!success)
		return false;

	assert(lookup(key).has_value());
	assert(lookup(key).value() == value);

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
PageID BTree<KeyT, ValueT, UseDeltaTree>::get_new_page() {
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
		frame.get_data()));

	auto page_id = next_free_page;
	++next_free_page;
	state.next_free_page = next_free_page;

	buffer_manager.unfix_page(frame, true);

	return page_id;
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::split(const KeyT &key,
											  const ValueT &value) {
	using Pivot = std::pair<const KeyT, const PageID>;

	// No frames are to be held at this point.
	while (true) {
		auto *curr_frame = &buffer_manager.fix_page(segment_id, root, true);
		auto *curr_node = reinterpret_cast<InnerNode *>(curr_frame->get_data());
		// All nodes that lie on the path to the key with the leaf in the back
		// and root in front.
		std::deque<BufferFrame *> path{curr_frame};
		// All nodes we touch are locked. Must be released in the end.
		std::vector<BufferFrame *> locked_nodes{curr_frame};

		// Collect all nodes on path to leaf for given key.
		while (!curr_node->is_leaf()) {
			curr_frame = &buffer_manager.fix_page(segment_id,
												  curr_node->lookup(key), true);
			path.push_front(curr_frame);
			locked_nodes.push_back(curr_frame);
			curr_node = reinterpret_cast<InnerNode *>(curr_frame->get_data());
		}
		assert(path.size() >= 1);
		auto max_level = path.size() - 1;

		// We stop when the target leaf fits the new key-value-pair.
		auto *leaf_frame = path.at(0);
		auto *leaf = reinterpret_cast<LeafNode *>(leaf_frame->get_data());
		if (leaf->has_space(key, value)) {
			// Release path. TODO: Don't mark all as dirty.
			for (auto *frame : locked_nodes)
				// Don't mark dirty here. Done while splitting.
				buffer_manager.unfix_page(*frame, false);
			break;
		}

		// Split leaf.
		const auto new_pid = get_new_page();
		auto *new_leaf_frame =
			&buffer_manager.fix_page(segment_id, new_pid, true);
		auto *new_leaf = (new (new_leaf_frame->get_data())
							  LeafNode(buffer_manager.page_size));
		const auto pivot =
			leaf->split(*new_leaf, key, buffer_manager.page_size);

		new_leaf_frame->set_dirty();
		leaf_frame->set_dirty();

		locked_nodes.push_back(new_leaf_frame);
		std::vector<Pivot> insertion_queue{{pivot, new_pid}};

		size_t curr_level = 1;
		// Propagate splits along path.
		while (!insertion_queue.empty()) {
			// Arrived at root? Create new root.
			if (curr_level > max_level) {
				auto old_root = root;
				auto new_root = get_new_page();

				auto &frame = buffer_manager.fix_page(segment_id, 0, true);
				auto &state =
					*(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
						frame.get_data()));
				root = new_root;
				state.root = new_root;
				buffer_manager.unfix_page(frame, true);

				auto *root_frame =
					&buffer_manager.fix_page(segment_id, root, true);
				new (root_frame->get_data())
					InnerNode(buffer_manager.page_size, ++max_level, old_root);
				root_frame->set_dirty();
				locked_nodes.push_back(root_frame);
				path.push_back(root_frame);
				assert(max_level == curr_level);
			}

			// Insert split into parent.
			curr_frame = path.at(curr_level);
			curr_node = reinterpret_cast<InnerNode *>(curr_frame->get_data());
			auto [curr_key, curr_pid] = insertion_queue.back();

			// Split inner node. Moving up.
			if (!curr_node->has_space(curr_key)) {
				// Create new node.
				const auto new_pid = get_new_page();
				auto *new_frame =
					&buffer_manager.fix_page(segment_id, new_pid, true);
				auto *new_node = (new (new_frame->get_data()) InnerNode(
					buffer_manager.page_size, curr_node->level,
					curr_node->get_upper()));
				locked_nodes.push_back(new_frame);
				// Split current node.
				const auto new_pivot =
					curr_node->split(*new_node, buffer_manager.page_size);

				new_frame->set_dirty();
				curr_frame->set_dirty();
				insertion_queue.push_back({new_pivot, new_pid});

				// Update path to insert split.
				if (new_pivot < curr_key)
					path.at(curr_level) = new_frame;
				// Insert this new split directly into parent.
				++curr_level;
				continue;
			}

			// Moving down.
			bool success = curr_node->insert_split(curr_key, curr_pid);
			assert(success);
			curr_frame->set_dirty();
			insertion_queue.pop_back();
			assert(curr_level > 0);
			--curr_level;
		}

		// Release path. TODO: Don't mark all as dirty.
		for (auto *frame : locked_nodes)
			// Don't mark dirty here. Done while splitting.
			buffer_manager.unfix_page(*frame, false);
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::print() {
	// Acquire root to get height of tree.
	auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
	auto *node = reinterpret_cast<
		typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
		root_frame.get_data());
	auto level = node->level;
	buffer_manager.unfix_page(root_frame, false);

	std::cout << std::endl;
	std::cout << "root: " << root << std::endl;
	std::cout << "next_free_page: " << next_free_page << std::endl;

	// Traverse and print each level of nodes.
	std::vector<PageID> nodes_on_current_level{root};

	while (level > 0) {
		std::cout << std::endl;
		std::cout << "################ LEVEL " << level << " ###############"
				  << std::endl;
		// Print current level & collect their children.
		std::vector<PageID> children{};
		for (auto pid : nodes_on_current_level) {
			// Acquire page.
			auto &frame = buffer_manager.fix_page(segment_id, pid, false);
			auto *node = reinterpret_cast<
				typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
				frame.get_data());

			// Sanity Check.
			assert(node->level == level);
			std::cout << "[" << sizeof(*node) << "B] ";
			std::cout << "PID " << pid;
			node->print();
			std::cout << "-----------------------------------------------------"
					  << std::endl;

			// Collect children
			for (auto child : node->get_children()) {
				children.push_back(child);
			}
			children.push_back(node->get_upper());

			// Release page.
			buffer_manager.unfix_page(frame, false);
		}
		nodes_on_current_level = children;
		--level;
	}

	// Traverse leaf level
	std::cout << "################ LEVEL " << level << " ###############"
			  << std::endl;
	for (auto pid : nodes_on_current_level) {
		auto &frame = buffer_manager.fix_page(segment_id, pid, false);
		auto *leaf = reinterpret_cast<
			typename BTree<KeyT, ValueT, UseDeltaTree>::LeafNode *>(
			frame.get_data());
		// Sanity Check.
		assert(leaf->level == level);

		std::cout << "[" << sizeof(*leaf) << "B] ";
		std::cout << "PID " << pid;
		leaf->print();
		std::cout << "-----------------------------------------------------"
				  << std::endl;

		buffer_manager.unfix_page(frame, false);
	}
	std::cout << "########################################################"
			  << std::endl;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
size_t BTree<KeyT, ValueT, UseDeltaTree>::size() {
	// Acquire root to get height of tree.
	auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
	auto *node = reinterpret_cast<
		typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
		root_frame.get_data());
	auto level = node->level;
	buffer_manager.unfix_page(root_frame, false);

	// Traverse each level of nodes.
	std::vector<PageID> nodes_on_current_level{root};
	while (level > 0) {
		// Collect children.
		std::vector<PageID> children{};
		for (auto pid : nodes_on_current_level) {
			// Acquire page.
			auto &frame = buffer_manager.fix_page(segment_id, pid, false);
			auto *node = reinterpret_cast<
				typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
				frame.get_data());

			// Sanity Check.
			assert(node->level == level);
			assert(node->get_upper());

			// Collect children
			for (auto child : node->get_children()) {
				children.push_back(child);
			}
			children.push_back(node->get_upper());

			// Release page.
			buffer_manager.unfix_page(frame, false);
		}
		nodes_on_current_level = children;
		--level;
	}

	// Traverse leaf level
	size_t result = 0;
	for (auto pid : nodes_on_current_level) {
		auto &frame = buffer_manager.fix_page(segment_id, pid, false);
		auto &leaf = *reinterpret_cast<
			typename BTree<KeyT, ValueT, UseDeltaTree>::LeafNode *>(
			frame.get_data());
		// Sanity Check.
		assert(leaf.level == level);

		result += leaf.slot_count;
		buffer_manager.unfix_page(frame, false);
	}

	return result;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
size_t BTree<KeyT, ValueT, UseDeltaTree>::height() {
	auto &frame = buffer_manager.fix_page(segment_id, root, false);
	auto &root_node = *reinterpret_cast<InnerNode *>(frame.get_data());
	size_t height = root_node.level + 1;
	buffer_manager.unfix_page(frame, false);
	return height;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
const KeyT BTree<KeyT, ValueT, UseDeltaTree>::Node::Slot::get_key(
	const std::byte *begin) const {
	assert(key_size);
	assert(offset);
	return KeyT::deserialize(begin + offset, key_size);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::InnerNode(uint32_t page_size,
														uint16_t level,
														PageID upper)
	: Node(page_size, level), upper(upper) {
	// Sanity Check: Node must fit page.
	assert(page_size > sizeof(InnerNode));
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::Pivot *
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::lower_bound(const KeyT &pivot) {
	// Compare keys from two slots
	auto comp = [&](const Pivot &slot, const KeyT &key) -> bool {
		const auto slot_key = slot.get_key(this->get_data());

		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), pivot, comp);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
PageID BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::lookup(const KeyT &pivot) {
	assert(upper);
	auto *slot = lower_bound(pivot);

	if (slot == slots_end()) {
		return upper;
	}

	assert(slot->child > 0);
	return slot->child;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
const KeyT
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::split(InnerNode &new_node,
													size_t page_size) {
	++stats.inner_node_splits;
	// Sanity Check.
	assert(this->slot_count > 0);
	// Buffer the node.
	std::vector<std::byte> buffer{page_size};
	std::memcpy(&buffer[0], this, page_size);
	auto *buffer_node = reinterpret_cast<InnerNode *>(&buffer[0]);

	uint16_t pivot_i = (buffer_node->slot_count + 1) / 2 - 1;

	// First half of slots is reinserted into left leaf to compactiy space.
	// TODO: Make this better.
	this->slot_count = 0;
	this->data_start = page_size;
	const auto *slot_to_copy = buffer_node->slots_begin();
	while (slot_to_copy < buffer_node->slots_begin() + pivot_i + 1) {
		auto success = insert(slot_to_copy->get_key(buffer_node->get_data()),
							  slot_to_copy->child);
		assert(success);
		++slot_to_copy;
	}
	assert(this->slot_count == pivot_i + 1);

	// Second half of slots is inserted into new, right leaf.
	while (slot_to_copy < buffer_node->slots_end()) {
		auto success =
			new_node.insert(slot_to_copy->get_key(buffer_node->get_data()),
							slot_to_copy->child);
		assert(success);
		++slot_to_copy;
	}

	// Set `upper` to right-most slot. Delete slot.
	assert(this->slot_count > 0);
	const auto &pivot_slot = *(slots_end() - 1);
	upper = pivot_slot.child;
	--this->slot_count;
	this->data_start += pivot_slot.key_size;
	assert(this->data_start <= page_size);

	// Returning a reference to a deleted slot. Use with care.
	return pivot_slot.get_key(this->get_data());
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::insert_split(
	const KeyT &new_pivot, PageID new_child) {
	// Sanity checks.
	assert(has_space(new_pivot));
	assert(upper);

	auto *slot_target = lower_bound(new_pivot);
	PageID old_child;
	if (slot_target == slots_end()) {
		// Target slot is rightmost `upper`.
		old_child = upper;
		upper = new_child;
		// Insert slot with <new_pivot, old_upper>
	} else {
		const auto found_pivot = slot_target->get_key(this->get_data());
		// Keys must be unique. We don't throw here because we don't manage
		// the lock.
		if (found_pivot == new_pivot)
			return false;

		old_child = slot_target->child;
		slot_target->child = new_child;
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > slot_target; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	// Insert new slot.
	this->data_start -= new_pivot.size();
	++this->slot_count;
	*slot_target =
		Pivot{this->get_data(), this->data_start, new_pivot, old_child};
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::insert(const KeyT &new_pivot,
														  PageID new_child) {
	// Sanity checks.
	assert(has_space(new_pivot));

	assert(upper);

	// Find target position for new pivotal slot.
	auto *slot_target = lower_bound(new_pivot);
	if (slot_target != slots_end()) {
		const auto found_pivot = slot_target->get_key(this->get_data());
		// Keys must be unique. We don't throw here because we don't manage
		// the lock.
		if (found_pivot == new_pivot)
			return false;
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > slot_target; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	// Insert new slot.
	this->data_start -= new_pivot.size();
	++this->slot_count;
	*slot_target =
		Pivot{this->get_data(), this->data_start, new_pivot, new_child};
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	if constexpr (UseDeltaTree) {
		slot_target->state = OperationType::Insert;
	}

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::vector<PageID>
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::get_children() {
	std::vector<PageID> children{};
	for (uint16_t i = 0; i < this->slot_count; ++i) {
		auto &slot = *(slots_begin() + i);
		children.push_back(slot.child);
	}
	return children;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::print() {
	// Print Header.
	std::cout << "	data_start: " << this->data_start;
	std::cout << ", level: " << this->level;
	std::cout << ", slot_count: " << this->slot_count << std::endl;

	// Print Slots.
	for (const auto *slot = slots_begin(); slot < slots_end(); slot++) {
		slot->print(this->get_data());
	}
	std::cout << "  upper: " << upper << std::endl;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::Pivot::Pivot(
	std::byte *page_begin, uint32_t offset, const KeyT &key, PageID child)
	: Node::Slot(offset, key.size()), child(child) {
	// Store key at offset. Caller must ensure that it has enough space for
	// `key.size()`.
	key.serialize(page_begin + offset);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::Pivot::print(
	const std::byte *begin) const {
	std::cout << "[" << sizeof(*this) << "B + " << this->key_size << "B + "
			  << sizeof(this->child) << "B] ";
	std::cout << "  offset: " << this->offset;
	std::cout << ", key_size: " << this->key_size;
	std::cout << ", pivot: " << this->get_key(begin);
	std::cout << ", child: " << child << std::endl;

	if constexpr (UseDeltaTree) {
		std::cout << "    state: " << this->state << std::endl;
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
const KeyT BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::split(
	LeafNode &new_node, const KeyT &key, size_t page_size) {

	++stats.leaf_node_splits;
	assert(this->slot_count >= 1);
	assert(page_size > 0);
	// Buffer the node.
	std::vector<std::byte> buffer{page_size};
	std::memcpy(&buffer[0], this, page_size);
	auto *buffer_node = reinterpret_cast<LeafNode *>(&buffer[0]);

	// Determine how many keys go left/right.
	// If the new key goes in the left node, move more entries to the new
	// right node. Important when having a tree where each leaf holds only
	// one entry.
	const auto &middle_slot =
		*(buffer_node->slots_begin() + ((buffer_node->slot_count + 1) / 2) - 1);
	const auto &middle_key = middle_slot.get_key(this->get_data());
	bool skew_left = (middle_key < key); // Will key go right?
	uint16_t num_slots_left = skew_left ? (buffer_node->slot_count + 1) / 2
										: (buffer_node->slot_count / 2);

	// First half of slots is reinserted into left leaf to compactiy space.
	// TODO: Make this better.
	this->slot_count = 0;
	this->data_start = page_size;
	const auto *slot_to_copy = buffer_node->slots_begin();
	while (slot_to_copy < buffer_node->slots_begin() + num_slots_left) {
		auto success = insert(slot_to_copy->get_key(buffer_node->get_data()),
							  slot_to_copy->get_value(buffer_node->get_data()));
		if (!success)
			throw std::logic_error("BTree::split(): Insert must succeed.");
		++slot_to_copy;
	}

	// Second half of slots is inserted into new, right leaf.
	while (slot_to_copy < buffer_node->slots_end()) {
		auto success =
			new_node.insert(slot_to_copy->get_key(buffer_node->get_data()),
							slot_to_copy->get_value(buffer_node->get_data()));
		if (!success)
			throw std::logic_error("BTree::split(): Insert must succeed.");

		++slot_to_copy;
	}
	assert(this->slot_count == num_slots_left);

	// If left node is left empty, the key to be inserted will be the new
	// pivotal key in the parent.
	if (this->slot_count == 0)
		return key;

	// Return last slot of left node as new pivot to insert into parent.
	const auto &pivot_slot = *(slots_begin() + this->slot_count - 1);
	return pivot_slot.get_key(this->get_data());
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::LeafSlot *
BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::lower_bound(const KeyT &key) {
	auto comp = [&](const LeafSlot &slot, const KeyT &key) -> bool {
		const auto &slot_key = slot.get_key(this->get_data());
		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), key, comp);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::optional<ValueT>
BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::lookup(const KeyT &key) {
	auto *slot = lower_bound(key);

	if (slot == slots_end())
		return {};

	const auto found_key = slot->get_key(this->get_data());
	if (found_key != key)
		return {};

	return {slot->get_value(this->get_data())};
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::insert(const KeyT &key,
														 const ValueT &value) {
	assert(has_space(key, value));
	// Find insert position.
	auto *slot_target = lower_bound(key);
	if (slot_target != slots_end()) {
		const auto &found_key = slot_target->get_key(this->get_data());
		// Keys must be unique. We don't throw here because we don't manage
		// the lock.
		if (found_key == key)
			return false;
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > slot_target; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	// Insert new slot.
	assert(this->data_start >= key.size() + value.size());
	this->data_start -= (key.size() + value.size());
	++this->slot_count;
	*slot_target = LeafSlot{this->get_data(), this->data_start, key, value};
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	// Track delta.
	if constexpr (UseDeltaTree) {
		slot_target->state = OperationType::Insert;
	}

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::print() {
	// Print Header.
	std::cout << ", data_start: " << this->data_start;
	std::cout << ", level: " << this->level;
	std::cout << ", slot_count: " << this->slot_count << std::endl;

	// Print Slots.
	for (const auto *slot = slots_begin(); slot < slots_end(); ++slot) {
		slot->print(this->get_data());
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::LeafSlot::LeafSlot(
	std::byte *page_begin, uint32_t offset, const KeyT &key,
	const ValueT &value)
	: Node::Slot(offset, key.size()), value_size(value.size()) {
	// Copy key and value into the slot's buffer.
	key.serialize(page_begin + offset);
	value.serialize(page_begin + offset + key.size());
};
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
const ValueT BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::LeafSlot::get_value(
	const std::byte *begin) const {
	assert(value_size);
	// Returns a view to the slot's buffer.
	return ValueT::deserialize(begin + this->offset + this->key_size,
							   value_size);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::LeafSlot::print(
	const std::byte *begin) const {

	std::cout << "[" << sizeof(*this) << "B + " << this->key_size << "B + "
			  << this->value_size << "B] ";
	std::cout << "  offset: " << this->offset;
	std::cout << ", key_size: " << this->key_size;
	std::cout << ", value_size: " << value_size;

	std::cout << ", key: " << this->get_key(begin);
	std::cout << ", value: " << get_value(begin) << std::endl;

	if constexpr (UseDeltaTree) {
		std::cout << "    state: " << this->state << std::endl;
	}
}
std::ostream &operator<<(std::ostream &os, const OperationType &type) {
	switch (type) {
	case OperationType::None:
		os << "None";
		break;
	case OperationType::Insert:
		os << "Insert";
		break;
	case OperationType::Delete:
		os << "Delete";
		break;
	case OperationType::Update:
		os << "Update";
		break;
	default:
		os << "Unknown";
		break;
	}
	return os;
}
// -----------------------------------------------------------------
// Explicit instantiations
template struct BTree<UInt64, TID>;
template struct BTree<UInt64, UInt64>;
template struct BTree<UInt64, String>;
template struct BTree<String, TID>;
template struct BTree<String, UInt64>;
template struct BTree<String, String>;
template struct BTree<UInt64, TID, true>;
template struct BTree<String, TID, true>;
template struct BTree<PID, Deltas<UInt64, TID>>;
template struct BTree<PID, Deltas<String, TID>>;
// --------------------------------------------------------------
} // namespace bbbtree