#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
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
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::BTree(SegmentID segment_id, BufferManager &buffer_manager)
	: Segment(segment_id, buffer_manager) {
	// TODO: Create some meta-data segment that stores information like the
	// root's page id on file.
	auto &frame = buffer_manager.fix_page(segment_id, 0, false);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT> *>(frame.get_data()));

	// Load meta-data into memory.
	root = state.root;
	next_free_page = state.next_free_page;

	// Start a new tree?
	if (next_free_page == 0) {
		root = 1;
		next_free_page = 2;
		// Intialize root node.
		auto &root_page = buffer_manager.fix_page(segment_id, root, true);
		new (root_page.get_data()) LeafNode(buffer_manager.page_size);
		buffer_manager.unfix_page(root_page, true);
	}

	buffer_manager.unfix_page(frame, false);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT> BTree<KeyT, ValueT>::~BTree() {
	// Write out all meta-data needed to pick up index again later.
	// Caller must make sure that Buffer Manager is destroyed after this,
	// not before.
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT> *>(frame.get_data()));
	state.root = root;
	state.next_free_page = next_free_page;
	buffer_manager.unfix_page(frame, true);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
std::optional<ValueT> BTree<KeyT, ValueT>::lookup(const KeyT &key) {
	auto &leaf_frame = get_leaf(key, false);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// TODO: Not thread-safe.
	auto result = leaf.lookup(key);

	buffer_manager.unfix_page(leaf_frame, false);

	return result;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::erase(const KeyT & /*key*/) {
	throw std::logic_error("BTree::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BufferFrame &BTree<KeyT, ValueT>::get_leaf(const KeyT &key, bool exclusive) {
	// TODO: When multithreading, we only want the leaf to be locked
	// exclusively. Must not lock all exclusively on the path here. Start with
	// inexclusively reading root. Check if it's a leaf. If so, restart but take
	// exclusive lock and return.
	auto *frame = &buffer_manager.fix_page(segment_id, root, exclusive);
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
template <Indexable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::insert(const KeyT &key, const ValueT &value) {
	// Sanity check
	{
		auto page_size = buffer_manager.page_size;
		auto required_leaf_size = key.size() + sizeof(value);
		auto required_node_size = key.size();
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
		split(key);
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
template <Indexable KeyT, typename ValueT>
PageID BTree<KeyT, ValueT>::get_new_page() {
	// TODO: Synchronize this when multi-threading.
	auto page_id = next_free_page;
	++next_free_page;

	return page_id;
}

// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::split(const KeyT &key) {
	// No frames are to be held at this point.
	auto *frame = &buffer_manager.fix_page(segment_id, root, true);
	auto *curr_node = reinterpret_cast<InnerNode *>(frame->get_data());
	std::deque<BufferFrame *> locked_path{frame};

	// Collect all nodes on path to leaf for given key.
	while (!curr_node->is_leaf()) {
		frame =
			&buffer_manager.fix_page(segment_id, curr_node->lookup(key), true);
		locked_path.push_back(frame);
		curr_node = reinterpret_cast<InnerNode *>(frame->get_data());
	}
	auto height = locked_path.size();
	// TODO: When multithreading, another thread might have already split.
	// Release everything and do nothing in that case.

	// Split Leaf.
	auto &leaf = *reinterpret_cast<LeafNode *>(curr_node);
	const auto new_pid = get_new_page();
	auto &new_leaf_frame = buffer_manager.fix_page(segment_id, new_pid, true);
	auto &new_leaf =
		*(new (new_leaf_frame.get_data()) LeafNode(buffer_manager.page_size));
	const auto pivot = leaf.split(new_leaf, buffer_manager.page_size);
	locked_path.push_back(&new_leaf_frame);

	assert(height > 0);
	int insertion_level = height - 1; // Leaf level.
	--insertion_level; // Insertion starts above leaf level. Can be -1.

	// Never release or split a page which owns this key from the path before
	// its copied to the parent!
	std::vector<std::pair<const KeyT, const PageID>> insertion_queue{
		{pivot, new_pid}};

	// A new pivot must be inserted into the parent.
	while (!insertion_queue.empty()) {
		// Arrived at root? Create new root.
		if (insertion_level < 0) {
			auto old_root = root;
			root = get_new_page();
			auto &root_frame = buffer_manager.fix_page(segment_id, root, true);
			new (root_frame.get_data())
				InnerNode(buffer_manager.page_size, height++, old_root);
			locked_path.push_front(&root_frame);
			insertion_level = 0;
		}

		assert(0 <= insertion_level);
		assert(insertion_level < static_cast<int>(height));

		auto &frame = *locked_path[insertion_level];
		auto &node = *reinterpret_cast<InnerNode *>(frame.get_data());
		assert(node.level == (height - 1 - insertion_level));

		auto [new_pivot, new_child] = insertion_queue.back();
		insertion_queue.pop_back();

		// Cascarding split?
		if (!node.has_space(new_pivot)) {
			// Create new node.
			const auto newer_child = get_new_page();
			auto &new_frame =
				buffer_manager.fix_page(segment_id, newer_child, true);
			auto &new_node = *(new (new_frame.get_data()) InnerNode(
				buffer_manager.page_size, node.level, node.get_upper()));

			// Split. Must not release any pages since these pivots might
			// reference the node's keys (e.g. String is a view on the key on
			// the node).
			const auto newer_pivot =
				node.split(new_node, buffer_manager.page_size);

			// Update insertion queue.
			insertion_queue.push_back({newer_pivot, newer_child});
			insertion_queue.push_back({new_pivot, new_child});

			// Update lock path: New node might be on path now.
			if (key > newer_pivot) {
				locked_path[insertion_level] = &new_frame;
				locked_path.push_back(&frame);
			} else {
				locked_path.push_back(&new_frame);
			}

			// Retry current insertion.
			continue;
		}

		bool success = node.insert_split(new_pivot, new_child);
		assert(success);
		--insertion_level;
	}

	// Release path. TODO: Don't mark all as dirty.
	for (auto *frame : locked_path) {
		buffer_manager.unfix_page(*frame, true);
	}
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT> void BTree<KeyT, ValueT>::print() {
	// Acquire root to get height of tree.
	auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
	auto *node = reinterpret_cast<typename BTree<KeyT, ValueT>::InnerNode *>(
		root_frame.get_data());
	auto level = node->level;
	buffer_manager.unfix_page(root_frame, false);

	std::cout << "BTree:" << std::endl;
	std::cout << "	root: " << root << std::endl;
	std::cout << "	next_free_page: " << next_free_page << std::endl;

	// Traverse and print each level of nodes.
	std::vector<PageID> nodes_on_current_level{root};

	while (level > 0) {
		std::cout << "################ LEVEL " << level << " ###############"
				  << std::endl;
		// Print current level & collect their children.
		std::vector<PageID> children{};
		for (auto pid : nodes_on_current_level) {
			// Acquire page.
			auto &frame = buffer_manager.fix_page(segment_id, pid, false);
			auto *node =
				reinterpret_cast<typename BTree<KeyT, ValueT>::InnerNode *>(
					frame.get_data());

			// Sanity Check.
			assert(node->level == level);

			std::cout << "PID " << pid << std::endl;
			node->print();

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
		auto *leaf = reinterpret_cast<typename BTree<KeyT, ValueT>::LeafNode *>(
			frame.get_data());
		// Sanity Check.
		assert(leaf->level == level);

		std::cout << "PID " << pid << std::endl;
		leaf->print();

		buffer_manager.unfix_page(frame, false);
	}
	std::cout << "########################################################"
			  << std::endl;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT> size_t BTree<KeyT, ValueT>::size() {
	// Acquire root to get height of tree.
	auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
	auto *node = reinterpret_cast<typename BTree<KeyT, ValueT>::InnerNode *>(
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
			auto *node =
				reinterpret_cast<typename BTree<KeyT, ValueT>::InnerNode *>(
					frame.get_data());

			// Sanity Check.
			assert(node->level == level);
			assert(node->get_upper());
			assert(node->slot_count);

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
		auto &leaf =
			*reinterpret_cast<typename BTree<KeyT, ValueT>::LeafNode *>(
				frame.get_data());
		// Sanity Check.
		assert(leaf.level == level);

		result += leaf.slot_count;
		buffer_manager.unfix_page(frame, false);
	}

	return result;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
size_t BTree<KeyT, ValueT>::height() {
	auto &frame = buffer_manager.fix_page(segment_id, root, false);
	auto &root_node = *reinterpret_cast<InnerNode *>(frame.get_data());
	size_t height = root_node.level + 1;
	buffer_manager.unfix_page(frame, false);
	return height;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
const KeyT
BTree<KeyT, ValueT>::Node::Slot::get_key(const std::byte *begin) const {
	assert(key_size);
	assert(offset);
	return KeyT::deserialize(begin + offset, key_size);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::InnerNode(uint32_t page_size, uint16_t level,
										  PageID upper)
	: Node(page_size, level), upper(upper) {
	// Sanity Check: Node must fit page.
	assert(page_size > sizeof(InnerNode));
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::Pivot *
BTree<KeyT, ValueT>::InnerNode::lower_bound(const KeyT &pivot) {
	// Compare keys from two slots
	auto comp = [&](const Pivot &slot, const KeyT &key) -> bool {
		const auto slot_key = slot.get_key(this->get_data());

		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), pivot, comp);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
PageID BTree<KeyT, ValueT>::InnerNode::lookup(const KeyT &pivot) {
	assert(upper);
	auto *slot = lower_bound(pivot);

	if (slot == slots_end()) {
		return upper;
	}

	assert(slot->child > 0);
	return slot->child;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
const KeyT BTree<KeyT, ValueT>::InnerNode::split(InnerNode &new_node,
												 size_t page_size) {
	++stats.inner_node_splits;
	// Sanity Check.
	assert(this->slot_count > 1);
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

	// Returning a reference to a deleted slot. Use with care.
	return pivot_slot.get_key(this->get_data());
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::InnerNode::insert_split(const KeyT &new_pivot,
												  PageID new_child) {
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
	}
	slot_target->child = new_child;

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
template <Indexable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::InnerNode::insert(const KeyT &new_pivot,
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

	return true;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
std::vector<PageID> BTree<KeyT, ValueT>::InnerNode::get_children() {
	std::vector<PageID> children{};
	for (uint16_t i = 0; i < this->slot_count; ++i) {
		auto &slot = *(slots_begin() + i);
		children.push_back(slot.child);
	}
	return children;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::InnerNode::print() {
	// Print Header.
	std::cout << "-----------------------------------------------------"
			  << std::endl;
	std::cout << "Header:" << std::endl;
	std::cout << "	data_start: " << this->data_start << std::endl;
	std::cout << "	level: " << this->level << std::endl;
	std::cout << "	slot_count: " << this->slot_count << std::endl;
	std::cout << "	upper: " << upper << std::endl;

	// Print Slots.
	std::cout << "Slots:" << std::endl;
	for (const auto *slot = slots_begin(); slot < slots_end(); slot++) {
		slot->print(this->get_data());
	}
	std::cout << "-----------------------------------------------------"
			  << std::endl;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::Pivot::Pivot(std::byte *page_begin,
											 uint32_t offset, const KeyT &key,
											 PageID child)
	: Node::Slot(offset, key.size()), child(child) {
	// Store key at offset.
	std::memcpy(page_begin + offset, key.serialize(), key.size());
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::InnerNode::Pivot::print(
	const std::byte *begin) const {
	std::cout << "	offset: " << this->offset;
	std::cout << ",	key_size: " << this->key_size;
	std::cout << ",	pivot: " << this->get_key(begin);
	std::cout << ",	child: " << child << std::endl;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
const KeyT BTree<KeyT, ValueT>::LeafNode::split(LeafNode &new_node,
												size_t page_size) {
	++stats.leaf_node_splits;
	assert(this->slot_count > 1);
	assert(page_size > 0);
	// Buffer the node.
	std::vector<std::byte> buffer{page_size};
	std::memcpy(&buffer[0], this, page_size);
	auto *buffer_node = reinterpret_cast<LeafNode *>(&buffer[0]);

	uint16_t pivot_i = (buffer_node->slot_count + 1) / 2 - 1;

	// First half of slots is reinserted into left leaf to compactiy space.
	// TODO: Make this better.
	this->slot_count = 0;
	this->data_start = page_size;
	const auto *slot_to_copy = buffer_node->slots_begin();
	while (slot_to_copy < buffer_node->slots_begin() + pivot_i + 1) {
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

	// Return new pivot to insert into parent node.
	assert(this->slot_count > 0);
	const auto &pivot_slot = *(slots_begin() + this->slot_count - 1);
	assert(this->slot_count == pivot_i + 1);

	return pivot_slot.get_key(this->get_data());
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::LeafNode::LeafSlot *
BTree<KeyT, ValueT>::LeafNode::lower_bound(const KeyT &key) {
	auto comp = [&](const LeafSlot &slot, const KeyT &key) -> bool {
		const auto &slot_key = slot.get_key(this->get_data());
		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), key, comp);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
std::optional<ValueT> BTree<KeyT, ValueT>::LeafNode::lookup(const KeyT &key) {
	auto *slot = lower_bound(key);

	if (slot == slots_end())
		return {};

	const auto found_key = slot->get_key(this->get_data());
	if (found_key != key)
		return {};

	return {slot->get_value(this->get_data())};
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::LeafNode::insert(const KeyT &key,
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
	this->data_start -= (key.size() + sizeof(ValueT));
	++this->slot_count;
	*slot_target = LeafSlot{this->get_data(), this->data_start, key, value};
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	return true;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::LeafNode::print() {
	// Print Header.
	std::cout << "-----------------------------------------------------"
			  << std::endl;
	std::cout << "Header:" << std::endl;
	std::cout << "	data_start: " << this->data_start << std::endl;
	std::cout << "	level: " << this->level << std::endl;
	std::cout << "	slot_count: " << this->slot_count << std::endl;

	// Print Slots.
	std::cout << "Slots:" << std::endl;
	for (const auto *slot = slots_begin(); slot < slots_end(); ++slot) {
		slot->print(this->get_data());
	}
	std::cout << "-----------------------------------------------------"
			  << std::endl;
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
BTree<KeyT, ValueT>::LeafNode::LeafSlot::LeafSlot(std::byte *page_begin,
												  uint32_t offset,
												  const KeyT &key,
												  const ValueT &value)
	: Node::Slot(offset, key.size()), value_size(sizeof(value)) {
	// Copy key and value into the slot's buffer.
	std::memcpy(page_begin + offset, key.serialize(), key.size());
	std::memcpy(page_begin + offset + key.size(), &value, sizeof(value));
};
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
const ValueT &BTree<KeyT, ValueT>::LeafNode::LeafSlot::get_value(
	const std::byte *begin) const {
	assert(value_size);
	return *reinterpret_cast<const ValueT *>(begin + this->offset +
											 this->key_size);
}
// -----------------------------------------------------------------
template <Indexable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::LeafNode::LeafSlot::print(
	const std::byte *begin) const {
	std::cout << "	offset: " << this->offset;
	std::cout << ",	key_size: " << this->key_size;
	std::cout << ",	value_size: " << value_size;

	std::cout << ",	key: " << this->get_key(begin);
	std::cout << ",	value: " << get_value(begin) << std::endl;
}
// -----------------------------------------------------------------
// Explicit instantiations
template struct BTree<UInt64, TID>;
template struct BTree<UInt64, UInt64>;
template struct BTree<String, UInt64>;
// -----------------------------------------------------------------
} // namespace bbbtree