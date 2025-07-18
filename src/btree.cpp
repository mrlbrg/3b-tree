#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/types.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <vector>

namespace bbbtree {
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
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
		new (root_page.get_data()) LeafNode(buffer_manager.get_page_size());
		buffer_manager.unfix_page(root_page, true);
	}

	buffer_manager.unfix_page(frame, false);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::~BTree() {
	// Write out all meta-data needed to pick up index again later.
	// Caller must make sure that Buffer Manager is destroyed after this, not
	// before.
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT> *>(frame.get_data()));
	state.root = root;
	state.next_free_page = next_free_page;
	buffer_manager.unfix_page(frame, true);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
std::optional<ValueT> BTree<KeyT, ValueT>::lookup(const KeyT &key) {
	auto &leaf_frame = get_leaf(key, false);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// TODO: Not thread-safe.
	auto result = leaf.lookup(key);

	buffer_manager.unfix_page(leaf_frame, false);

	return result;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::erase(const KeyT & /*key*/) {
	throw std::logic_error("BTree::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
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
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::insert(const KeyT &key, const ValueT &value) {
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
		throw std::logic_error("BTree::insert(): Key already exists.");

	assert(lookup(key).has_value());
	assert(lookup(key).value() == value);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
PageID BTree<KeyT, ValueT>::get_new_page() {
	// TODO: Synchronize this when multi-threading.
	auto page_id = next_free_page;
	++next_free_page;

	return page_id;
}

// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::split(const KeyT &key) {
	// No frames should be held at this point.

	// Traverse anew to leaf but keep all locks on the path.
	auto page_id = root;
	auto *frame = &buffer_manager.fix_page(segment_id, page_id, true);
	// TODO: When multithreading, poll if this is still root at this point,
	// when having acquired the lock.
	auto *node = reinterpret_cast<InnerNode *>(frame->get_data());
	std::vector<BufferFrame *> locked_nodes{frame};

	while (!node->is_leaf()) {
		page_id = node->lookup(key);
		frame = &buffer_manager.fix_page(segment_id, page_id, true);
		locked_nodes.push_back(frame);
		node = reinterpret_cast<InnerNode *>(frame->get_data());
	}
	// TODO: When multithreading, another thread might have already split.
	// Release everything and do nothing in that case.

	// Split Leaf.
	auto &leaf = *reinterpret_cast<LeafNode *>(node);
	const auto new_page_id = get_new_page();
	auto &new_leaf_frame =
		buffer_manager.fix_page(segment_id, new_page_id, true);
	auto &new_leaf = *(new (new_leaf_frame.get_data())
						   LeafNode(buffer_manager.get_page_size()));
	const auto *pivot = &leaf.split(new_leaf, buffer_manager.get_page_size());

	buffer_manager.unfix_page(new_leaf_frame, true);
	assert(!locked_nodes.empty());

// A new pivot must be inserted into the parent.
insert_new_pivot:
	auto &child_frame = *locked_nodes.back();
	locked_nodes.pop_back();

	// Arrived at root? Create new root.
	if (locked_nodes.empty()) {
		root = get_new_page();
		auto &root_frame = buffer_manager.fix_page(segment_id, root, true);
		auto &child_node =
			*reinterpret_cast<InnerNode *>(child_frame.get_data());
		new (root_frame.get_data())
			InnerNode(buffer_manager.get_page_size(), child_node.level + 1);
		locked_nodes.push_back(&root_frame);
	}
	// Release child. Must wait with release until here since this frame might
	// have been the root. Cannot release the root until we have a new one.
	buffer_manager.unfix_page(child_frame, true);

	auto &parent_frame = *locked_nodes.back();
	auto &parent_node = *reinterpret_cast<InnerNode *>(parent_frame.get_data());

	// Split new parent if necessary.
	assert(pivot);
	if (parent_node.has_space(*pivot)) {
		// Child pointer must be updated before inserting the new pivot.
		parent_node.update(key, new_page_id);
		auto success = parent_node.insert(*pivot, page_id);
		assert(success); // TODO: Just throw.
	} else {
		// TODO: Make space for the pivot, then insert, then goto
		// insert_new_pivot
		throw std::logic_error(
			"BTree::split(): Cascading node split not implemented yet.");
		goto insert_new_pivot;
	}

	// Release dirty frames.
	assert(!locked_nodes.empty());
	locked_nodes.pop_back();
	buffer_manager.unfix_page(parent_frame, true);

	// Release rest of path of clean frames.
	for (auto *frame : locked_nodes) {
		buffer_manager.unfix_page(*frame, false);
	}
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::print() {
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
template <LessEqualComparable KeyT, typename ValueT>
size_t BTree<KeyT, ValueT>::size() {
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
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::InnerNode(uint32_t page_size, uint16_t level)
	: Node(page_size, level) {
	// Sanity Check: Node must fit page.
	assert(page_size > sizeof(InnerNode));
	static_assert(sizeof(KeyT) <=
				  std::numeric_limits<typeof(Slot::key_size)>::max());
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::Slot *
BTree<KeyT, ValueT>::InnerNode::lower_bound(const KeyT &pivot) {
	// Compare keys from two slots
	auto comp = [&](const Slot &slot, const KeyT &key) -> bool {
		assert(slot.offset > 0);
		assert(slot.key_size > 0);

		// Get key from slots
		const auto &slot_key =
			*reinterpret_cast<KeyT *>(this->get_data() + slot.offset);

		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), pivot, comp);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
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
template <LessEqualComparable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::InnerNode::insert(const KeyT &pivot,
											const PageID child) {
	assert(has_space(pivot));
	assert(upper);

	auto *slot_target = lower_bound(pivot);
	if (slot_target != slots_end()) {
		const auto &found_pivot = slot_target->get_key(this->get_data());
		// Keys must be unique. We don't throw here because we don't manage
		// the lock.
		if (found_pivot == pivot)
			return false;
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > slot_target; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	// Insert new slot.
	this->data_start -= (sizeof(KeyT) + sizeof(ValueT));
	*slot_target =
		Slot{child, this->data_start, static_cast<uint16_t>(sizeof(KeyT))};
	++this->slot_count;
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	// Insert pivot.
	KeyT &pivot_target = slot_target->get_key(this->get_data());
	// TODO: `KeyT` must implement copy assignment.
	pivot_target = pivot;

	return true;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::InnerNode::update(const KeyT &pivot,
											const PageID child) {
	auto *slot = lower_bound(pivot);

	if (slot == slots_end()) {
		upper = child;
		return;
	}

	slot->child = child;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
std::vector<PageID> BTree<KeyT, ValueT>::InnerNode::get_children() {
	std::vector<PageID> children{};
	for (uint16_t i = 0; i < this->slot_count; ++i) {
		auto &slot = *(slots_begin() + i);
		children.push_back(slot.child);
	}
	return children;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
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
		std::cout << "	offset: " << slot->offset;
		std::cout << ",	key_size: " << slot->key_size;
		std::cout << ",	pivot: " << slot->get_key(this->get_data());
		std::cout << ",	child: " << slot->child << std::endl;
	}
	std::cout << "-----------------------------------------------------"
			  << std::endl;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
const KeyT &BTree<KeyT, ValueT>::LeafNode::split(LeafNode &new_node,
												 size_t page_size) {

	// Split.
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
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::LeafNode::Slot *
BTree<KeyT, ValueT>::LeafNode::lower_bound(const KeyT &key) {
	auto comp = [&](const Slot &slot, const KeyT &key) -> bool {
		assert(slot.offset > 0);
		assert(slot.key_size > 0);

		const auto &slot_key = slot.get_key(this->get_data());
		return slot_key < key;
	};

	return std::lower_bound(slots_begin(), slots_end(), key, comp);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
std::optional<ValueT> BTree<KeyT, ValueT>::LeafNode::lookup(const KeyT &key) {
	auto *slot = lower_bound(key);

	if (slot == slots_end())
		return {};

	const auto &found_key = slot->get_key(this->get_data());
	if (found_key != key)
		return {};

	assert(slot->value_size);
	return {slot->get_value(this->get_data())};
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::LeafNode::insert(const KeyT &key,
										   const ValueT &value) {
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
	this->data_start -= (sizeof(KeyT) + sizeof(ValueT));
	*slot_target = Slot{this->data_start, static_cast<uint16_t>(sizeof(KeyT)),
						static_cast<uint16_t>(sizeof(ValueT))};
	++this->slot_count;
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	// Insert key.
	KeyT &key_target = slot_target->get_key(this->get_data());
	// TODO: `KeyT` must implement copy assignment.
	key_target = key;
	// Insert value.
	ValueT &value_target = slot_target->get_value(this->get_data());
	value_target = value;

	return true;
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
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
		std::cout << "	offset: " << slot->offset;
		std::cout << ",	key_size: " << slot->key_size;
		std::cout << ",	value_size: " << slot->value_size;

		std::cout << ",	key: " << slot->get_key(this->get_data());
		std::cout << ",	value: " << slot->get_value(this->get_data())
				  << std::endl;
	}
	std::cout << "-----------------------------------------------------"
			  << std::endl;
}
// -----------------------------------------------------------------
// Explicit instantiations
template struct BTree<uint64_t, uint64_t>;
// -----------------------------------------------------------------
} // namespace bbbtree