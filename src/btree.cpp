#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"

#include <algorithm>
#include <cstdint>
#include <iostream>

namespace bbbtree {
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::BTree(SegmentID segment_id, BufferManager &buffer_manager)
	: Segment(segment_id, buffer_manager) {
	// TODO: Create some meta-data segment that stores information like the
	// root's page id on file.
	auto &frame = buffer_manager.fix_page(segment_id, 0, false);
	auto &header = *(reinterpret_cast<Header *>(frame.get_data()));

	// Load meta-data into memory.
	root = header.root_page;
	next_free_page = header.next_free_page;

	// Start a new tree?
	if (next_free_page == 0) {
		root = 1;
		next_free_page = 2;
		// Intialize root node.
		auto &root_page = buffer_manager.fix_page(segment_id, root, true);
		*(new (root_page.get_data()) LeafNode(buffer_manager.get_page_size()));
		buffer_manager.unfix_page(root_page, true);
	}

	buffer_manager.unfix_page(frame, false);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::~BTree() {
	// Write out all meta-data needed to pick up index again later.
	// Caller must make sure that Buffer Manager is destroyed after this.
	auto &frame = buffer_manager.fix_page(segment_id, 0, true);
	auto &header = *(reinterpret_cast<Header *>(frame.get_data()));
	header.root_page = root;
	header.next_free_page = next_free_page;
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
void BTree<KeyT, ValueT>::erase(const KeyT &key) {
	throw std::logic_error("BTree::erase(): Not implemented yet.");
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BufferFrame &BTree<KeyT, ValueT>::get_leaf(const KeyT &key, bool exclusive) {
	// TODO: We only want the leaf to be locked exclusively. Must not lock all
	// exclusively on the path here. Start with inexclusively reading root.
	// Check if it's a leaf. If so, restart but take exclusive lock and return.
	auto &root_frame = buffer_manager.fix_page(segment_id, root, exclusive);
	auto &root_node = *reinterpret_cast<InnerNode *>(root_frame.get_data());

	if (root_node.is_leaf())
		return root_frame;

	// TODO: Root is not a leaf.
	buffer_manager.unfix_page(root_frame, false);
	throw std::logic_error("BTree::get_leaf(): Not implemented yet.");

	// Find the first slot which is not smaller than the key.
	auto child = root_node.lower_bound(key);

	// CONTINUE HERE
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
		split(key, value);
		goto restart;
	}

	auto success = leaf.insert(key, value);
	buffer_manager.unfix_page(leaf_frame, false);

	if (!success)
		throw std::logic_error("BTree::insert(): Key already exists.");
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
const KeyT &BTree<KeyT, ValueT>::LeafNode::split(const KeyT &key,
												 const ValueT &value) {

	// Split.
	assert(this->slot_count > 1);
	uint16_t pivot_nr = (this->slot_count + 1) / 2;
	const auto &pivot_slot = *(slots_begin() + pivot_nr);

	// CONTINUE HERE.
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::split(const KeyT &key, const ValueT &value) {
	throw std::logic_error("BTree::split(): Node split not implemented yet.");

	// No locks are held at this point.

	// Traverse anew to leaf but keep all locks on the path.
	auto *frame = &buffer_manager.fix_page(segment_id, root, true);
	auto *node = reinterpret_cast<InnerNode *>(frame->get_data());
	std::vector<BufferFrame *> locked_frames{};
	locked_frames.push_back(frame);

	while (!node->is_leaf()) {
		auto page_id = node->lower_bound(key);
		frame = &buffer_manager.fix_page(segment_id, page_id, true);
		locked_frames.push_back(frame);
		node = reinterpret_cast<InnerNode *>(frame->get_data());
	}
	auto &leaf = *reinterpret_cast<LeafNode *>(node);
	// Another thread might have already split. Release
	// everything and do  nothing.
	if (leaf.has_space(key, value)) {
		// TODO.
	}
	leaf.split(key, value);

	// Update root pointer.

	// Release all locks again.
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::InnerNode(uint32_t page_size, uint16_t level,
										  PageID first_child, PageID upper)
	: Node(page_size, level, 1), upper(upper) {
	// Sanity Check: Node must fit page.
	assert(page_size > sizeof(InnerNode));
	assert(sizeof(KeyT) <= std::numeric_limits<typeof(Slot::key_size)>::max());
	// TODO: Insert first child.
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
PageID BTree<KeyT, ValueT>::InnerNode::lower_bound(const KeyT &key) {
	// Sanity Check: An internal node must manage at least one pivot.
	assert(this->slot_count > 0);

	// Compare keys from two slots
	auto comp = [&](const Slot &slot, const KeyT &key) -> bool {
		assert(slot.offset > 0);
		assert(slot.key_size > 0);

		// Get key from slots
		const auto &slot_key =
			*reinterpret_cast<KeyT *>(this->get_data() + slot.offset);

		return slot_key < key;
	};

	auto *slot = std::lower_bound(slots_begin(), slots_end(), key, comp);

	if (slot != slots_end()) {
		assert(slot->child > 0);
		return slot->child;
	}

	// All keys are smaller than the searched key. Return `upper`.
	assert(upper > 0);
	return upper;
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
	// Sanity Checks
	assert(has_space(key, value));
	assert(sizeof(KeyT) <= std::numeric_limits<typeof(Slot::key_size)>::max());
	assert(sizeof(ValueT) <=
		   std::numeric_limits<typeof(Slot::value_size)>::max());

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
// Explicit instantiations
template class BTree<uint64_t, uint64_t>;
// -----------------------------------------------------------------
} // namespace bbbtree