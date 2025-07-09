#include "bbbtree/btree.h"

#include <algorithm>

namespace bbbtree {
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::BTree(SegmentID segment_id, BufferManager &buffer_manager)
	: Segment(segment_id, buffer_manager) {
	// TODO: only read from page at startup-time, then load it into memory and
	// write it out on destruction again.
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
	header.next_free_page = header.next_free_page;
	buffer_manager.unfix_page(frame, true);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
std::optional<ValueT> BTree<KeyT, ValueT>::lookup(const KeyT &key) {
	auto &leaf_frame = get_leaf(key);
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
BufferFrame &BTree<KeyT, ValueT>::get_leaf(const KeyT &key) {
	// TODO: Allow variable for exclusive/shared lock.

	auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
	auto &root_node = *reinterpret_cast<InnerNode *>(root_frame.get_data());

	if (root_node.is_leaf())
		return root_frame;

	// TODO:
	buffer_manager.unfix_page(root_frame, false);
	throw std::logic_error("BTree::get_leaf(): Not implemented yet.");

	// Find the first slot which is not smaller than the key.
	auto child = root_node.lower_bound(key);

	// CONTINUE HERE
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
void BTree<KeyT, ValueT>::insert(const KeyT &key, const ValueT &value) {
	// TODO: This frame is not locked exclusively.
	auto &leaf_frame = get_leaf(key);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// Node Split?
	if (leaf.get_free_space() <
		(sizeof(KeyT) + sizeof(typename LeafNode::Slot))) {
		buffer_manager.unfix_page(leaf_frame, false);
		throw std::logic_error(
			"BTree::insert(): Node split not implemented yet.");
	}

	auto success = leaf.insert(key, value);

	if (!success) {
		buffer_manager.unfix_page(leaf_frame, false);
		throw std::logic_error("BTree::insert(): Key already exists.");
	}

	buffer_manager.unfix_page(leaf_frame, false);

	// auto &leaf_page = get_leaf(key);
	// auto &leaf_node = LeafNode::page_to_leaf(leaf_page.get_data());
	// assert(!leaf_node.is_full() && "leaf_node must have enough space for new
	// entry"); leaf_node.insert(key, value);
	// buffer_manager.unfix_page(leaf_page, true);
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
BTree<KeyT, ValueT>::InnerNode::InnerNode(uint32_t page_size, uint16_t level,
										  PageID first_child, PageID upper)
	: Node(page_size, level, 1), upper(upper) {
	// Sanity Check: Node must fit page.
	assert(page_size > sizeof(InnerNode));

	// TODO: Insert child.
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

	if (this->slot_count == 0)
		return {};

	auto *slot_begin = slots_begin();
	auto *slot_end = slot_begin + this->slot_count;
	auto *slot = std::lower_bound(slot_begin, slot_end, key, comp);

	if (slot != slot_end) {
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

		// Get key from slots
		const auto &slot_key =
			*reinterpret_cast<KeyT *>(this->get_data() + slot.offset);

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

	const auto &found_key =
		*reinterpret_cast<KeyT *>(this->get_data() + slot->offset);
	if (found_key == key)
		return {slot->value};

	return {};
}
// -----------------------------------------------------------------
template <LessEqualComparable KeyT, typename ValueT>
bool BTree<KeyT, ValueT>::LeafNode::insert(const KeyT &key,
										   const ValueT &value) {
	// Sanity Check: User must ensure that there is enough space on leaf node
	// for insertion.
	assert(get_free_space() >= (sizeof(KeyT) + sizeof(LeafNode::Slot)));

	// Insert new slot.
	auto *insert_pos = lower_bound(key);
	if (insert_pos != slots_end()) {
		const auto &found_key =
			*reinterpret_cast<KeyT *>(this->get_data() + insert_pos->offset);
		if (found_key == key)
			return false;
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > insert_pos; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	this->data_start -= sizeof(KeyT);
	assert(sizeof(KeyT) <= std::numeric_limits<uint16_t>::max());
	*insert_pos =
		Slot{value, this->data_start, static_cast<uint16_t>(sizeof(KeyT))};
	++this->slot_count;
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	// Insert key.
	KeyT &target_data =
		*(reinterpret_cast<KeyT *>(this->get_data() + this->data_start));
	// TODO: `KeyT` must implement copy assignment.
	target_data = key;

	return true;
}
// -----------------------------------------------------------------
// Explicit instantiations
template class BTree<uint64_t, uint64_t>;
// -----------------------------------------------------------------
} // namespace bbbtree