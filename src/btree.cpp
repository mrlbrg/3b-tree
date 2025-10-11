#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/delta.h"
#include "bbbtree/logger.h"
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
	auto &frame = buffer_manager.fix_page(segment_id, 0, false, nullptr);
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
void BTree<KeyT, ValueT, UseDeltaTree>::clear() {
	// Reset meta-data.
	auto &frame = buffer_manager.fix_page(segment_id, 0, true, nullptr);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
		frame.get_data()));

	root = 1;
	next_free_page = 2;
	state.root = root;
	state.next_free_page = next_free_page;

	// Intialize root node.
	auto &root_page =
		buffer_manager.fix_page(segment_id, root, true, page_logic);
	new (root_page.get_data()) LeafNode(buffer_manager.page_size);
	buffer_manager.unfix_page(root_page, true);

	buffer_manager.unfix_page(frame, true);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::optional<ValueT>
BTree<KeyT, ValueT, UseDeltaTree>::lookup(const KeyT &key) {
	stats.num_lookups_index++;

	auto &leaf_frame = get_leaf(key, false);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// TODO: Not thread-safe.
	auto result = leaf.lookup(key);

	buffer_manager.unfix_page(leaf_frame, false);

	return result;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::erase(const KeyT &key,
											  size_t page_size) {
	assert(!UseDeltaTree && "Erase not supported with delta tree yet.");

	auto &leaf_frame = get_leaf(key, true);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());

	// TODO: Not thread-safe.
	leaf.erase(key, page_size);

	buffer_manager.unfix_page(leaf_frame, true);

	stats.num_deletions_index++;
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
		auto *child_frame = &buffer_manager.fix_page(segment_id, child_id,
													 exclusive, page_logic);

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

	stats.num_insertions_index++;

restart:
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
	if (!success) {
		buffer_manager.unfix_page(leaf_frame, false);
		return false;
	}

	buffer_manager.unfix_page(leaf_frame, true);
	assert(lookup(key).has_value());
	assert(lookup(key).value() == value);

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::update(const KeyT &key,
											   const ValueT &value) {
	auto &leaf_frame = get_leaf(key, true);
	auto &leaf = *reinterpret_cast<LeafNode *>(leaf_frame.get_data());
	leaf.update(key, value);
	buffer_manager.unfix_page(leaf_frame, true);
	stats.num_updates_index++;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
PageID BTree<KeyT, ValueT, UseDeltaTree>::get_new_page() {
	auto &frame = buffer_manager.fix_page(segment_id, 0, true, nullptr);
	auto &state = *(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
		frame.get_data()));

	auto page_id = next_free_page;
	++next_free_page;
	state.next_free_page = next_free_page;

	buffer_manager.unfix_page(frame, true);

	stats.pages_created++;

	return page_id;
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::split(const KeyT &key,
											  const ValueT &value) {
	using Pivot = std::pair<const KeyT, const PageID>;
	// logger.log("### Splitting node to insert key " + std::string(key));
	//  No frames are to be held at this point.
	while (true) {
		// logger.log("Before split:\n" + std::string(*this));
		auto *curr_frame =
			&buffer_manager.fix_page(segment_id, root, true, page_logic);
		auto *curr_node = reinterpret_cast<InnerNode *>(curr_frame->get_data());
		// All nodes that lie on the path to the key with the leaf in the
		// back and root in front.
		std::deque<BufferFrame *> path{curr_frame};
		// All nodes we touch are locked. Must be released in the end.
		std::vector<BufferFrame *> locked_nodes{curr_frame};

		// Collect all nodes on path to leaf for given key.
		while (!curr_node->is_leaf()) {
			curr_frame = &buffer_manager.fix_page(
				segment_id, curr_node->lookup(key), true, page_logic);
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
			for (auto *frame : locked_nodes)
				// Don't mark dirty here. Done while splitting.
				buffer_manager.unfix_page(*frame, false);
			break;
		}
		assert(leaf->slot_count > 0);

		// Split leaf.
		const auto new_pid = get_new_page();
		auto *new_leaf_frame =
			&buffer_manager.fix_page(segment_id, new_pid, true, page_logic);
		assert(new_leaf_frame->is_new());
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

				auto &frame =
					buffer_manager.fix_page(segment_id, 0, true, nullptr);
				auto &state =
					*(reinterpret_cast<BTree<KeyT, ValueT, UseDeltaTree> *>(
						frame.get_data()));
				root = new_root;
				state.root = new_root;
				buffer_manager.unfix_page(frame, true);

				auto *root_frame = &buffer_manager.fix_page(segment_id, root,
															true, page_logic);
				assert(root_frame->is_new());
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
			if (!curr_node->has_space(curr_key, curr_pid)) {
				// Create new node.
				const auto new_pid = get_new_page();
				auto *new_frame = &buffer_manager.fix_page(segment_id, new_pid,
														   true, page_logic);
				assert(new_frame->is_new());
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
			curr_node->insert_split(curr_key, curr_pid);
			curr_frame->set_dirty();
			insertion_queue.pop_back();
			assert(curr_level > 0);
			--curr_level;
		}

		for (auto *frame : locked_nodes)
			// Don't mark dirty here. Done while splitting.
			buffer_manager.unfix_page(*frame, false);
	}
	// logger.log("After split:\n" + std::string(*this));
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::ostream &operator<<(std::ostream &os,
						 const BTree<KeyT, ValueT, UseDeltaTree> &type) {

	auto size = type.size();
	// Acquire root to get height of tree.
	auto &root_frame = type.buffer_manager.fix_page(type.segment_id, type.root,
													false, type.page_logic);
	auto *node = reinterpret_cast<
		typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
		root_frame.get_data());
	auto level = node->level;
	type.buffer_manager.unfix_page(root_frame, false);

	os << std::endl;
	os << "size: " << size << std::endl;
	os << "root: " << type.root << std::endl;
	os << "next_free_page: " << type.next_free_page << std::endl;

	// Traverse and print each level of nodes.
	std::vector<PageID> nodes_on_current_level{type.root};

	while (level > 0) {
		os << std::endl;
		os << "################ LEVEL " << level << " ###############"
		   << std::endl;
		// Print current level & collect their children.
		std::vector<PageID> children{};
		for (auto pid : nodes_on_current_level) {
			// Acquire page.
			auto &frame = type.buffer_manager.fix_page(type.segment_id, pid,
													   false, type.page_logic);
			auto *node = reinterpret_cast<
				typename BTree<KeyT, ValueT, UseDeltaTree>::InnerNode *>(
				frame.get_data());

			// Sanity Check.
			assert(node->level == level);
			os << "[" << sizeof(*node) << "B] ";
			os << "PID " << pid;
			node->print(os);
			os << "-----------------------------------------------------"
			   << std::endl;

			// Collect children
			for (auto child : node->get_children()) {
				children.push_back(child);
			}
			children.push_back(node->get_upper());

			// Release page.
			type.buffer_manager.unfix_page(frame, false);
		}
		nodes_on_current_level = children;
		--level;
	}

	// Traverse leaf level
	os << "################ LEVEL " << level << " ###############" << std::endl;
	for (auto pid : nodes_on_current_level) {
		auto &frame = type.buffer_manager.fix_page(type.segment_id, pid, false,
												   type.page_logic);
		auto *leaf = reinterpret_cast<
			typename BTree<KeyT, ValueT, UseDeltaTree>::LeafNode *>(
			frame.get_data());
		// Sanity Check.
		assert(leaf->level == level);

		os << "[" << sizeof(*leaf) << "B] ";
		os << "PID " << pid;
		leaf->print(os);
		os << "-----------------------------------------------------"
		   << std::endl;

		type.buffer_manager.unfix_page(frame, false);
	}
	os << "########################################################"
	   << std::endl;

	return os;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
size_t BTree<KeyT, ValueT, UseDeltaTree>::size() const {
	// Acquire root to get height of tree.
	auto &root_frame =
		buffer_manager.fix_page(segment_id, root, false, page_logic);
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
			auto &frame =
				buffer_manager.fix_page(segment_id, pid, false, page_logic);
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
		auto &frame =
			buffer_manager.fix_page(segment_id, pid, false, page_logic);
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
	auto &frame = buffer_manager.fix_page(segment_id, root, false, page_logic);
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
	uint16_t pivot_i = (this->slot_count + 1) / 2 - 1;

	// Second half of slots is inserted into new, right leaf.
	const auto *slot_to_copy = this->slots_begin() + pivot_i + 1;
	while (slot_to_copy < this->slots_end()) {
		auto key = slot_to_copy->get_key(this->get_data());
		auto value = slot_to_copy->child;
		auto success = new_node.insert(key, value);
		assert(success);

		// Count bytes that have changed on this node.
		if constexpr (UseDeltaTree) {
			if (slot_to_copy->state == OperationType::Unchanged)
				this->num_bytes_changed += required_space(key, value);
		}

		++slot_to_copy;
	}
	// Cut off this node's slots at first half.
	this->slot_count = pivot_i + 1;

	// Set `upper` to right-most slot. Delete slot.
	assert(this->slot_count > 0);
	const auto *last_slot = slots_begin() + this->slot_count - 1;
	auto upper_key = last_slot->get_key(this->get_data());
	upper = last_slot->child;
	--this->slot_count;
	std::vector<std::byte> buffered_key{upper_key.size()};
	upper_key.serialize(buffered_key.data());

	// Compactify the node.
	compactify(page_size);
	assert(this->data_start <= page_size);

	// Copy upper key into the data section to return a reference to it.
	auto dst = this->get_data() + this->data_start - upper_key.size();
	assert(dst >= reinterpret_cast<std::byte *>(this->slots_end()));
	std::memcpy(dst, buffered_key.data(), buffered_key.size());

	// Returning a reference to a section that can be modified. Use with
	// care.
	return KeyT::deserialize(dst, buffered_key.size());
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::insert_split(
	const KeyT &new_pivot, PageID new_child) {
	// Sanity checks.
	assert(has_space(new_pivot, new_child));
	assert(upper);

	// Find the slot of the child that was split.
	auto *slot_target = lower_bound(new_pivot);
	PageID old_child;
	if (slot_target == slots_end()) {
		// Upper child was split.
		old_child = upper;
		upper = new_child;

		// TODO: Also track changed bytes here? We don't know if upper has
		// already changed. `upper` will always be tracked in the delta.
	} else {
		assert(slot_target->get_key(this->get_data()) != new_pivot);
		old_child = slot_target->child;
		slot_target->child = new_child;

		if constexpr (UseDeltaTree) {
			// Track the amount of change on the node.
			if (slot_target->state == OperationType::Unchanged)
				this->num_bytes_changed += required_space(
					slot_target->get_key(this->get_data()), new_child);
			// Indicate that the child has changed from the disk state.
			// Unless it was newly inserted since loaded from disk.
			if (slot_target->state != OperationType::Inserted)
				slot_target->state = OperationType::Updated;
		}
	}

	// Move each slot up by one to make space for new one.
	for (auto *slot = slots_end(); slot > slot_target; --slot) {
		auto &source_slot = *(slot - 1);
		auto &target_slot = *(slot);
		target_slot = source_slot;
	}
	// Insert slot with <new_pivot, old_upper>
	this->data_start -= new_pivot.size();
	++this->slot_count;
	*slot_target =
		Pivot{this->get_data(), this->data_start, new_pivot, old_child};
	assert(reinterpret_cast<std::byte *>(slots_end()) <=
		   this->get_data() + this->data_start);

	if constexpr (UseDeltaTree) {
		this->num_bytes_changed += required_space(new_pivot, old_child);
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::update(const KeyT &key,
														  PageID new_child) {
	auto has_child = [&](const Pivot *slot_end, PageID child) -> bool {
		for (auto *slot = slots_begin(); slot < slot_end; ++slot) {
			if (slot->child == child)
				return true;
		}
		return false;
	};

	auto *slot = lower_bound(key);
	assert(slot != slots_end());
	assert(slot->get_key(this->get_data()) == key);
	auto old_child = slot->child;
	slot->child = new_child;
	// Sanity Check: When updating a slot to a new child, we must make
	// sure we don't loose the old child, but that a previous slot has
	// been inserted with the old child.
	assert(has_child(slot, old_child));
	if constexpr (UseDeltaTree) {
		assert(slot->state == OperationType::Unchanged);
		slot->state = OperationType::Updated;
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::insert(
	const KeyT &new_pivot, PageID new_child, bool allow_duplicates) {
	// Sanity checks.
	assert(has_space(new_pivot, new_child));
	assert(upper);

	// Find target position for new pivotal slot.
	auto *slot_target = lower_bound(new_pivot);
	if (!allow_duplicates && slot_target != slots_end()) {
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
		slot_target->state = OperationType::Inserted;
		this->num_bytes_changed += required_space(new_pivot, new_child);
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
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::print(std::ostream &os) {
	// Print Header.
	os << "	data_start: " << this->data_start;
	os << ", level: " << this->level;
	os << ", slot_count: " << this->slot_count;
	if constexpr (UseDeltaTree)
		os << ", num_bytes_changed: " << this->num_bytes_changed;

	os << std::endl;

	// Print Slots.
	for (const auto *slot = slots_begin(); slot < slots_end(); slot++) {
		slot->print(os, this->get_data());
	}
	os << "  upper: " << upper << std::endl;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::Pivot::Pivot(
	std::byte *page_begin, uint32_t offset, const KeyT &key, PageID child)
	: Node::Slot(offset, key.size()), child(child) {
	// Store key at offset. Caller must ensure that it has enough space for
	// `key.size()`.
	key.serialize(page_begin + offset);
	if constexpr (UseDeltaTree) {
		this->state = OperationType::Inserted;
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::Pivot::print(
	std::ostream &os, const std::byte *begin) const {
	os << "[" << sizeof(*this) << "B + " << this->key_size << "B + "
	   << sizeof(this->child) << "B] ";
	os << "  offset: " << this->offset;
	os << ", key_size: " << this->key_size;
	os << ", pivot: " << this->get_key(begin);
	os << ", child: " << child << std::endl;

	if constexpr (UseDeltaTree) {
		os << "    state: " << this->state << std::endl;
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
uint16_t
BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::compactify(uint32_t page_size) {
	// Collect all slot pointers.
	std::vector<Pivot *> slots;
	for (auto *slot = slots_begin(); slot < slots_end(); ++slot)
		slots.push_back(slot);

	// Sort them by their offset with the biggest offset first.
	std::sort(slots.begin(), slots.end(), [](const Pivot *a, const Pivot *b) {
		return a->offset > b->offset;
	});

	// Move all keys up.
	auto target_offset = page_size;
	for (auto *slot : slots) {
		target_offset -= slot->key_size;
		auto *key_start = this->get_data() + slot->offset;
		auto *target_start = this->get_data() + target_offset;
		std::memmove(target_start, key_start, slot->key_size);
		slot->offset = target_offset;
	}

	// Update data_start.
	assert(target_offset >=
		   this->data_start); // Can also have no space savings.
	uint16_t saved_space = target_offset - this->data_start;
	this->data_start = target_offset;

	return saved_space;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::InnerNode::shrink(
	uint32_t current_page_size, uint32_t target_page_size) {
	// Get the distance we need to move the data segment up by.
	assert(current_page_size > target_page_size);
	auto size_reduction = current_page_size - target_page_size;

	// Move full data segment up.
	assert(this->data_start <= current_page_size);
	auto data_size = current_page_size - this->data_start;
	auto *current_data_start = this->get_data() + this->data_start;
	auto *target_data_start = current_data_start - size_reduction;
	assert(target_data_start < current_data_start);
	assert(target_data_start >=
		   reinterpret_cast<std::byte *>(
			   this->slots_end())); // Do not overwrite slot segment.
	std::memmove(target_data_start, current_data_start, data_size);

	// Update all slots.
	for (auto slot = slots_begin(); slot < slots_end(); ++slot) {
		assert(slot->offset > size_reduction);
		slot->offset -= size_reduction;
		assert(this->get_data() + slot->offset >=
			   reinterpret_cast<std::byte *>(this->slots_end()));
	}

	// Update data_start.
	this->data_start -= size_reduction;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
const KeyT BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::split(
	LeafNode &new_node, const KeyT &key, size_t page_size) {

	++stats.leaf_node_splits;
	assert(this->slot_count >= 1);
	assert(page_size > 0);

	// Determine how many keys go left/right.
	// If the new key goes in the left node, move more entries to the new
	// right node. Important when having a tree where each leaf holds only
	// one entry.
	const auto &middle_slot =
		*(this->slots_begin() + ((this->slot_count + 1) / 2) - 1);
	const auto &middle_key = middle_slot.get_key(this->get_data());
	bool skew_left = (middle_key < key); // Will key go right?
	uint16_t num_slots_left =
		skew_left ? (this->slot_count + 1) / 2 : (this->slot_count / 2);

	// Second half of slots is inserted into new, right leaf.
	for (const auto *slot_to_copy = this->slots_begin() + num_slots_left;
		 slot_to_copy < this->slots_end(); ++slot_to_copy) {
		auto key = slot_to_copy->get_key(this->get_data());
		auto value = slot_to_copy->get_value(this->get_data());
		auto success = new_node.insert(key, value);
		assert(success);

		// Track delta.
		if constexpr (UseDeltaTree) {
			if (slot_to_copy->state == OperationType::Unchanged)
				this->num_bytes_changed += required_space(key, value);
		}
	}
	assert(new_node.slot_count == this->slot_count - num_slots_left);

	// Cut off right half from this node and compactify space.
	this->slot_count = num_slots_left;
	compactify(page_size);

	// If left node is empty, the key to be inserted will be the new
	// pivotal key in the parent.
	if (this->slot_count == 0)
		return key;

	// Return last slot of left node as new pivot to insert into parent.
	const auto &last_slot = *(slots_begin() + this->slot_count - 1);
	return last_slot.get_key(this->get_data());
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
bool BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::insert(
	const KeyT &key, const ValueT &value, bool allow_duplicates) {
	assert(has_space(key, value));
	assert(!allow_duplicates); // Never allow duplicates for leafs.
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
		slot_target->state = OperationType::Inserted;
		this->num_bytes_changed += required_space(key, value);
	}

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::update(const KeyT &key,
														 const ValueT &value) {
	auto *slot = lower_bound(key);
	if (slot == slots_end())
		throw std::runtime_error("LeafNode::update: Key not found");

	auto &found_key = slot->get_key(this->get_data());
	if (found_key != key)
		throw std::runtime_error("LeafNode::update: Key not found");

	// Overwrite value in place if it has the same size.
	if (value.size() != slot->value_size)
		throw std::runtime_error("LeafNode::update: Updating to a value of "
								 "different size is not supported");

	value.serialize(this->get_data() + slot->offset + slot->key_size);

	// Track delta.
	if constexpr (UseDeltaTree) {
		if (slot->state != OperationType::Inserted) {
			slot->state = OperationType::Updated;
			this->num_bytes_changed += value.size();
		}
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
bool BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::erase(const KeyT &key,
														size_t page_size) {
	auto *slot = lower_bound(key);

	// Key not found.
	if (slot == slots_end())
		return false;
	if (slot->get_key(this->get_data()) != key)
		return false;

	// Track delta.
	if constexpr (UseDeltaTree) {
		// Only count changed bytes if they have not already been changed by
		// a previous operation.
		if (slot->state == OperationType::Unchanged)
			this->num_bytes_changed +=
				required_space(key, slot->get_value(this->get_data()));
	}

	// Remove slot by shifting all following slots down.
	for (auto *s = slot; s < slots_end() - 1; ++s) {
		*s = std::move(*(s + 1));
	}
	--this->slot_count;

	// TODO: Only temporarily compactify here. We want to track free
	// space and only compactify when we need the space.
	compactify(page_size);

	return true;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::print(std::ostream &os) {
	// Print Header.
	os << ", data_start: " << this->data_start;
	os << ", level: " << this->level;
	os << ", slot_count: " << this->slot_count;

	if constexpr (UseDeltaTree)
		os << ", num_bytes_changed: " << this->num_bytes_changed;

	os << ":" << std::endl;

	// Print Slots.
	for (const auto *slot = slots_begin(); slot < slots_end(); ++slot) {
		slot->print(os, this->get_data());
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
uint16_t
BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::compactify(uint32_t page_size) {
	// Collect all slot pointers.
	std::vector<LeafSlot *> slots;
	for (auto *slot = slots_begin(); slot < slots_end(); ++slot) {
		slots.push_back(slot);
	}

	// Sort them by their offset with the biggest offset first.
	std::sort(slots.begin(), slots.end(),
			  [](const LeafSlot *a, const LeafSlot *b) {
				  return a->offset > b->offset;
			  });

	// Move all keys up.
	auto target_offset = page_size;
	for (auto *slot : slots) {
		target_offset -= (slot->key_size + slot->value_size);
		auto *key_start = this->get_data() + slot->offset;
		auto *target_start = this->get_data() + target_offset;
		std::memmove(target_start, key_start,
					 slot->key_size + slot->value_size);
		slot->offset = target_offset;
	}

	// Update data_start.
	assert(target_offset >= this->data_start);
	uint16_t saved_space = target_offset - this->data_start;
	this->data_start = target_offset;
	return saved_space;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
void BTree<KeyT, ValueT, UseDeltaTree>::LeafNode::shrink(
	uint32_t current_page_size, uint32_t target_page_size) {
	// Get the distance we need to move the data segment up by.
	assert(current_page_size > target_page_size);
	auto size_reduction = current_page_size - target_page_size;

	// Move full data segment up.
	assert(this->data_start <= current_page_size);
	auto data_size = current_page_size - this->data_start;
	auto *current_data_start = this->get_data() + this->data_start;
	auto *target_data_start = current_data_start - size_reduction;
	assert(target_data_start < current_data_start);
	assert(target_data_start >=
		   reinterpret_cast<std::byte *>(
			   this->slots_end())); // Do not overwrite slot segment.
	std::memmove(target_data_start, current_data_start, data_size);

	// Update all slots.
	for (auto slot = slots_begin(); slot < slots_end(); ++slot) {
		assert(slot->offset > size_reduction);
		slot->offset -= size_reduction;
	}

	// Update data_start.
	this->data_start -= size_reduction;
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
	std::ostream &os, const std::byte *begin) const {
	os << "[" << sizeof(*this) << "B + " << this->key_size << "B + "
	   << this->value_size << "B] ";
	os << "  offset: " << this->offset;
	os << ", key_size: " << this->key_size;
	os << ", value_size: " << value_size;

	os << ", key: " << this->get_key(begin);
	os << ", value: " << get_value(begin) << std::endl;

	if constexpr (UseDeltaTree) {
		os << "    state: " << this->state << std::endl;
	}
}
// -----------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const OperationType &type) {
	switch (type) {
	case OperationType::Unchanged:
		os << "Unchanged";
		break;
	case OperationType::Inserted:
		os << "Inserted";
		break;
	case OperationType::Deleted:
		os << "Deleted";
		break;
	case OperationType::Updated:
		os << "Updated";
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
template std::ostream &operator<<(std::ostream &, const BTree<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<UInt64, UInt64> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<UInt64, String> &);
template std::ostream &operator<<(std::ostream &, const BTree<String, TID> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<String, UInt64> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<String, String> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<UInt64, TID, true> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<String, TID, true> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<PID, Deltas<UInt64, TID>> &);
template std::ostream &operator<<(std::ostream &,
								  const BTree<PID, Deltas<String, TID>> &);
// -----------------------------------------------------------------
} // namespace bbbtree