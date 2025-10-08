#pragma once

#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/delta.h"
#include "bbbtree/types.h"

#include <cstdint>
#include <sstream>

namespace bbbtree {

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
class BBBTree;
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree>
std::ostream &operator<<(std::ostream &os,
						 const BBBTree<KeyT, ValueT, UseDeltaTree> &type);
// -----------------------------------------------------------------
/// A delta tree is a BTree that maps from PIDs of the corresponding BTree
/// nodes to deltas on that node.
template <KeyIndexable KeyT, ValueIndexable ValueT>
class DeltaTree : public PageLogic, public BTree<PID, Deltas<KeyT, ValueT>> {
	using LeafDeltas = typename Deltas<KeyT, ValueT>::LeafDeltas;
	using InnerNodeDeltas = typename Deltas<KeyT, ValueT>::InnerNodeDeltas;

	using Node = BTree<KeyT, ValueT, true>::Node;
	using LeafNode = BTree<KeyT, ValueT, true>::LeafNode;
	using InnerNode = BTree<KeyT, ValueT, true>::InnerNode;

  public:
	/// Constructor.
	DeltaTree(SegmentID segment_id, BufferManager &buffer_manager,
			  uint16_t wa_threshold)
		: PageLogic(),
		  BTree<PID, Deltas<KeyT, ValueT>>(segment_id, buffer_manager, nullptr),
		  wa_threshold(wa_threshold) {}

	/// Scans the given BTree node for dirty entries and buffers them in the
	/// delta tree.
	/// `first` returns true if unloading was successful.
	// `second` returns true if the page should be written out to disk.
	std::pair<bool, bool> before_unload(char *data, const State &state,
										PageID page_id,
										size_t page_size) override;
	/// Looks up the deltas for the given node and applies them.
	void after_load(char *data, PageID page_id) override;

  private:
	/// Cleans the slots of a node of their dirty state. Done to reset the state
	/// of a node when we want to actually write it out. The delta tracking
	/// should only be kept in memory.
	template <typename NodeT> void clean_node(NodeT *node);
	/// Extracts the deltas from the node.
	template <typename NodeT, typename DeltasT>
	void extract_deltas(const NodeT *node, DeltasT &deltas);
	/// Applies the deltas to the node. TODO: Also remove from the tree?
	template <typename NodeT, typename DeltasT>
	void apply_deltas(NodeT *node, const DeltasT &deltas, uint16_t slot_count);

	/// Calls the correct cleaning codefor the node type.
	void clean_node(Node *node);
	/// Calls the correct extraction code for the node type.
	void store_deltas(PageID page_id, const Node *node);

	/// Locking mechanism to prevent re-entrant calls to (un)load functions.
	/// When a page is unloaded/loaded (`before_unload`), we must prevent a
	/// subcall to unload as well to make space for the delta tree nodes.
	/// Therefore we lock the delta tree during load and unload.
	/// When calling `before_unload`, while the tree is locked, we return false
	/// to indicate that this node cannot be evicted at this time.
	/// During `after_load`, the tree should never be locked.
	bool is_locked = false;
	/// The write amplification threshold. When the ratio of bytes changed
	/// in a node is below this threshold, we buffer the changes in this tree.
	const uint16_t wa_threshold;
};
// -----------------------------------------------------------------
/// A B-Tree that can buffer its deltas. Cannot just inherit from `BTree`
/// because the `DeltaTree` member must be initialized first but the constructor
/// must fulfill certain requirements to be a database's index.
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree = false>
class BBBTree {
  public:
	/// Constructor. The Delta Tree is stored in `segment_id` + 1.
	BBBTree(SegmentID segment_id, BufferManager &buffer_manager,
			uint16_t wa_threshold)
		: delta_tree(segment_id + 1, buffer_manager, wa_threshold),
		  btree(segment_id, buffer_manager, &delta_tree) {}

	/// Lookup an entry in the tree. Returns `nullopt` if key was not found.
	inline std::optional<ValueT> lookup(const KeyT &key) {
		return btree.lookup(key);
	}
	/// Inserts a new entry into the tree. Returns false if key already exists.
	[[nodiscard]] inline bool insert(const KeyT &key, const ValueT &value) {
		return btree.insert(key, value);
	}
	/// Erase an entry in the tree.
	inline void erase(const KeyT &key, size_t page_size) {
		btree.erase(key, page_size);
	}

	/// Returns the number of key/value pairs stored in the B-tree.
	inline size_t size() { return btree.size(); }

	/// Returns the number of levels in the B-tree.
	inline size_t height() { return btree.height(); }

	/// Sets the height in the stats.
	void set_height() {
		stats.b_tree_height = height();
		stats.delta_tree_height = delta_tree.height();
	}

	/// Clears the trees.
	void clear() {
		btree.clear();
		delta_tree.clear();
	}

	/// Prints the tree.
	friend std::ostream &
	operator<< <>(std::ostream &os,
				  const BBBTree<KeyT, ValueT, UseDeltaTree> &tree);
	/// Converts the tree to a string.
	operator std::string() const {
		std::stringstream ss;
		ss << *this;
		return ss.str();
	}

  protected:
	/// The delta tree that stores changes to entries of the `btree` nodes,
	/// identified through their PID.
	DeltaTree<KeyT, ValueT> delta_tree;
	/// The actual index tree that stores the key-value pairs.
	BTree<KeyT, ValueT, true> btree;

	static_assert(!UseDeltaTree);
};
// -----------------------------------------------------------------
} // namespace bbbtree