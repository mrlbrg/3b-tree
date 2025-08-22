#pragma once

#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/delta.h"
#include "bbbtree/types.h"

namespace bbbtree {
// -----------------------------------------------------------------
static const constexpr SegmentID DELTA_SEGMENT_ID = 123;
// -----------------------------------------------------------------
/// A delta tree is a BTree that maps from PIDs of the corresponding BTree nodes
/// to deltas on that node.
template <KeyIndexable KeyT, ValueIndexable ValueT>
class DeltaTree : public PageLogic, public BTree<PID, Deltas<KeyT, ValueT>> {
	using LeafDeltas = typename Deltas<KeyT, ValueT>::LeafDeltas;
	using InnerNodeDeltas = typename Deltas<KeyT, ValueT>::InnerNodeDeltas;

	using Node = BTree<KeyT, ValueT, true>::Node;
	using LeafNode = BTree<KeyT, ValueT, true>::LeafNode;
	using InnerNode = BTree<KeyT, ValueT, true>::InnerNode;

  public:
	/// Constructor.
	DeltaTree(SegmentID segment_id, BufferManager &buffer_manager)
		: PageLogic(),
		  BTree<PID, Deltas<KeyT, ValueT>>(segment_id, buffer_manager) {}

	/// Scans the given BTree node for dirty entries and buffers them in the
	/// delta tree.
	bool before_unload(BufferFrame &frame) override;
	/// Looks up the deltas for the given node and applies them.
	void after_load(const BufferFrame &frame) override;

  private:
	/// Cleans the slots of a node of their dirty state. Done to reset the state
	/// of a node when we want to actually write it out. The delta tracking
	/// should only be kept in memory.
	template <typename NodeT> void clean_node(NodeT *node);
	/// Extracts the deltas from the node and stores them in the delta tree.
	template <typename NodeT, typename DeltasT>
	DeltasT extract_deltas(const NodeT *node, DeltasT &&deltas);

	/// Calls the correct cleaning codefor the node type.
	void clean_node(Node *node);
	/// Calls the correct extraction code for the node type.
	void store_deltas(PID &&page_id, const Node *node);
};
// -----------------------------------------------------------------
/// A B-Tree that can buffer its deltas. Cannot just inherit from `BTree`
/// because the `DeltaTree` member must be initialized first but the constructor
/// must fulfill certain requirements to be a database's index.
template <KeyIndexable KeyT, ValueIndexable ValueT> class BBBTree {
  public:
	/// Constructor. The Delta Tree is stored in `segment_id` + 1.
	BBBTree(SegmentID segment_id, BufferManager &buffer_manager)
		: delta_tree(segment_id + 1, buffer_manager),
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
	inline void erase(const KeyT &key) { btree.erase(key); }

	/// Print tree and its delta tree. Not thread-safe.
	void print();

  private:
	/// The delta tree that stores changes to entries of the `btree` nodes,
	/// identified through their PID.
	DeltaTree<KeyT, ValueT> delta_tree;
	/// The actual index tree that stores the key-value pairs.
	BTree<KeyT, ValueT, true> btree;
};
// -----------------------------------------------------------------
} // namespace bbbtree