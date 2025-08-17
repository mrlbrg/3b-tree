#pragma once

#include "bbbtree/btree.h"
#include "bbbtree/delta.h"
#include "bbbtree/types.h"

namespace bbbtree {

/// A B-Tree that can buffer its deltas.
template <KeyIndexable KeyT, ValueIndexable ValueT>
class BBBTree : BTree<KeyT, ValueT> {
  public:
	/// Applies the changes to a page.
	void load(PageID pid);
	/// Retrieves the changes on a page.
	void unload(PageID pid);

  private:
	/// Erases all changes on a page.
	void erase(PageID pid);

	/// The delta tree that stores changes to the BTree pages, identified
	/// through their `PID`.
	BTree<PID, Delta<KeyT, ValueT>> delta_tree;
};
} // namespace bbbtree