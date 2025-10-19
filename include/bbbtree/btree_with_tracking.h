#pragma once
// -----------------------------------------------------------------
#include "bbbtree/btree.h"
// -----------------------------------------------------------------
namespace bbbtree {
class EmptyPageLogic : public PageLogic {
	bool before_unload(char * /*data*/, const State & /*state*/,
					   PageID /*page_id*/, size_t /*page_size*/) override {
		return true;
	}
	/// The function to call after the page was loaded from disk.
	void after_load(char * /*data*/, PageID /*page_id*/) override {}
} empty_page_logic;
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
struct BTreeWithTracking : public BTree<KeyT, ValueT, true> {
	BTreeWithTracking(SegmentID segment_id, BufferManager &buffer_manager,
					  float /*wa_threshold*/)
		: BTree<KeyT, ValueT, true>(segment_id, buffer_manager,
									&empty_page_logic) {}
};
// -----------------------------------------------------------------
} // namespace bbbtree