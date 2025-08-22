#include "bbbtree/bbbtree.h"
#include "bbbtree/types.h"
#include <gtest/gtest.h>

using namespace bbbtree;

namespace {
// -----------------------------------------------------------------
using BBBTreeInt = BBBTree<UInt64, TID>;
static const constexpr SegmentID TEST_SEGMENT_ID = 834;
// -----------------------------------------------------------------

class BBBTreeTest : public ::testing::Test {
  protected:
	static const constexpr size_t TEST_PAGE_SIZE = 128;
	static const constexpr size_t TEST_NUM_PAGES = 4;

	void Reset(bool clear_files) {
		// BTrees use the buffer manager to persist their state at
		// destruction-time. Therefore we must delete the BTree first.
		buffer_manager_.reset();
		bbbtree_int_.reset();
		// Create anew.
		buffer_manager_ = std::make_unique<BufferManager>(
			TEST_PAGE_SIZE, TEST_NUM_PAGES, clear_files);
		bbbtree_int_ =
			std::make_unique<BBBTreeInt>(TEST_SEGMENT_ID, *buffer_manager_);
	}

	std::unique_ptr<BufferManager> buffer_manager_ =
		std::make_unique<BufferManager>(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);
	std::unique_ptr<BBBTreeInt> bbbtree_int_ =
		std::make_unique<BBBTreeInt>(TEST_SEGMENT_ID, *buffer_manager_);
};
// -----------------------------------------------------------------
/// When a node of the BBBTree is evicted, its deltas are stored in the delta
/// tree and not persisted on disk.
/// TODO: Later only when write amplification was high.
TEST_F(BBBTreeTest, EvictNode) {
	// Create a BBBTree and insert some key-value pairs to fill a single node.
	// Loads the root page into the buffer. Should callback into eviction
	// handler `after_load`.
	EXPECT_TRUE(bbbtree_int_->insert(1, 2));
	bbbtree_int_->print();

	// Force full BTree and DeltaTree to disk so no node is in new-state
	// anymore.
	Reset(false);

	EXPECT_TRUE(bbbtree_int_->insert(2, 2));
	bbbtree_int_->print();

	// Force an eviction of the BTree. Should call back into Delta Tree to store
	// its nodes.
	Reset(false);
	// buffer_manager_->clear_all();
	bbbtree_int_->print();

	Reset(false);

	// Verify that the new entry added since the last read from disk is not on
	// the node anymore. The node has been cleared of changes since. The changes
	// are stored in the delta tree instead.
	EXPECT_FALSE(bbbtree_int_->lookup(2).has_value());

	// Expect

	// Force an eviction of the node.

	// Verify that the deltas are stored in the delta tree.

	// Verify that the node is not persisted on disk.

	// Load the node into the buffer again.

	// Verify that the deltas are applied correctly.

	// The page should be equivalent to before eviction.
}
///
// TEST_F(BBBTreeTest, LoadNode) {}
// -----------------------------------------------------------------
} // namespace