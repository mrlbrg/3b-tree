#include "bbbtree/btree.h"

#include <gtest/gtest.h>
#include <optional>

using namespace bbbtree;

namespace {
static const constexpr size_t BTREE_SEGMENT_ID = 834;
static const constexpr size_t TEST_PAGE_SIZE = 1024;
static const constexpr size_t TEST_NUM_PAGES = 3;

class BTreeTest : public ::testing::Test {
  protected:
	// Runs *before* each TEST_F
	void SetUp() override { buffer_manager_.reset(BTREE_SEGMENT_ID); }

	// Runs *after* each TEST_F
	void TearDown() override {}

	/// The Buffer Manager of the B-Tree.
	BufferManager buffer_manager_{TEST_PAGE_SIZE, TEST_NUM_PAGES};
	/// The B-Tree under test.
	BTree<uint64_t, uint64_t> btree_{BTREE_SEGMENT_ID, buffer_manager_};
};

// TODO: Add test that checks that all pages are not in use after using the
// BTree.
TEST_F(BTreeTest, SingleNodeLookup) {
	// Init.
	btree_.insert(1, 2);
	EXPECT_EQ(btree_.lookup(1), 2);

	// Insert in sort order.
	btree_.insert(3, 4);
	EXPECT_EQ(btree_.lookup(1), 2);
	EXPECT_EQ(btree_.lookup(3), 4);
	EXPECT_FALSE(btree_.lookup(2).has_value());

	// Insert out of sort order.
	btree_.insert(2, 6);
	EXPECT_EQ(btree_.lookup(1), 2);
	EXPECT_EQ(btree_.lookup(3), 4);
	EXPECT_EQ(btree_.lookup(2), 6);

	// Duplicate keys throw.
	EXPECT_THROW(btree_.insert(2, 7), std::logic_error);
}

/// A Tree is bootstrapped correctly at initialization.
TEST_F(BTreeTest, Startup) {
	EXPECT_THROW(btree_.lookup(59834), std::logic_error);
}
/// A Tree is picked up from storage when it exists already.
TEST_F(BTreeTest, Pickup) {}
/// A key and value can be retrieved from the tree after insertion.
TEST_F(BTreeTest, Get) {}
/// A B-Tree can split its nodes.
TEST_F(BTreeTest, NodeSplit) {}
/// Search of an empty node works as expected.
} // namespace