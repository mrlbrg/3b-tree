#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <random>

using namespace bbbtree;

namespace {

using Key = uint64_t;
using Value = uint64_t;

static const constexpr size_t BTREE_SEGMENT_ID = 834;
static const constexpr size_t TEST_PAGE_SIZE = 256;
static const constexpr size_t TEST_NUM_PAGES = 10;
const uint64_t TUPLES_PER_LEAF =
	(TEST_PAGE_SIZE - sizeof(BTree<Key, Value>::LeafNode)) /
	(sizeof(BTree<Key, Value>::LeafNode::Slot) + sizeof(Key) + sizeof(Value));
const uint64_t TUPLES_PER_NODE =
	(TEST_PAGE_SIZE - sizeof(BTree<Key, Value>::InnerNode)) /
	(sizeof(BTree<Key, Value>::InnerNode::Slot) + sizeof(Key) + sizeof(Value));

class BTreeTest : public ::testing::Test {
  protected:
	void Reset(bool clear_files) {
		// Destroy B-Tree. Uses the buffer manager to persist state.
		btree_ = nullptr;
		// Destroy & create buffer manager.
		buffer_manager_ = std::make_unique<BufferManager>(
			TEST_PAGE_SIZE, TEST_NUM_PAGES, clear_files);
		// Destroy & create new B-Tree.
		btree_ = std::make_unique<BTree<uint64_t, uint64_t>>(BTREE_SEGMENT_ID,
															 *buffer_manager_);
	}

	// Create a blank B-Tree.
	void SetUp() override {
		Reset(true);
		stats = Stats{};
	}

	void TearDown() override {}

	std::unordered_map<Key, Value> Seed(size_t num_tuples) {
		std::unordered_map<Key, Value> expected_map;

		std::mt19937_64 rng(42); // Fixed seed for reproducibility
		std::uniform_int_distribution<uint64_t> dist;

		// Generate random tuples with unique keys
		while (expected_map.size() < num_tuples) {
			Key key = dist(rng);
			Value value = dist(rng);

			if (expected_map.count(key) == 0) {
				EXPECT_TRUE(btree_->insert(key, value));
				expected_map[key] = value;
				EXPECT_EQ(expected_map.size(), btree_->size());
			} else {
				EXPECT_FALSE(btree_->insert(key, value));
			}
		}

		return expected_map;
	}

	/// The Buffer Manager of the B-Tree.
	std::unique_ptr<BufferManager> buffer_manager_;
	/// The B-Tree under test.
	std::unique_ptr<BTree<Key, Value>> btree_;
};

// TODO: Add test that checks that all pages are not in use after using the
// BTree.
TEST_F(BTreeTest, SingleNodeLookup) {
	// Init.
	EXPECT_TRUE(btree_->insert(1, 2));
	EXPECT_EQ(btree_->lookup(1), 2);

	// Insert in sort order.
	EXPECT_TRUE(btree_->insert(3, 4));
	EXPECT_EQ(btree_->lookup(1), 2);
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_FALSE(btree_->lookup(2).has_value());

	// Insert out of sort order.
	EXPECT_TRUE(btree_->insert(2, 6));
	EXPECT_EQ(btree_->lookup(1), 2);
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(2), 6);

	// Duplicate keys throw.
	EXPECT_FALSE(btree_->insert(2, 7));
}

/// A Tree is bootstrapped correctly at initialization.
TEST_F(BTreeTest, Persistency) {
	// Insert on a blank B-Tree.
	EXPECT_TRUE(btree_->insert(1, 2));
	EXPECT_TRUE(btree_->insert(5, 6));
	EXPECT_TRUE(btree_->insert(3, 4));
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(5), 6);
	EXPECT_EQ(btree_->lookup(1), 2);

	// Destroy B-Tree but persist state on disk.
	Reset(false);

	// New B-Tree should pick up previous state.
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(5), 6);
	EXPECT_EQ(btree_->lookup(1), 2);
}
/// A Tree can grow beyond a single node.
TEST_F(BTreeTest, LeafSplits) {
	// Force a leaf split during inserts.
	auto expected_values = Seed(TUPLES_PER_LEAF * 5);

	ASSERT_EQ(btree_->size(), expected_values.size());

	// Lookup all values.
	for (auto &[k, v] : expected_values) {
		auto res = btree_->lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// A tree can grow beyond one level.
TEST_F(BTreeTest, InnerNodeSplits) {
	auto num_tuples = TUPLES_PER_LEAF * TUPLES_PER_NODE * TUPLES_PER_NODE * 2;
	auto expected_values = Seed(num_tuples);

	ASSERT_EQ(btree_->size(), expected_values.size());

	// Lookup all values.
	for (auto &[k, v] : expected_values) {
		auto res = btree_->lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// Search of an empty node works as expected.
} // namespace