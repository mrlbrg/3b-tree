#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"

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
static const constexpr size_t TEST_PAGE_SIZE = 1024;
static const constexpr size_t TEST_NUM_PAGES = 3;

class BTreeTest : public ::testing::Test {
  protected:
	void Destroy(bool reset) {
		// Destroy B-Tree. Uses the buffer manager to persist state.
		btree_ = nullptr;
		// Destroy & create buffer manager.
		buffer_manager_ =
			std::make_unique<BufferManager>(TEST_PAGE_SIZE, TEST_NUM_PAGES);
		// Deletes B-Tree state.
		if (reset)
			buffer_manager_->reset(BTREE_SEGMENT_ID);
		// Destroy & create new B-Tree.
		btree_ = std::make_unique<BTree<uint64_t, uint64_t>>(BTREE_SEGMENT_ID,
															 *buffer_manager_);
	}

	// Create a blank B-Tree.
	void SetUp() override { Destroy(true); }

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
				btree_->insert(key, value);
				expected_map[key] = value;
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
	btree_->insert(1, 2);
	EXPECT_EQ(btree_->lookup(1), 2);

	// Insert in sort order.
	btree_->insert(3, 4);
	EXPECT_EQ(btree_->lookup(1), 2);
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_FALSE(btree_->lookup(2).has_value());

	// Insert out of sort order.
	btree_->insert(2, 6);
	EXPECT_EQ(btree_->lookup(1), 2);
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(2), 6);

	// Duplicate keys throw.
	EXPECT_THROW(btree_->insert(2, 7), std::logic_error);
}

/// A Tree is bootstrapped correctly at initialization.
TEST_F(BTreeTest, Persistency) {
	// Insert on a blank B-Tree.
	btree_->insert(1, 2);
	btree_->insert(5, 6);
	btree_->insert(3, 4);
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(5), 6);
	EXPECT_EQ(btree_->lookup(1), 2);

	// Destroy B-Tree but persist state on disk.
	Destroy(false);

	// New B-Tree should pick up previous state.
	EXPECT_EQ(btree_->lookup(3), 4);
	EXPECT_EQ(btree_->lookup(5), 6);
	EXPECT_EQ(btree_->lookup(1), 2);
}
/// A Tree can grow beyond a single node.
TEST_F(BTreeTest, NodeSplit) {
	// Number of tuples requires to force a node split.
	const uint64_t max_tuples_per_leaf =
		TEST_PAGE_SIZE / (sizeof(Key) + sizeof(Value));

	// Force a node split during inserts.
	auto expected_values = Seed(max_tuples_per_leaf * 2);

	// Lookup all values.
	for (auto &[k, v] : expected_values) {
		EXPECT_EQ(btree_->lookup(k), v);
	}
}
/// Search of an empty node works as expected.
} // namespace