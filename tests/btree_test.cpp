#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <cstddef>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <random>
#include <type_traits>
#include <unordered_map>
#include <vector>

using namespace bbbtree;

namespace {
static const constexpr size_t TEST_PAGE_SIZE = 256;
static const constexpr size_t TEST_NUM_PAGES = 50;
static const constexpr size_t BUFFER_SIZE = TEST_PAGE_SIZE * TEST_NUM_PAGES;

static std::vector<std::byte> get_random_bytes(size_t num_bytes) {
	static std::mt19937 gen(42); // Mersenne Twister engine
	static std::uniform_int_distribution<uint8_t> dist(65, 90);

	std::vector<std::byte> res;
	res.resize(num_bytes);
	for (auto &byte : res)
		byte = std::byte(dist(gen));

	return res;
}

/// A B-Tree that can be seeded with randomly generated tuples.
template <KeyIndexable KeyT, ValueIndexable ValueT>
struct SeedableBTree : public BTree<KeyT, ValueT> {

	static const constexpr size_t SPACE_ON_LEAF =
		TEST_PAGE_SIZE - SeedableBTree::LeafNode::min_space;
	static const constexpr size_t SPACE_ON_NODE =
		TEST_PAGE_SIZE - SeedableBTree::InnerNode::min_space;

	/// Constructor.
	SeedableBTree(SegmentID segment_id, BufferManager &buffer_manager)
		: BTree<KeyT, ValueT>(segment_id, buffer_manager) {}

	/// Seed the tree with random key/value pairs.
	/// @insert_size: the number of bytes to be inserted in total.
	/// @min_size: the minimum number of bytes an inserted key should have.
	void seed(size_t insert_size, size_t min_size = 1) {
		auto get_kv_size = [min_size]() -> std::pair<uint16_t, uint16_t> {
			static std::random_device rd;
			static std::mt19937 gen(rd());
			uint16_t key_size, value_size;
			auto const constexpr max_size =
				std::min(SPACE_ON_LEAF, SPACE_ON_NODE);
			assert(min_size < max_size);
			// Get Key Size
			if constexpr (std::is_same_v<KeyT, UInt64>) {
				key_size = sizeof(KeyT);
			} else {
				std::uniform_int_distribution<uint16_t> dist(min_size,
															 max_size - 1);
				key_size = dist(gen);
			}
			// Get Value Size
			if constexpr (std::is_same_v<ValueT, UInt64>) {
				value_size = sizeof(ValueT);
			} else {
				std::uniform_int_distribution<uint16_t> dist(1, max_size -
																	key_size);
				value_size = dist(gen);
			}

			return {key_size, value_size};
		};

		size_t num_bytes = 0;
		while (num_bytes < insert_size) {
			// Get random key and value.
			auto [key_size, value_size] = get_kv_size();
			data.push_back(
				{get_random_bytes(key_size), get_random_bytes(value_size)});
			auto &[key_data, value_data] = data.back();

			KeyT key = KeyT::deserialize(&key_data[0], key_data.size());
			ValueT value =
				ValueT::deserialize(&value_data[0], value_data.size());

			// Insert into tree.
			if (expected_map.count(key) == 0) {
				bool success = this->insert(key, value);
				assert(success);
				expected_map[key] = value;
				assert(expected_map.size() == this->size());
				num_bytes += key_size + value_size;
				assert(this->height() < TEST_PAGE_SIZE);
			} else {
				assert(this->insert(key, value) == false);
				data.pop_back();
			}
		}
	}

	/// Validates that all previously `seed`ed key/value pairs are still
	/// present in the tree.
	/// @return: true if the tree contains all expected key/value pairs.
	/// false otherwise.
	bool validate() {
		// Validate size
		if (this->size() != expected_map.size())
			return false;

		// Validate stored values.
		for (const auto &[key, value] : expected_map) {
			auto res = this->lookup(key);
			if (!res.has_value())
				return false;
			if (res.value() != value)
				return false;
		}

		return true;
	}

	using KeyData = std::vector<std::byte>;
	using ValueData = std::vector<std::byte>;

	/// The buffer that owns the Key/Value data. Simulates page data.
	std::vector<std::pair<KeyData, ValueData>> data;
	/// The key/value pairs that are expected to live in the tree.
	std::unordered_map<KeyT, ValueT> expected_map;
};

// Tree types under test.
using BTreeInt = SeedableBTree<UInt64, UInt64>;
using BTreeString = SeedableBTree<String, UInt64>;
using BTreeStringToString = SeedableBTree<String, String>;

// A shared buffer manager.
class BTreeTest : public ::testing::Test {
  protected:
	void Reset(bool clear_files) {
		// Call destructors to ensure files are written to disk.
		btree_int_.reset();
		btree_str_.reset();
		btree_str_to_str_.reset();
		buffer_manager_.reset();
		// Create anew.
		buffer_manager_ = std::make_unique<BufferManager>(
			TEST_PAGE_SIZE, TEST_NUM_PAGES, clear_files);
		btree_int_ = std::make_unique<BTreeInt>(834, *buffer_manager_);
		btree_str_ = std::make_unique<BTreeString>(835, *buffer_manager_);
		btree_str_to_str_ =
			std::make_unique<BTreeStringToString>(836, *buffer_manager_);
	}

	void SetUp() override { Reset(true); }

	std::unique_ptr<BufferManager> buffer_manager_;
	std::unique_ptr<BTreeInt> btree_int_;
	std::unique_ptr<BTreeString> btree_str_;
	std::unique_ptr<BTreeStringToString> btree_str_to_str_;
};

// TODO: Add test that checks that all pages are not in use after using the
// BTree.
TEST_F(BTreeTest, SingleNodeLookup) {
	// Insert.
	EXPECT_FALSE(btree_int_->lookup(1).has_value());
	EXPECT_TRUE(btree_int_->insert(1, 2));
	EXPECT_EQ(btree_int_->lookup(1).value(), UInt64(2));

	// Insert in sort order.
	EXPECT_TRUE(btree_int_->insert(3, 4));
	EXPECT_EQ(btree_int_->lookup(1).value(), UInt64(2));
	EXPECT_EQ(btree_int_->lookup(3).value(), UInt64(4));
	EXPECT_FALSE(btree_int_->lookup(2).has_value());

	// Insert out of sort order.
	EXPECT_TRUE(btree_int_->insert(2, 6));
	EXPECT_EQ(btree_int_->lookup(1).value(), UInt64(2));
	EXPECT_EQ(btree_int_->lookup(3).value(), UInt64(4));
	EXPECT_EQ(btree_int_->lookup(2).value(), UInt64(6));

	// Duplicate keys throw.
	EXPECT_FALSE(btree_int_->insert(2, 7));
}
/// A Tree is bootstrapped correctly at initialization.
TEST_F(BTreeTest, Persistency) {
	{
		// Insert on a blank B-Tree.
		EXPECT_TRUE(btree_int_->insert(1, 2));
		EXPECT_TRUE(btree_int_->insert(5, 6));
		EXPECT_TRUE(btree_int_->insert(3, 4));
		EXPECT_EQ(btree_int_->lookup(3).value(), UInt64(4));
		EXPECT_EQ(btree_int_->lookup(5).value(), UInt64(6));
		EXPECT_EQ(btree_int_->lookup(1).value(), UInt64(2));
	}

	// Destroy B-Tree but persist state on disk.
	Reset(false);

	{
		// New B-Tree should pick up previous state.
		EXPECT_EQ(btree_int_->lookup(3).value(), UInt64(4));
		EXPECT_EQ(btree_int_->lookup(5).value(), UInt64(6));
		EXPECT_EQ(btree_int_->lookup(1).value(), UInt64(2));
	}
}
/// A Tree can grow beyond a single node.
TEST_F(BTreeTest, LeafSplits) {
	// Force a leaf split during inserts.
	auto leaf_splits_before = stats.leaf_node_splits;
	btree_int_->seed(BTreeInt::SPACE_ON_LEAF * 2);
	auto leaf_splits_after = stats.leaf_node_splits;

	EXPECT_TRUE(leaf_splits_before < leaf_splits_after);
	EXPECT_TRUE(btree_int_->validate());

	std::cout << "Leaf splits: " << (leaf_splits_after - leaf_splits_before)
			  << std::endl;
}
/// A tree can grow beyond one level.
TEST_F(BTreeTest, InnerNodeSplits) {
	auto node_splits_before = stats.inner_node_splits;
	btree_int_->seed(BTreeInt::SPACE_ON_LEAF * BTreeInt::SPACE_ON_NODE);
	auto node_splits_after = stats.inner_node_splits;

	// There were node splits
	EXPECT_TRUE(node_splits_before < node_splits_after);
	EXPECT_TRUE(btree_int_->validate());

	std::cout << "Inner node splits: "
			  << (node_splits_after - node_splits_before) << std::endl;
}
/// A tree can handle spilling nodes to disk.
TEST_F(BTreeTest, Spilling) {
	auto page_swaps_before = stats.pages_evicted;
	btree_str_->seed(BUFFER_SIZE);
	auto page_swaps_after = stats.pages_evicted;

	EXPECT_TRUE(page_swaps_before < page_swaps_after);
	EXPECT_TRUE(btree_str_->validate());

	std::cout << "Keys inserted: " << btree_str_->expected_map.size()
			  << std::endl;
	std::cout << "Pages swapped: " << (page_swaps_after - page_swaps_before)
			  << std::endl;
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, StringKeys) {
	String str1{"Hello World!"};
	String str2{"How are you today?"};
	String str3{"Hallo Welt!!"};

	EXPECT_TRUE(btree_str_->insert(str1, 1));
	EXPECT_FALSE(btree_str_->insert(str1, 1));
	EXPECT_TRUE(btree_str_->insert(str2, 2));
	EXPECT_TRUE(btree_str_->insert(str3, 2));

	EXPECT_EQ(btree_str_->lookup(str1).value(), UInt64(1));
	EXPECT_EQ(btree_str_->lookup(str2).value(), UInt64(2));
	EXPECT_EQ(btree_str_->lookup(str3).value(), UInt64(2));
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, BinaryTree) {
	// Seed keys which require a single leaf each.
	btree_str_->seed(5 * BTreeString::SPACE_ON_LEAF,
					 size_t(0.75 * BTreeString::SPACE_ON_LEAF));
	EXPECT_TRUE(btree_str_->validate());
}
/// A tree cannot store entries bigger than a page.
TEST_F(BTreeTest, TooLargeKeys) {
	std::vector<std::byte> buffer(BTreeString::SPACE_ON_LEAF + 1);
	String str{{reinterpret_cast<char *>(&buffer[0]), buffer.size()}};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
	EXPECT_THROW(btree_str_->insert(str, 1), std::logic_error);
#pragma GCC diagnostic pop
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, LargeStringTree) {
	// This sometimes fails due to a buffer full error.
	btree_str_->seed(BTreeString::SPACE_ON_LEAF * 1000);
	EXPECT_TRUE(btree_str_->validate());
}
/// A tree can store variable sized values.
TEST_F(BTreeTest, VariableSizeValues) {
	btree_str_to_str_->seed(BTreeString::SPACE_ON_LEAF * 1000);
	std::cout << "Keys inserted: " << btree_str_to_str_->expected_map.size()
			  << std::endl;
	std::cout << "Tree height: " << btree_str_to_str_->height() << std::endl;
	EXPECT_TRUE(btree_str_->validate());
}
/// A tree can handle thousands of variable sized keys and values. TODO.
} // namespace