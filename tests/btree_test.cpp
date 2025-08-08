#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <type_traits>
#include <unordered_map>
#include <vector>

using namespace bbbtree;

namespace {
static const constexpr size_t BTREE_SEGMENT_ID = 834;
static const constexpr size_t TEST_PAGE_SIZE = 256;
static const constexpr size_t TEST_NUM_PAGES = 50;

/// A B-Tree that can be seeded with randomly generated tuples.
template <Indexable KeyT, typename ValueT>
struct SeedableBTree : public BTree<KeyT, ValueT> {

	static const constexpr size_t SPACE_ON_LEAF =
		TEST_PAGE_SIZE - SeedableBTree::LeafNode::min_space;
	static const constexpr size_t SPACE_ON_NODE =
		TEST_PAGE_SIZE - SeedableBTree::InnerNode::min_space;

	static std::vector<std::byte> get_random_bytes(size_t num_bytes) {
		static std::random_device rd;  // Seed
		static std::mt19937 gen(rd()); // Mersenne Twister engine
		static std::uniform_int_distribution<uint8_t> dist(65, 90);

		std::vector<std::byte> res;
		res.resize(num_bytes);
		for (auto &byte : res)
			byte = std::byte(dist(gen));

		return res;
	}

	/// Constructor.
	/// @insert_size: the number of bytes to be inserted in total.
	/// @min_size: the minimum number of bytes an inserted key should have.
	SeedableBTree(BufferManager &buffer_manager, size_t insert_size,
				  size_t min_size = 1)
		: BTree<KeyT, ValueT>(BTREE_SEGMENT_ID, buffer_manager) {
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
				// this->print();
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

// A shared buffer manager.
class BTreeTest : public ::testing::Test {
  protected:
	void Reset(bool clear_files) {
		// Reset buffer manager.
		buffer_manager_ = std::make_unique<BufferManager>(
			TEST_PAGE_SIZE, TEST_NUM_PAGES, clear_files);
	}

	void SetUp() override { Reset(true); }

	// void TearDown() override {}

	std::unique_ptr<BufferManager> buffer_manager_;
};

// TODO: Add test that checks that all pages are not in use after using the
// BTree.
TEST_F(BTreeTest, SingleNodeLookup) {
	BTreeInt btree{*buffer_manager_, 0};
	// Init.
	EXPECT_FALSE(btree.lookup(1).has_value());
	EXPECT_TRUE(btree.insert(1, 2));
	EXPECT_EQ(btree.lookup(1), 2);

	// Insert in sort order.
	EXPECT_TRUE(btree.insert(3, 4));
	EXPECT_EQ(btree.lookup(1), 2);
	EXPECT_EQ(btree.lookup(3), 4);
	EXPECT_FALSE(btree.lookup(2).has_value());

	// Insert out of sort order.
	EXPECT_TRUE(btree.insert(2, 6));
	EXPECT_EQ(btree.lookup(1), 2);
	EXPECT_EQ(btree.lookup(3), 4);
	EXPECT_EQ(btree.lookup(2), 6);

	// Duplicate keys throw.
	EXPECT_FALSE(btree.insert(2, 7));
}

/// A Tree is bootstrapped correctly at initialization.
TEST_F(BTreeTest, Persistency) {
	{
		BTreeInt btree{*buffer_manager_, 0};
		// Insert on a blank B-Tree.
		EXPECT_TRUE(btree.insert(1, 2));
		EXPECT_TRUE(btree.insert(5, 6));
		EXPECT_TRUE(btree.insert(3, 4));
		EXPECT_EQ(btree.lookup(3), 4);
		EXPECT_EQ(btree.lookup(5), 6);
		EXPECT_EQ(btree.lookup(1), 2);
	}

	// Destroy B-Tree but persist state on disk.
	Reset(false);

	{
		BTreeInt btree{*buffer_manager_, 0};
		// New B-Tree should pick up previous state.
		EXPECT_EQ(btree.lookup(3), 4);
		EXPECT_EQ(btree.lookup(5), 6);
		EXPECT_EQ(btree.lookup(1), 2);
	}
}
/// A Tree can grow beyond a single node.
TEST_F(BTreeTest, LeafSplits) {
	// Force a leaf split during inserts.
	auto leaf_splits_before = stats.leaf_node_splits;
	BTreeInt btree{*buffer_manager_, BTreeInt::SPACE_ON_LEAF * 2};
	auto leaf_splits_after = stats.leaf_node_splits;

	EXPECT_TRUE(leaf_splits_before < leaf_splits_after);
	ASSERT_EQ(btree.size(), btree.expected_map.size());

	// Lookup all values.
	for (auto &[k, v] : btree.expected_map) {
		auto res = btree.lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// A tree can grow beyond one level.
TEST_F(BTreeTest, InnerNodeSplits) {
	auto node_splits_before = stats.inner_node_splits;
	BTreeInt btree{*buffer_manager_,
				   BTreeInt::SPACE_ON_LEAF * BTreeInt::SPACE_ON_NODE};
	auto node_splits_after = stats.inner_node_splits;

	// There were node splits
	EXPECT_TRUE(node_splits_before < node_splits_after);
	ASSERT_EQ(btree.size(), btree.expected_map.size());

	// Lookup all values.
	for (auto &[k, v] : btree.expected_map) {
		auto res = btree.lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, StringKeys) {
	BTreeString btree{*buffer_manager_, 0};
	String str1{"Hello World!"};
	String str2{"How are you today?"};
	String str3{"Hallo Welt!!"};

	// CONTINUE HERE: Fix this test for strings. Should not work yet.
	EXPECT_TRUE(btree.insert(str1, 1));
	EXPECT_FALSE(btree.insert(str1, 1));
	EXPECT_TRUE(btree.insert(str2, 2));
	EXPECT_TRUE(btree.insert(str3, 2));

	EXPECT_EQ(btree.lookup(str1), 1);
	EXPECT_EQ(btree.lookup(str2), 2);
	EXPECT_EQ(btree.lookup(str3), 2);
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, BinaryTree) {
	// Seed keys which require a single leaf each.
	BTreeString btree{*buffer_manager_, 5 * BTreeString::SPACE_ON_LEAF,
					  size_t(0.75 * BTreeString::SPACE_ON_LEAF)};

	// Lookup all values.
	for (auto &[k, v] : btree.expected_map) {
		auto res = btree.lookup(k);
		EXPECT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// A tree cannot store entries bigger than a page.
TEST_F(BTreeTest, TooLargeKeys) {
	BTreeString btree{*buffer_manager_, 0};
	std::vector<std::byte> buffer(BTreeString::SPACE_ON_LEAF + 1);
	String str{{reinterpret_cast<char *>(&buffer[0]), buffer.size()}};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
	EXPECT_THROW(btree.insert(str, 1), std::logic_error);
#pragma GCC diagnostic pop
}
/// A tree can store variable sized keys.
TEST_F(BTreeTest, LargeStringTree) {
	BTreeString btree{*buffer_manager_, BTreeString::SPACE_ON_LEAF * 1000};

	// Lookup all values.
	for (auto &[k, v] : btree.expected_map) {
		auto res = btree.lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// A tree can store variable sized values.
TEST_F(BTreeTest, VariableSizeValues) {}
/// A tree can handle thousands of variable sized keys and values. TODO.
} // namespace