#include "bbbtree/delta.h"
#include "bbbtree/types.h"

#include <cstddef>
#include <gtest/gtest.h>
#include <random>

using namespace bbbtree;

namespace {
// -----------------------------------------------------------------
using IntDelta = Delta<UInt64, TID>;
using StringDelta = Delta<String, TID>;
using IntDeltas = Deltas<UInt64, TID>;
using StringDeltas = Deltas<String, TID>;
using IntDeltaTree = BTree<PID, IntDeltas>;
using StringDeltaTree = BTree<PID, StringDeltas>;
// -----------------------------------------------------------------
class DeltaTest : public ::testing::Test {
  protected:
	static const constexpr size_t TEST_PAGE_SIZE = 256;
	static const constexpr size_t TEST_NUM_PAGES = 10;

	std::unique_ptr<BufferManager> buffer_manager_ =
		std::make_unique<BufferManager>(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);
};
// -----------------------------------------------------------------
/// A Delta is serialized and deserialized correctly.
TEST_F(DeltaTest, DeltaSerialization) {

	{
		IntDelta delta{OperationType::Insert, 42, 1001};
		std::vector<std::byte> buffer(delta.size());
		delta.serialize(buffer.data());
		auto deserialized = IntDelta::deserialize(buffer.data());
		EXPECT_EQ(delta, deserialized);
		EXPECT_EQ(delta.size(), deserialized.size());
	}

	{
		StringDelta delta{OperationType::Insert, {"Hello"}, 1001};
		std::vector<std::byte> buffer(delta.size());
		delta.serialize(buffer.data());
		auto deserialized = StringDelta::deserialize(buffer.data());
		EXPECT_EQ(delta, deserialized);
		EXPECT_EQ(delta.size(), deserialized.size());
	}
}
/// Deltas are serialized and deserialized correctly.
TEST_F(DeltaTest, DeltasSerialization) {
	// Empty deltas.
	{
		IntDeltas deltas{{}};

		std::vector<std::byte> buffer(deltas.size());
		deltas.serialize(buffer.data());
		auto deserialized =
			IntDeltas::deserialize(buffer.data(), buffer.size());

		EXPECT_EQ(deltas.size(), deserialized.size());
		EXPECT_EQ(deltas, deserialized);
	}
	// Int deltas.
	{
		IntDeltas deltas{{{OperationType::Insert, 42, 1001},
						  {OperationType::Update, 43, 1002},
						  {OperationType::Delete, 44, 1003}}};

		std::vector<std::byte> buffer(deltas.size());
		deltas.serialize(buffer.data());
		auto deserialized =
			IntDeltas::deserialize(buffer.data(), buffer.size());

		EXPECT_EQ(deltas.size(), deserialized.size());
		EXPECT_EQ(deltas, deserialized);
	}
	// String deltas.
	{
		StringDeltas deltas{{{OperationType::Insert, {"Hello"}, 1001},
							 {OperationType::Update, {"World"}, 1002},
							 {OperationType::Delete, {"!"}, 1003}}};

		std::vector<std::byte> buffer(deltas.size());
		deltas.serialize(buffer.data());
		auto deserialized =
			StringDeltas::deserialize(buffer.data(), buffer.size());

		EXPECT_EQ(deltas.size(), deserialized.size());
		EXPECT_EQ(deltas, deserialized);
	}
}
/// Deltas can be stored in a BTree.
TEST_F(DeltaTest, DeltaTree) {
	IntDeltaTree delta_tree{342, *buffer_manager_};

	auto expected_deltas1 = IntDeltas{
		{{OperationType::Insert, 42, 1001}, {OperationType::Update, 45, 1004}}};
	auto expected_deltas2 = IntDeltas{{}};
	auto expected_deltas3 = IntDeltas{{{OperationType::Delete, 44, 1003},
									   {OperationType::Update, 43, 1002},
									   {OperationType::Insert, 46, 1003}}};

	// Insert some deltas.
	EXPECT_TRUE(delta_tree.insert(1, expected_deltas1));
	EXPECT_TRUE(delta_tree.insert(2, expected_deltas2));
	EXPECT_TRUE(delta_tree.insert(3, expected_deltas3));
	EXPECT_TRUE(delta_tree.insert(4, expected_deltas3));

	// Check if the deltas can be retrieved.
	auto deltas1 = delta_tree.lookup(1);
	ASSERT_TRUE(deltas1.has_value());
	EXPECT_EQ(deltas1.value(), expected_deltas1);

	auto deltas2 = delta_tree.lookup(2);
	ASSERT_TRUE(deltas2.has_value());
	EXPECT_EQ(deltas2.value(), expected_deltas2);

	auto deltas3 = delta_tree.lookup(3);
	ASSERT_TRUE(deltas3.has_value());
	EXPECT_EQ(deltas3.value(), expected_deltas3);

	auto deltas4 = delta_tree.lookup(4);
	ASSERT_TRUE(deltas4.has_value());
	EXPECT_EQ(deltas4.value(), expected_deltas3);
}
/// A BTree can store deltas with variable sized keys.
TEST_F(DeltaTest, VariableSizedDeltas) {
	static constexpr size_t NUM_DELTAS = 100;
	static constexpr size_t MAX_KEY_SIZE = 32;
	static constexpr size_t NUM_TREE_ENTRIES = 20;

	std::mt19937 rng(42);
	std::uniform_int_distribution<size_t> key_size_dist(1, MAX_KEY_SIZE);
	std::uniform_int_distribution<int> op_dist(0, 2);
	std::uniform_int_distribution<int> value_dist(1000, 9999);

	StringDeltaTree delta_tree{123, *buffer_manager_};
	std::vector<std::string> keys;
	std::vector<StringDeltas> expected_deltas;

	// Generate random strings for keys to use as views later.
	for (size_t i = 0; i < NUM_TREE_ENTRIES; ++i) {
		size_t key_len = key_size_dist(rng);
		std::string key;
		for (size_t j = 0; j < key_len; ++j)
			key += static_cast<char>('a' + rng() % 26);
		keys.push_back(std::move(key));
	}

	// Generate random StringDeltas and insert into tree
	for (size_t i = 0; i < NUM_TREE_ENTRIES; ++i) {
		std::vector<StringDelta> deltas;
		size_t num_deltas = 1 + rng() % (NUM_DELTAS / NUM_TREE_ENTRIES);

		for (size_t j = 0; j < num_deltas; ++j) {

			OperationType op = static_cast<OperationType>(op_dist(rng));
			int value = value_dist(rng);

			deltas.emplace_back(op, String{keys[i]}, value);
		}
		expected_deltas.emplace_back(deltas);
		ASSERT_TRUE(delta_tree.insert(i, expected_deltas.back()));
	}

	// Lookup and verify
	for (size_t i = 0; i < NUM_TREE_ENTRIES; ++i) {
		auto found = delta_tree.lookup(i);
		ASSERT_TRUE(found.has_value());
		EXPECT_EQ(found.value(), expected_deltas[i]);
	}
}
/// TODO: Deltas can be erased from the tree.
TEST_F(DeltaTest, Erase) {}
// -----------------------------------------------------------------
} // namespace