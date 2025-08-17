#include "bbbtree/delta.h"
#include "bbbtree/types.h"
#include <cstddef>
#include <gtest/gtest.h>
#include <iostream>

using namespace bbbtree;

namespace {
// -----------------------------------------------------------------
using IntDelta = Delta<UInt64, TID>;
using StringDelta = Delta<String, TID>;
using IntDeltas = Deltas<UInt64, TID>;
using StringDeltas = Deltas<String, TID>;
// -----------------------------------------------------------------
class DeltaTest : public ::testing::Test {};
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

	{
		StringDeltas deltas{{{OperationType::Insert, {"Hello"}, 1001},
							 {OperationType::Update, {"World"}, 1002},
							 {OperationType::Delete, {"!"}, 1003}}};

		// Serialize and deserialize the deltas.
		std::vector<std::byte> buffer(deltas.size());
		deltas.serialize(buffer.data());
		auto deserialized =
			StringDeltas::deserialize(buffer.data(), buffer.size());

		EXPECT_EQ(deltas.size(), deserialized.size());
		EXPECT_EQ(deltas, deserialized);
	}
}
/// Deltas can be stored in a BTree.
TEST_F(DeltaTest, DeltaTree) {}
/// Deltas cannot exceed BTree nodes.
TEST_F(DeltaTest, DeltasLimit) {}
/// A BTree can store large deltas.
TEST_F(DeltaTest, LargeDeltas) {}
/// A BTree can store deltas with variable sized keys.
TEST_F(DeltaTest, VariableSizedKeys) {}
/// Deltas can be erased from the tree.
TEST_F(DeltaTest, Erase) {}
// -----------------------------------------------------------------
} // namespace