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
		auto deserialized = IntDelta::deserialize(buffer.data(), buffer.size());
		EXPECT_EQ(delta, deserialized);
	}

	{
		StringDelta delta{OperationType::Insert, {"Hello"}, 1001};
		std::vector<std::byte> buffer(delta.size());
		delta.serialize(buffer.data());
		auto deserialized =
			StringDelta::deserialize(buffer.data(), buffer.size());
		EXPECT_EQ(delta, deserialized);
	}
}
/// Deltas are serialized and deserialized correctly.
TEST_F(DeltaTest, DeltasSerialization) {}
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