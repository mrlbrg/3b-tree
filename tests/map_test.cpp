#include "bbbtree/map.h"

#include <cstddef>
#include <gtest/gtest.h>
#include <random>

using namespace bbbtree;

namespace {

using Key = uint64_t;
using Value = uint64_t;

class MapTest : public ::testing::Test {
  protected:
	void Reset() {
		map_ = nullptr;
		map_ = std::make_unique<Map<Key, Value>>();
	}

	// Create a blank Map.
	void SetUp() override { Reset(); }

	void TearDown() override {}

	std::unordered_map<Key, Value> Seed(size_t num_tuples) {
		std::unordered_map<Key, Value> expected_map;

		std::mt19937_64 rng(42); // Fixed seed for reproducibility
		std::uniform_int_distribution<Key> dist;

		// Generate random tuples with unique keys
		while (expected_map.size() < num_tuples) {
			Key key = dist(rng);
			Value value = dist(rng);

			if (expected_map.count(key) == 0) {
				EXPECT_TRUE(map_->insert(key, value));
				expected_map[key] = value;
				EXPECT_EQ(expected_map.size(), map_->size());
			} else {
				EXPECT_FALSE(map_->insert(key, value));
			}
		}

		return expected_map;
	}

	/// The Map under test.
	std::unique_ptr<Map<Key, Value>> map_;
};
/// Can insert and lookup a few values.
TEST_F(MapTest, Inserts) {
	// Init.
	EXPECT_FALSE(map_->lookup(1).has_value());
	EXPECT_TRUE(map_->insert(1, 2));
	EXPECT_EQ(map_->lookup(1), 2);

	// Insert in sort order.
	EXPECT_TRUE(map_->insert(3, 4));
	EXPECT_EQ(map_->lookup(1), 2);
	EXPECT_EQ(map_->lookup(3), 4);
	EXPECT_FALSE(map_->lookup(2).has_value());

	// Insert out of sort order.
	EXPECT_TRUE(map_->insert(2, 6));
	EXPECT_EQ(map_->lookup(1), 2);
	EXPECT_EQ(map_->lookup(3), 4);
	EXPECT_EQ(map_->lookup(2), 6);

	// Duplicate keys throw.
	EXPECT_FALSE(map_->insert(2, 7));
}
/// In-memory map looses state after destruction.
TEST_F(MapTest, Persistency) {
	// Insert on a blank Map.
	EXPECT_TRUE(map_->insert(1, 2));
	EXPECT_TRUE(map_->insert(5, 6));
	EXPECT_TRUE(map_->insert(3, 4));
	EXPECT_EQ(map_->lookup(3), 4);
	EXPECT_EQ(map_->lookup(5), 6);
	EXPECT_EQ(map_->lookup(1), 2);

	// Destroy Map.
	Reset();

	// Map should have lost state.
	EXPECT_FALSE(map_->lookup(1).has_value());
}
/// Map can store and lookup many tuples.
TEST_F(MapTest, Lookups) {
	auto expected_values = Seed(1'000);

	ASSERT_EQ(map_->size(), expected_values.size());

	// Lookup all values.
	for (auto &[k, v] : expected_values) {
		auto res = map_->lookup(k);
		ASSERT_TRUE(res.has_value());
		EXPECT_EQ(res.value(), v);
	}
}
/// Search of an empty node works as expected.
} // namespace