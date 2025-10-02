#include "bbbtree/btree.h"
#include "bbbtree/database.h"
#include "bbbtree/types.h"

#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

using namespace bbbtree;

namespace {
using IntDatabase = Database<BTree, UInt64>;
using StringDatabase = Database<BTree, String>;

static const constexpr size_t TEST_PAGE_SIZE = 1024;
static const constexpr size_t TEST_NUM_PAGES = 10;

static std::vector<std::byte> get_random_bytes(size_t num_bytes) {
	static std::mt19937 gen(42); // Mersenne Twister engine
	static std::uniform_int_distribution<uint8_t> dist(65, 90);

	std::vector<std::byte> res;
	res.resize(num_bytes);
	for (auto &byte : res)
		byte = std::byte(dist(gen));

	return res;
}

class IntDatabaseTest : public ::testing::Test {
  protected:
	void Destroy(bool reset) {
		// Call destructors. Required before calling `make_unique`. Otherwise
		// destructor is called after constructor, but we need the files written
		// to disk before constructing anew.
		db_.reset();
		db_ = std::make_unique<IntDatabase>(TEST_PAGE_SIZE, TEST_NUM_PAGES, 0,
											reset);
		if (reset)
			expected_map.clear();
	}

	void Seed(size_t num_tuples) {
		std::vector<IntDatabase::Tuple> tuples;

		std::mt19937_64 rng(42); // Fixed seed for reproducibility
		std::uniform_int_distribution<uint64_t> dist;

		// Generate random tuples with unique keys
		while (expected_map.size() < num_tuples) {
			UInt64 key = dist(rng);
			auto value = dist(rng);

			if (expected_map.count(key) == 0) {
				IntDatabase::Tuple t{key, value};
				tuples.push_back(t);
				expected_map[key] = t;
			}
		}
		db_->insert(tuples);
	}

	void Validate() {
		// Validate size.
		ASSERT_EQ(db_->size(), expected_map.size());

		// Validate stored values.
		for (const auto &[key, expected_tuple] : expected_map) {
			auto tuple = db_->get({key});
			EXPECT_EQ(tuple.key, expected_tuple.key);
			EXPECT_EQ(tuple.value, expected_tuple.value);
		}
	}

	/// The database under test. When changing these parameters, also change the
	/// test's parameters, e.g. when testing in or out of memory behavior.
	std::unique_ptr<IntDatabase> db_ =
		std::make_unique<IntDatabase>(TEST_PAGE_SIZE, TEST_NUM_PAGES, 0, true);
	// The key/value pairs that are expected to live in the database.
	std::unordered_map<UInt64, IntDatabase::Tuple> expected_map{};
};

class StringDatabaseTest : public ::testing::Test {
  protected:
	void Destroy(bool reset) {
		// Call destructors. Required before calling `make_unique`. Otherwise
		// destructor is called after constructor, but we need the files written
		// to disk before constructing anew.
		db_.reset();
		db_ = std::make_unique<StringDatabase>(TEST_PAGE_SIZE, TEST_NUM_PAGES,
											   0, reset);
		if (reset)
			expected_map.clear();
	}

	void Seed(size_t num_tuples) {
		auto get_key_size = []() -> uint16_t {
			size_t const constexpr max_size = TEST_PAGE_SIZE * 0.6;
			static std::random_device rd;
			static std::mt19937 gen(rd());
			std::exponential_distribution<double> dist(0.05);
			uint16_t key_size = static_cast<uint16_t>(dist(gen)) + 1;
			if (key_size > max_size)
				key_size = max_size;
			return key_size;
		};

		std::mt19937_64 rng(42); // Fixed seed for reproducibility
		std::uniform_int_distribution<uint64_t> dist;

		// Generate random tuples with unique keys
		while (expected_map.size() < num_tuples) {
			auto key_size = get_key_size();
			auto key_data = get_random_bytes(key_size);
			std::string key{reinterpret_cast<const char *>(key_data.data()),
							key_size};
			if (expected_map.count(key) == 0) {
				auto value = dist(rng);
				StringDatabase::Tuple t{{key}, value};
				db_->insert(t);
				expected_map.emplace(std::move(key), t);
			}
		};
	}

	void Validate() {
		// Validate size.
		ASSERT_EQ(db_->size(), expected_map.size());

		// Validate stored values.
		for (const auto &[key, expected_tuple] : expected_map) {
			auto tuple = db_->get({key});
			EXPECT_EQ(tuple.key, expected_tuple.key);
			EXPECT_EQ(tuple.value, expected_tuple.value);
		}
	}

	/// The database under test. When changing these parameters, also change the
	/// test's parameters, e.g. when testing in or out of memory behavior.
	std::unique_ptr<StringDatabase> db_ = std::make_unique<StringDatabase>(
		TEST_PAGE_SIZE, TEST_NUM_PAGES, 0, true);
	/// The key/value pairs that are expected to live in the database.
	std::unordered_map<std::string, StringDatabase::Tuple> expected_map{};
};

// A tuple inserted can be read again when all data fits in memory buffer.
TEST_F(IntDatabaseTest, InMemory) {
	// Calculate the number of tuples that fit into the buffer.
	const size_t num_tuples =
		TEST_PAGE_SIZE * TEST_NUM_PAGES / sizeof(IntDatabase::Tuple) / 2;

	Seed(num_tuples);
	EXPECT_EQ(db_->size(), expected_map.size());

	// Verify correctness
	for (const auto &[key, expectedTuple] : expected_map) {
		auto actual = db_->get(key);
		EXPECT_EQ(expectedTuple, actual);
	}
}

// The same key can only be inserted once.
TEST_F(IntDatabaseTest, DuplicateKeys) {
	EXPECT_NO_THROW(db_->insert({IntDatabase::Tuple{0, 1}}));
	EXPECT_THROW(db_->insert({IntDatabase::Tuple{0, 2}}), std::logic_error);
}
// A tuple can be inserted and read again, also if data exceeds buffer size.
TEST_F(IntDatabaseTest, OutOfMemory) {
	// Calculate the number of tuples that overflow  the buffer.
	const size_t num_tuples =
		TEST_PAGE_SIZE * TEST_NUM_PAGES / sizeof(IntDatabase::Tuple) * 10;

	Seed(num_tuples);
	EXPECT_EQ(db_->size(), expected_map.size());

	// Verify correctness
	for (const auto &[key, expectedTuple] : expected_map) {
		EXPECT_EQ(expectedTuple, db_->get(key));
	}
}
// A tuple inserted can be read again, also after destroying the database.
TEST_F(IntDatabaseTest, Persistency) {
	// Calculate the number of tuples that overflow the buffer.
	const size_t num_tuples = 10;

	Seed(num_tuples);
	Validate();
	Destroy(false);
	Validate();
}
// A database can store variable sized keys.
TEST_F(StringDatabaseTest, VariableSizedKeys) {
	Seed(1000);
	Validate();
	Seed(1000);
	Validate();
}
} // namespace