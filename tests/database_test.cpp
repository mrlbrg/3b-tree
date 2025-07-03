#include "bbbtree/database.h"

#include <gtest/gtest.h>
#include <vector>
#include <unordered_map>
#include <random>
#include <cstdint>
#include <algorithm>
#include <memory>
#include <utility>

using namespace bbbtree;

namespace
{
    using TestIndex = std::unordered_map<Tuple::Key, TID>;

    static const constexpr size_t TEST_PAGE_SIZE = 1024;
    static const constexpr size_t TEST_NUM_PAGES = 3;

    class DatabaseTest : public ::testing::Test
    {
    protected:
        void RestructDB(bool reset)
        {
            db_ = std::make_unique<Database<TestIndex>>(TEST_PAGE_SIZE, TEST_NUM_PAGES, reset);
        }

        // Runs *before* each TEST_F
        void SetUp() override
        {
            RestructDB(true);
        }

        // Runs *after* each TEST_F
        void TearDown() override
        {
            // TODO: Deletes created files.
        }

        std::unordered_map<Tuple::Key, Tuple> SeedDB(size_t num_tuples)
        {
            std::vector<Tuple> tuples;
            std::unordered_map<Tuple::Key, Tuple> expected_map;

            std::mt19937_64 rng(42); // Fixed seed for reproducibility
            std::uniform_int_distribution<uint64_t> dist;

            // Generate random tuples with unique keys
            while (expected_map.size() < num_tuples)
            {
                Tuple::Key key = dist(rng);
                uint64_t value = dist(rng);

                if (expected_map.count(key) == 0)
                {
                    Tuple t{key, value};
                    tuples.push_back(t);
                    expected_map[key] = t;
                }
            }

            // Insert into the database
            db_->insert(tuples);

            return expected_map;
        }

        /// The database under test. When changing these parameters, also change the test's parameters,
        /// e.g. when testing in or out of memory behavior.
        std::unique_ptr<Database<TestIndex>> db_;
    };

    // A tuple inserted can be read again when all data fits in memory buffer.
    TEST_F(DatabaseTest, InMemory)
    {
        // Calculate the number of tuples that fit into the buffer.
        const size_t num_tuples = TEST_PAGE_SIZE * TEST_NUM_PAGES / sizeof(Tuple) / 2;

        auto expected_map = SeedDB(num_tuples);
        EXPECT_EQ(db_->size(), expected_map.size());

        // Verify correctness
        for (const auto &[key, expectedTuple] : expected_map)
        {
            Tuple actual = db_->get(key);
            EXPECT_EQ(expectedTuple, actual);
        }
    }

    // The same key can only be inserted once.
    TEST_F(DatabaseTest, DuplicateKeys)
    {
        EXPECT_NO_THROW(db_->insert({Tuple{0, 1}}));
        EXPECT_THROW(db_->insert({Tuple{0, 2}}), std::logic_error);
    }
    // A tuple can be inserted and read again, also if data exceeds buffer size.
    TEST_F(DatabaseTest, OutOfMemory)
    {
        // Calculate the number of tuples that overflow  the buffer.
        const size_t num_tuples = TEST_PAGE_SIZE * TEST_NUM_PAGES / sizeof(Tuple) * 2;

        auto expected_map = SeedDB(num_tuples);
        EXPECT_EQ(db_->size(), expected_map.size());

        // Verify correctness
        for (const auto &[key, expectedTuple] : expected_map)
        {
            Tuple actual = db_->get(key);
            EXPECT_EQ(expectedTuple, actual);
        }
    }
    // A tuple inserted can be read again, also after destroying the database.
    TEST_F(DatabaseTest, Persistency)
    {
        // Calculate the number of tuples that overflow  the buffer.
        const size_t num_tuples = TEST_PAGE_SIZE * TEST_NUM_PAGES / sizeof(Tuple) * 2;

        auto expected_map = SeedDB(num_tuples);
        EXPECT_EQ(db_->size(), expected_map.size());

        // Destroy db and create a new one.
        RestructDB(false);

        // Verify correctness
        for (const auto &[key, expectedTuple] : expected_map)
        {
            Tuple actual = db_->get(key);
            EXPECT_EQ(expectedTuple, actual);
        }
    }

}