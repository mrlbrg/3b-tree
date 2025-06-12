#include "bbbtree/database.h"

#include <gtest/gtest.h>
#include <vector>
#include <unordered_map>
#include <random>
#include <cstdint>
#include <algorithm>

using namespace bbbtree;

namespace
{
    // A tuple inserted can be read again when all data fits in memory buffer.
    TEST(Database, InMemory)
    {
        const size_t numTuples = 200;
        std::vector<Tuple> tuples;
        std::unordered_map<Tuple::Key, Tuple> expectedMap;

        std::mt19937_64 rng(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<uint64_t> dist;

        // Generate random tuples with unique keys
        while (expectedMap.size() < numTuples)
        {
            Tuple::Key key = dist(rng);
            uint64_t value = dist(rng);

            if (expectedMap.count(key) == 0)
            {
                Tuple t{key, value};
                tuples.push_back(t);
                expectedMap[key] = t;
            }
        }

        // Convert to vector<const Tuple>
        std::vector<Tuple> constTuples;
        for (auto &t : tuples)
        {
            constTuples.push_back(t);
        }

        // Insert into the database
        Database db;
        db.insert(constTuples);

        // Verify correctness
        for (const auto &[key, expectedTuple] : expectedMap)
        {
            Tuple actual = db.get(key);
            EXPECT_EQ(expectedTuple, actual);
        }
    }
    // A tuple can be inserted and read again, also if data exceeds buffer size.
    TEST(Database, OutOfMemory)
    {
    }
    // A tuple inserted can be read again, also after destroying the database.
    TEST(Database, Persistency)
    {
    }

}