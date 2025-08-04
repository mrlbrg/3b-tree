#include "bbbtree/btree.h"
#include "bbbtree/database.h"
#include "bbbtree/map.h"
#include "bbbtree/types.h"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

using namespace bbbtree;

namespace {
using KeyT = UInt64;
using OutOfMemoryDatabase = Database<BTree, KeyT>;
using InMemoryDatabase = Database<Map, KeyT>;

static const constexpr size_t BENCH_PAGE_SIZE = 1024;
static const constexpr size_t BENCH_NUM_PAGES = 10;

template <typename TestDatabase>
std::vector<typename TestDatabase::Tuple> GetTuples(size_t num_tuples) {
	std::vector<typename TestDatabase::Tuple> keys;
	std::unordered_map<KeyT, uint8_t> unique_keys;

	std::mt19937_64 rng(42); // Fixed seed for reproducibility
	std::uniform_int_distribution<uint64_t> dist;

	// Generate random unique keys
	while (unique_keys.size() < num_tuples) {
		KeyT key = dist(rng);
		ValueT value = dist(rng);

		if (unique_keys.count(key) == 0) {
			keys.push_back({key, value});
			unique_keys[key] = 0;
		}
	}

	return keys;
}

static void BM_InMemoryRandomWrite(benchmark::State &state) {
	using DatabaseUnderTest = InMemoryDatabase;

	auto tuples = GetTuples<DatabaseUnderTest>(state.range(0));
	for (auto _ : state) {
		state.PauseTiming();
		DatabaseUnderTest db{BENCH_PAGE_SIZE, BENCH_NUM_PAGES, true};
		state.ResumeTiming();

		db.insert(tuples);
	}
}

static void BM_OutOfMemoryRandomWrite(benchmark::State &state) {
	using DatabaseUnderTest = OutOfMemoryDatabase;

	auto tuples = GetTuples<DatabaseUnderTest>(state.range(0));
	for (auto _ : state) {
		state.PauseTiming();
		DatabaseUnderTest db{BENCH_PAGE_SIZE, BENCH_NUM_PAGES, true};
		state.ResumeTiming();

		db.insert(tuples);
	}
}

static void BM_OutOfMemorySequentialWrite(benchmark::State &state) {
	using DatabaseUnderTest = OutOfMemoryDatabase;

	auto tuples = GetTuples<DatabaseUnderTest>(state.range(0));
	std::sort(tuples.begin(), tuples.end());

	for (auto _ : state) {
		state.PauseTiming();
		DatabaseUnderTest db{BENCH_PAGE_SIZE, BENCH_NUM_PAGES, true};
		state.ResumeTiming();

		db.insert(tuples);
	}
}

static void BM_OutOfMemoryRandomLookup(benchmark::State &state) {
	using DatabaseUnderTest = OutOfMemoryDatabase;

	DatabaseUnderTest db{BENCH_PAGE_SIZE, BENCH_NUM_PAGES, true};
	auto tuples = GetTuples<DatabaseUnderTest>(state.range(0));
	db.insert(tuples);

	auto rng = std::default_random_engine{};
	std::ranges::shuffle(tuples, rng);

	for (auto _ : state) {
		for (auto &tuple : tuples) {
			db.get(tuple.key);
		}
	}
}
static void BM_OutOfMemorySequentialLookup(benchmark::State &state) {
	using DatabaseUnderTest = OutOfMemoryDatabase;

	DatabaseUnderTest db{BENCH_PAGE_SIZE, BENCH_NUM_PAGES, true};
	auto tuples = GetTuples<DatabaseUnderTest>(state.range(0));
	db.insert(tuples);

	auto rng = std::default_random_engine{};
	std::sort(tuples.begin(), tuples.end());

	for (auto _ : state) {
		for (auto &tuple : tuples) {
			db.get(tuple.key);
		}
	}
}
} // namespace

BENCHMARK(BM_InMemoryRandomWrite)->Arg(1000);
BENCHMARK(BM_OutOfMemoryRandomWrite)->Arg(1000);
BENCHMARK(BM_OutOfMemorySequentialWrite)->Arg(1000);
BENCHMARK(BM_OutOfMemoryRandomLookup)->Arg(1000);
BENCHMARK(BM_OutOfMemorySequentialLookup)->Arg(1000);