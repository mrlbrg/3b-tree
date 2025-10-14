#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <benchmark/benchmark.h>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>
// -----------------------------------------------------------------
using namespace bbbtree;
// -----------------------------------------------------------------
/// Benchmark for measuring insert performance of BBBTree vs BTree in an empty
/// database.
namespace {
// -----------------------------------------------------------------
using KeyT = UInt64;
using ValueT = TID;
using BBBTreeIndex = BBBTree<KeyT, ValueT>;
using BTreeIndex = BTree<KeyT, ValueT>;
// -----------------------------------------------------------------
static const constexpr size_t BENCH_PAGE_SIZE = 4096;
static const constexpr size_t BENCH_NUM_PAGES = 500;
// -----------------------------------------------------------------
std::vector<std::pair<KeyT, ValueT>> GetTuples(size_t num_tuples) {
	std::vector<std::pair<KeyT, ValueT>> tuples;
	std::unordered_map<KeyT, uint8_t> unique_keys;

	std::mt19937_64 rng(42); // Fixed seed for reproducibility
	std::uniform_int_distribution<uint64_t> dist;

	// Generate random unique keys
	while (unique_keys.size() < num_tuples) {
		KeyT key = dist(rng);
		ValueT value = dist(rng);

		if (unique_keys.count(key) == 0) {
			tuples.push_back({key, value});
			unique_keys[key] = 0;
		}
	}

	return tuples;
}
// -----------------------------------------------------------------
void SetBenchmarkCounters(benchmark::State &state, const Stats &stats) {
	size_t num_tuples = state.range(0);
	size_t num_pages = state.range(1);
	uint16_t wa_threshold = state.range(2);
	uint16_t page_size = state.range(3);

	state.counters["wa_threshold"] = wa_threshold;
	state.counters["page_size"] = page_size;
	state.counters["num_pages"] = num_pages;
	state.counters["num_tuples"] = num_tuples;

	state.counters["b_tree_height"] = stats.b_tree_height;
	state.counters["delta_tree_height"] = stats.delta_tree_height;
	state.counters["node_splits"] =
		stats.inner_node_splits + stats.leaf_node_splits;
	state.counters["bytes_written_logically"] = stats.bytes_written_logically;
	state.counters["bytes_written_physically"] = stats.bytes_written_physically;
	state.counters["pages_evicted"] = stats.pages_evicted;
	state.counters["pages_written"] = stats.pages_written;
	state.counters["pages_write_deferred"] = stats.pages_write_deferred;
	state.counters["pages_created"] = stats.pages_created;
	state.counters["slotted_pages_created"] = stats.slotted_pages_created;
}
// -----------------------------------------------------------------
static void BM_BTreeIndexFromScratch(benchmark::State &state) {
	size_t num_tuples = state.range(0);
	size_t num_pages = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;
	uint16_t page_size = state.range(3);

	BufferManager buffer_manager{page_size, num_pages, true};
	BTreeIndex index{2, buffer_manager, wa_threshold};

	auto tuples = GetTuples(num_tuples);
	for (auto _ : state) {
		stats.clear();

		for (auto &[key, value] : tuples)
			index.insert(key, value);

		state.PauseTiming();
		index.set_height();
		buffer_manager.clear_all(false);
		index.clear();
		state.ResumeTiming();
	}

	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
static void BM_BBBTreeIndexFromScratch(benchmark::State &state) {
	size_t num_tuples = state.range(0);
	size_t num_pages = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;
	uint16_t page_size = state.range(3);

	BufferManager buffer_manager{page_size, num_pages, true};
	BBBTreeIndex index{2, buffer_manager, wa_threshold};

	auto tuples = GetTuples(num_tuples);
	for (auto _ : state) {
		stats.clear();

		for (auto &[key, value] : tuples)
			index.insert(key, value);

		state.PauseTiming();
		index.set_height();
		buffer_manager.clear_all(false);
		index.clear();
		state.ResumeTiming();
	}

	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
} // namespace
// -----------------------------------------------------------------
// 0: Number of tuples
// 1: Number of pages in memory
// 2: Write Amplification Threshold
// 3: Page Size
// -----------------------------------------------------------------
// WA Threshold of 5
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
// BENCHMARK(BM_BTreeIndexFromScratch)
// 	->Args({1'000'000, BENCH_NUM_PAGES, 5,
// BENCH_PAGE_SIZE})->Iterations(1)->Repetitions(1);;
// BENCHMARK(BM_BBBTreeIndexFromScratch)
// 	->Args({1'000'000, BENCH_NUM_PAGES, 5,
// BENCH_PAGE_SIZE})->Iterations(1)->Repetitions(1);;
// -----------------------------------------------------------------
// WA Threshold of 10
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 10, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
// -----------------------------------------------------------------
// WA Threshold of 20
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 20, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
// -----------------------------------------------------------------
// WA Threshold of 50
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({1'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({10'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
BENCHMARK(BM_BBBTreeIndexFromScratch)
	->Args({100'000, BENCH_NUM_PAGES, 50, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
;
// -----------------------------------------------------------------
