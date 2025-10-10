#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/database.h"
#include "bbbtree/stats.h"
#include "helpers.h"
// -----------------------------------------------------------------
#include <benchmark/benchmark.h>
// -----------------------------------------------------------------
using namespace bbbtree;
// -----------------------------------------------------------------
namespace {
// -----------------------------------------------------------------
using KeyT = UInt64;
using BBBTreeIndexedDB = Database<BBBTree, KeyT>;
using BTreeIndexedDB = Database<BTree, KeyT>;
// -----------------------------------------------------------------
static const constexpr size_t BENCH_PAGE_SIZE = 4096;
static const constexpr size_t BENCH_NUM_PAGES = 100;
// -----------------------------------------------------------------
void SetBenchmarkCounters(benchmark::State &state, const Stats &stats) {
	size_t num_pages = state.range(0);
	uint16_t wa_threshold = state.range(1);
	uint16_t page_size = state.range(2);

	state.counters["wa_threshold"] = wa_threshold;
	state.counters["page_size"] = page_size;
	state.counters["num_pages"] = num_pages;

	for (const auto &[key, value] : stats.get_stats())
		state.counters[key] = value;
}
// -----------------------------------------------------------------
static void BM_PageViews_Insert_DB_BTree(benchmark::State &state) {
	stats.clear();
	using DatabaseUnderTest = BTreeIndexedDB;

	size_t num_pages = state.range(0);
	uint16_t wa_threshold = state.range(1);
	uint16_t page_size = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static const std::string filename = "pageviews_en_sample_1.csv";
	static std::vector<uint64_t> keys = load_pageview_keys(filename);

	for (auto _ : state) {
		for (auto key : keys)
			db.insert({key, 0}); // Value is dummy
	}

	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
static void BM_PageViews_Insert_DB_BBBTree(benchmark::State &state) {
	stats.clear();
	using DatabaseUnderTest = BBBTreeIndexedDB;

	size_t num_pages = state.range(0);
	uint16_t wa_threshold = state.range(1);
	uint16_t page_size = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static const std::string filename = "pageviews_en_sample_1.csv";
	static std::vector<uint64_t> keys = load_pageview_keys(filename);

	for (auto _ : state) {
		for (auto key : keys)
			db.insert({key, 0}); // Value is dummy
	}

	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
} // namespace
// -----------------------------------------------------------------
// 0: Number of pages in memory
// 1: Write Amplification Threshold
// 2: Page Size
// -----------------------------------------------------------------
BENCHMARK(BM_PageViews_Insert_DB_BTree)
	->Args({BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK(BM_PageViews_Insert_DB_BBBTree)
	->Args({BENCH_NUM_PAGES, 5, BENCH_PAGE_SIZE})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
// -----------------------------------------------------------------