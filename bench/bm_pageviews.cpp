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
using BBBTreeIndexed = Database<BBBTree, KeyT>;
using BTreeIndexed = Database<BTree, KeyT>;
// -----------------------------------------------------------------
static const constexpr size_t BENCH_PAGE_SIZE = 4096;
static const constexpr size_t BENCH_NUM_PAGES = 100;
static const constexpr size_t BENCH_WA_THRESHOLD = 5;
// -----------------------------------------------------------------
template <typename DatabaseUnderTest>
static void BM_PageViews_Insert_DB(benchmark::State &state) {
	stats.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static const std::string filename = "pageviews_en_sample_1.csv";
	static std::vector<uint64_t> keys = LoadPageviewKeys(filename);

	for (auto _ : state) {
		for (auto key : keys)
			db.insert({key, 0}); // Value is dummy
	}

	db.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename DatabaseUnderTest>
static void BM_PageViews_Lookup_DB(benchmark::State &state) {

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	std::string filename = "pageviews_en_sample_1.csv";
	static std::vector<uint64_t> keys = LoadPageviewKeys(filename);

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	filename = "operations_en_sample_1.csv";
	static std::vector<Operation> ops = LoadPageviewOps(filename);
	stats.clear();

	for (auto _ : state) {
		for (const auto &op : ops) {
			benchmark::DoNotOptimize(db.get(op.row_number));
		}
	}

	db.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename DatabaseUnderTest>
static void BM_PageViews_Mixed_DB(benchmark::State &state) {
	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	std::string filename = "pageviews_en_sample_1.csv";
	static std::vector<uint64_t> keys = LoadPageviewKeys(filename);

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	filename = "operations_en_sample_1.csv";
	static std::vector<Operation> ops = LoadPageviewOps(filename);
	stats.clear();

	for (auto _ : state) {
		for (const auto &op : ops) {
			if (op.op_type == 'L') {
				benchmark::DoNotOptimize(db.get(op.row_number));
			} else if (op.op_type == 'U') {
				db.update({op.row_number, 0});
			}
		}
	}

	db.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
} // namespace
// -----------------------------------------------------------------
// 0: Number of pages in memory
// 1: Page Size
// 2: Write Amplification Threshold
// -----------------------------------------------------------------
void CustomArgs(benchmark::internal::Benchmark *b) {
	for (int num_pages : {1, 2, 3}) {
		for (int page_size : {10, 20}) {
			for (int wa_threshold : {5, 10}) {
				b->Args({num_pages, page_size, wa_threshold});
			}
		}
	}
}
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Insert_DB, BTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Insert_DB, BBBTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_DB, BTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_DB, BBBTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_DB, BTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_DB, BBBTreeIndexed)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------