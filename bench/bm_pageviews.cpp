#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/database.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"
#include "helpers.h"
// -----------------------------------------------------------------
#include <benchmark/benchmark.h>
// -----------------------------------------------------------------
using namespace bbbtree;
// -----------------------------------------------------------------
namespace {
// -----------------------------------------------------------------
using KeyT = UInt64;
using BBBTreeDB = Database<BBBTree, KeyT>;
using BTreeDB = Database<BTree, KeyT>;
using BTreeIndex = BTree<KeyT, TID>;
using BBBTreeIndex = BBBTree<KeyT, TID>;
// -----------------------------------------------------------------
static const constexpr size_t BENCH_PAGE_SIZE = 4096;
static const constexpr size_t BENCH_NUM_PAGES = 100;
static const constexpr size_t BENCH_WA_THRESHOLD = 5;
static const constexpr SegmentID BENCH_SEGMENT_ID = 2;
// -----------------------------------------------------------------
template <typename DatabaseUnderTest>
static void BM_PageViews_Insert_DB(benchmark::State &state) {
	stats.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static const std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto _ : state) {
		for (auto key : keys) {
			typename DatabaseUnderTest::Tuple tuple = {key,
													   0}; // Value is dummy
			db.insert(tuple);							   // Value is dummy
			stats.bytes_written_logically += tuple.size();
		}
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
	static std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	static std::vector<Operation> ops =
		LoadPageviewOps("operations_en_sample_1.csv");

	// Clear buffer manager to force write-backs.
	db.clear_bm(true);
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
	static std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	static std::vector<Operation> ops =
		LoadPageviewOps("operations_en_sample_1.csv");

	// Clear buffer manager to force write-backs.
	db.clear_bm(true);
	stats.clear();

	for (auto _ : state) {
		for (const auto &op : ops) {
			switch (op.op_type) {
			case 'L': {
				benchmark::DoNotOptimize(db.get(op.row_number));
				break;
			}
			case 'U': {
				typename DatabaseUnderTest::Tuple tuple = {op.row_number,
														   0}; // Value is dummy
				db.update(tuple);
				stats.bytes_written_logically += tuple.size();
				break;
			}
			default:
				throw std::logic_error("Unknown operation type in workload.");
			}
		}
	}

	db.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename IndexUnderTest>
static void BM_PageViews_Mixed_Index(benchmark::State &state) {
	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};

	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto key : keys) {
		auto success = index.insert(key, 0); // Value is dummy
		assert(success);
	}

	// Get the workload
	static std::vector<Operation> ops =
		LoadPageviewOps("operations_en_sample_1.csv");

	// Clear buffer manager to force write-backs.
	buffer_manager.clear_all(true);
	stats.clear();

	for (auto _ : state) {
		for (const auto &op : ops) {
			switch (op.op_type) {
			case 'L':
				benchmark::DoNotOptimize(index.lookup(op.row_number));
				break;
			case 'U': {
				TID value = 0; // Value is dummy
				KeyT key = op.row_number;

				index.update(key, value);
				stats.bytes_written_logically += key.size() + value.size();
				break;
			}
			default:
				throw std::logic_error("Unknown operation type in workload.");
			}
		}
	}

	index.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename IndexUnderTest>
static void BM_PageViews_Insert_Index(benchmark::State &state) {
	stats.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};

	// Propagate the database with pageview keys
	static const std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto _ : state) {
		for (auto key : keys) {
			TID value = 0; // Value is dummy
			KeyT k = key;
			auto success = index.insert(k, value);
			stats.bytes_written_logically += k.size() + value.size();
			assert(success);
		}
	}

	index.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename IndexUnderTest>
static void BM_PageViews_Lookup_Index(benchmark::State &state) {
	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	uint16_t wa_threshold = state.range(2);

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};

	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys =
		LoadPageviewKeys("pageviews_en_sample_1.csv");

	for (auto key : keys) {
		auto success = index.insert(key, 0); // Value is dummy
		assert(success);
	}

	// Get the workload
	static std::vector<Operation> ops =
		LoadPageviewOps("operations_en_sample_1.csv");

	// Clear buffer manager to force write-backs.
	buffer_manager.clear_all(true);
	stats.clear();

	for (auto _ : state) {
		for (const auto &op : ops) {
			benchmark::DoNotOptimize(index.lookup(op.row_number));
		}
	}

	index.set_height();
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
BENCHMARK_TEMPLATE(BM_PageViews_Insert_DB, BTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Insert_DB, BBBTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_DB, BTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_DB, BBBTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_DB, BTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_DB, BBBTreeDB)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_Index, BTreeIndex)
	->Args({20, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({50, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({100, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({200, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_Index, BBBTreeIndex)
	->Args({20, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({50, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({100, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({200, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
// BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index, BTreeIndex)
// 	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
// 	->Iterations(1)
// 	->Repetitions(1);
// BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index, BBBTreeIndex)
// 	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
// 	->Iterations(1)
// 	->Repetitions(1);
// // -----------------------------------------------------------------
// BENCHMARK_TEMPLATE(BM_PageViews_Lookup_Index, BTreeIndex)
// 	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
// 	->Iterations(1)
// 	->Repetitions(1);
// BENCHMARK_TEMPLATE(BM_PageViews_Lookup_Index, BBBTreeIndex)
// 	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
// 	->Iterations(1)
// 	->Repetitions(1);
// // -----------------------------------------------------------------