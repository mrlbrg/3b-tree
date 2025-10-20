#include "bbbtree/bbbtree.h"
#include "bbbtree/btree.h"
#include "bbbtree/btree_with_tracking.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/database.h"
#include "bbbtree/logger.h"
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
using VarKeyT = String;
using BBBTreeDB = Database<BBBTree, KeyT>;
using BTreeDB = Database<BTree, KeyT>;
using BTreeIndex = BTree<KeyT, TID>;
using BTreeIndexVar = BTree<VarKeyT, TID>;
using BTreeWithTrackingIndex = BTreeWithTracking<KeyT, TID>;
using BBBTreeIndex = BBBTree<KeyT, TID>;
using BBBTreeIndexVar = BBBTree<VarKeyT, TID>;
// -----------------------------------------------------------------
static const constexpr size_t BENCH_PAGE_SIZE = 4096;
static const constexpr size_t BENCH_NUM_PAGES = 400;
static const constexpr size_t BENCH_WA_THRESHOLD = 5;
static const constexpr size_t BENCH_UPDATE_RATIO = 5;
static const constexpr size_t BENCH_SAMPLE_SIZE = 5;
static const constexpr SegmentID BENCH_SEGMENT_ID = 2;
// The file with the distinct pageview keys.
static const constexpr auto PAGES_FILE = "pageviews_en_sample_5.csv";
// The file with the workload on the pageview keys.
// filenames: `operations_en_sample_{sample_ratio}_{update_ratio}.csv`
// `sample_ratio` is the fraction of all pageview rows from the original
// dataset
// `update_ratio` is the percentage of lookups that we turned into
// updates
static const constexpr auto OPERATIONS_FILE = "operations_en_sample_5_5.csv";
// -----------------------------------------------------------------
/// Generate the operations filename based on the update ratio.
std::string
update_ratio_to_ops_filename(size_t update_ratio = BENCH_UPDATE_RATIO) {
	return "operations_en_sample_" + std::to_string(BENCH_SAMPLE_SIZE) + "_" +
		   std::to_string(update_ratio) + ".csv";
}
// -----------------------------------------------------------------
template <typename DatabaseUnderTest>
static void BM_PageViews_Insert_DB(benchmark::State &state) {
	stats.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static const std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

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
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;
	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	static std::vector<Operation> ops = LoadPageviewOps(OPERATIONS_FILE);

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
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;

	DatabaseUnderTest db{page_size, num_pages, wa_threshold, true};

	// TODO: Disable buffering until the after the insertions.

	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

	for (auto key : keys)
		db.insert({key, 0}); // Value is dummy

	// Propagate the database with pageview keys
	static std::vector<Operation> ops = LoadPageviewOps(OPERATIONS_FILE);

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
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;
	size_t update_ratio = state.range(3);
	auto ops_filename = update_ratio_to_ops_filename(update_ratio);

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};
	index.disable_buffering();
	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

	for (auto key : keys) {
		auto success = index.insert(key, 0); // Value is dummy
		assert(success);
	}

	// Get the workload
	std::vector<Operation> ops = LoadPageviewOps(ops_filename);

	// Clear buffer manager to force write-backs.
	buffer_manager.clear_all(true);
	stats.clear();
	logger.clear();
	index.enable_buffering();

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
	logger.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};

	// Propagate the database with pageview keys
	static const std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

	for (auto _ : state) {
		state.PauseTiming();
		stats.clear();
		buffer_manager.clear_all(false);
		index.clear();
		state.ResumeTiming();
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
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};
	index.disable_buffering();

	// Propagate the database with pageview keys
	static std::vector<uint64_t> keys = LoadPageviewKeys(PAGES_FILE);

	for (auto key : keys) {
		auto success = index.insert(key, 0); // Value is dummy
		assert(success);
	}

	// Get the workload
	static std::vector<Operation> ops = LoadPageviewOps(OPERATIONS_FILE);

	// Clear buffer manager to force write-backs.
	buffer_manager.clear_all(true);
	stats.clear();
	index.enable_buffering();

	for (auto _ : state) {
		for (const auto &op : ops) {
			benchmark::DoNotOptimize(index.lookup(op.row_number));
		}
	}

	index.set_height();
	SetBenchmarkCounters(state, stats);
}
// -----------------------------------------------------------------
template <typename IndexUnderTest>
static void BM_PageViews_Insert_Index_Var(benchmark::State &state) {
	stats.clear();
	logger.clear();

	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};

	// Propagate the database with pageview keys
	static const std::vector<std::string> keys =
		LoadPageviewKeysAsStrings(PAGES_FILE);

	// Insert as many bytes as the integer version
	for (auto _ : state) {
		state.PauseTiming();
		stats.clear();
		buffer_manager.clear_all(false);
		index.clear();
		state.ResumeTiming();
		for (auto &key : keys) {
			TID value = 0; // Value is dummy
			VarKeyT k{key};
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
static void BM_PageViews_Mixed_Index_Var(benchmark::State &state) {
	size_t num_pages = state.range(0);
	uint16_t page_size = state.range(1);
	float wa_threshold = static_cast<float>(state.range(2)) / 100.0;
	size_t update_ratio = state.range(3);
	auto ops_filename = update_ratio_to_ops_filename(update_ratio);

	BufferManager buffer_manager{page_size, num_pages, true};
	IndexUnderTest index{BENCH_SEGMENT_ID, buffer_manager, wa_threshold};
	index.disable_buffering();
	// Propagate the database with pageview keys
	static std::vector<std::string> keys =
		LoadPageviewKeysAsStrings(PAGES_FILE);

	for (auto &key : keys) {
		auto success = index.insert(VarKeyT{key}, 0); // Value is dummy
		assert(success);
	}

	// Get the workload
	std::vector<Operation> ops = LoadPageviewOps(ops_filename);

	// Clear buffer manager to force write-backs.
	buffer_manager.clear_all(true);
	stats.clear();
	logger.clear();
	index.enable_buffering();

	for (auto _ : state) {
		for (const auto &op : ops) {
			switch (op.op_type) {
			case 'L':
				benchmark::DoNotOptimize(index.lookup(VarKeyT{op.page_title}));
				break;
			case 'U': {
				TID value = 0; // Value is dummy
				VarKeyT key{op.page_title};

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
} // namespace
// -----------------------------------------------------------------
// 0: Number of pages in memory
// 1: Page Size
// 2: Write Amplification Threshold
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
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD, 5})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_Index, BBBTreeIndex)
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD, 5})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index, BTreeIndex)
	->Args({200, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index, BBBTreeIndex)
	->Args({200, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_Index, BTreeIndex)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Lookup_Index, BBBTreeIndex)
	->Args({BENCH_NUM_PAGES, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index_Var, BTreeIndexVar)
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({300, BENCH_PAGE_SIZE, 7})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Insert_Index_Var, BBBTreeIndexVar)
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD})
	->Args({300, BENCH_PAGE_SIZE, 7})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_Index_Var, BTreeIndexVar)
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD, 5})
	->Iterations(1)
	->Repetitions(1);
BENCHMARK_TEMPLATE(BM_PageViews_Mixed_Index_Var, BBBTreeIndexVar)
	->Args({300, BENCH_PAGE_SIZE, BENCH_WA_THRESHOLD, 5})
	->Iterations(1)
	->Repetitions(1);
// -----------------------------------------------------------------