#pragma once
// -----------------------------------------------------------------
#include "bbbtree/stats.h"
// -----------------------------------------------------------------
#include <benchmark/benchmark.h>
#include <string>
#include <vector>
// -----------------------------------------------------------------
using namespace bbbtree;
// -----------------------------------------------------------------
struct Operation {
	uint64_t row_number;
	std::string page_title;
	char op_type; // 'L' for lookup, 'U' for update
};
// -----------------------------------------------------------------
std::vector<uint64_t> LoadPageviewKeys(const std::string &filename);
std::vector<std::string> LoadPageviewKeysAsStrings(const std::string &filename);
std::vector<Operation> LoadPageviewOps(const std::string &filename);
void SetBenchmarkCounters(benchmark::State &state, const Stats &stats);
// -----------------------------------------------------------------
