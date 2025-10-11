#include "helpers.h"
// -----------------------------------------------------------------
#include <benchmark/benchmark.h>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
// -----------------------------------------------------------------
namespace fs = std::filesystem;
// -----------------------------------------------------------------
fs::path get_data_file(const std::string &name) {
#ifndef PROJECT_SOURCE_DIR
#error "PROJECT_SOURCE_DIR not defined!"
#endif

	fs::path project_root = PROJECT_SOURCE_DIR; // passed by CMake
	return project_root / "data" / name;
}
// -----------------------------------------------------------------
// Load the first column (row_number) from a CSV file of the form
// `<row_number,project,page_title,views,bytes>`
std::vector<uint64_t> LoadPageviewKeys(const std::string &filename) {
	std::vector<uint64_t> row_numbers;
	auto path = get_data_file(filename);
	std::ifstream file(path);
	if (!file.is_open()) {
		throw std::runtime_error("Failed to open file: " + path.string());
	}

	std::string line;
	// --- Skip header row ---
	if (!std::getline(file, line)) {
		throw std::runtime_error("CSV file is empty: " + filename);
	}

	while (std::getline(file, line)) {
		if (line.empty())
			continue;

		std::stringstream ss(line);
		std::string cell;

		// Read first field before the first comma
		if (std::getline(ss, cell, ',')) {
			try {
				uint64_t num = std::stoull(cell);
				row_numbers.push_back(num);
			} catch (const std::invalid_argument &) {
				// Skip header or invalid rows
				throw std::runtime_error("Invalid row: " + line);
			}
		}
	}
	return row_numbers;
}
// -----------------------------------------------------------------
void SetBenchmarkCounters(benchmark::State &state, const Stats &stats) {
	for (const auto &[key, value] : stats.get_stats())
		state.counters[key] = value;
}
// -----------------------------------------------------------------
// Loads a CSV file with columns: row_number,page_title,op_type
std::vector<Operation> LoadPageviewOps(const std::string &filename) {
	std::vector<Operation> ops;
	auto path = get_data_file(filename);
	std::ifstream file(path);
	if (!file.is_open()) {
		throw std::runtime_error("Failed to open file: " + path.string());
	}

	std::string line;

	// --- Skip header ---
	if (!std::getline(file, line)) {
		throw std::runtime_error("CSV file is empty: " + filename);
	}

	// --- Parse remaining lines ---
	while (std::getline(file, line)) {
		if (line.empty())
			continue;

		std::stringstream ss(line);
		std::string cell;

		// 1️⃣ row_number
		if (!std::getline(ss, cell, ','))
			continue;
		uint64_t row_number;
		try {
			row_number = std::stoull(cell);
		} catch (...) {
			continue; // skip invalid
		}

		// 2️⃣ page_title
		std::string page_title;
		if (!std::getline(ss, page_title, ','))
			continue;

		// 3️⃣ op_type
		if (!std::getline(ss, cell, ','))
			continue;

		char op_char;
		if (cell == "lookup") {
			op_char = 'L';
		} else if (cell == "update") {
			op_char = 'U';
		} else {
			continue; // skip invalid operation types
		}

		ops.push_back({row_number, page_title, op_char});
	}

	return ops;
}
// -----------------------------------------------------------------
