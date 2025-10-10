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
// Helper: Load the first column (row_number) from a CSV file
std::vector<uint64_t> load_pageview_keys(const std::string &filename) {
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
