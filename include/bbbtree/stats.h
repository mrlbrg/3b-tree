#pragma once

#include <cstdint>
#include <iostream>

namespace bbbtree {
struct Stats;

std::ostream &operator<<(std::ostream &os, const Stats &stats);

struct Stats {
	uint64_t inner_node_splits = 0;
	uint64_t leaf_node_splits = 0;

	uint64_t bytes_written_logically = 0;
	uint64_t bytes_written_physically = 0;

	~Stats() {
		std::cout << "inner_node_splits: " << inner_node_splits << std::endl;
		std::cout << "leaf_node_splits: " << leaf_node_splits << std::endl;
		std::cout << "bytes_written_logically: " << bytes_written_logically
				  << std::endl;
		std::cout << "bytes_written_physically: " << bytes_written_physically
				  << std::endl;
	}
};

extern Stats stats;
} // namespace bbbtree