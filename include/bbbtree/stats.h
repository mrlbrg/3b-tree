#pragma once

#include <iostream>

namespace bbbtree {
struct Stats;

std::ostream &operator<<(std::ostream &os, const Stats &stats);

struct Stats {
	size_t inner_node_splits = 0;
	size_t leaf_node_splits = 0;

	size_t bytes_written_logically = 0;
	size_t bytes_written_physically = 0;

	size_t pages_swapped = 0;
	size_t pages_written = 0;

	~Stats() { std::cout << *this << std::endl; }

	void clear();
};

extern Stats stats;
} // namespace bbbtree