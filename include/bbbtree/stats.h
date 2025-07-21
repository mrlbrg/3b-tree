#pragma once

#include <cstdint>
#include <iostream>

namespace bbbtree {
struct Stats;

std::ostream &operator<<(std::ostream &os, const Stats &stats);

struct Stats {
	uint64_t inner_node_splits = 0;
	uint64_t leaf_node_splits = 0;
};

extern Stats stats;
} // namespace bbbtree