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

	// Counts every time a page is removed from the buffer. May have been
	// written to disk or not. E.g. a clean page is removed but not written or a
	// dirty page whose writes are deferred.
	size_t pages_evicted = 0;
	// Counts every time a page is written to disk.
	size_t pages_written = 0;
	// TODO: add a counter every time a dirty page's write out is deferred.

	~Stats() { std::cout << *this << std::endl; }

	void clear();
};

extern Stats stats;
} // namespace bbbtree