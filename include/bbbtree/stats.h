#pragma once

#include <iostream>

namespace bbbtree {
struct Stats;

std::ostream &operator<<(std::ostream &os, const Stats &stats);

struct Stats {
	// Counts the number of split inner nodes in a B-Tree.
	size_t inner_node_splits = 0;
	// Counts the number of split leaf node in a B-Tree.
	size_t leaf_node_splits = 0;

	// The number of bytes that were changed by a user logically.
	size_t bytes_written_logically = 0;
	// The number of bytes that were actually changed physically in memory.
	size_t bytes_written_physically = 0;

	// Counts every time a page is removed from the buffer. May have been
	// written to disk or not. E.g. a clean page is removed but not written or a
	// dirty page whose writes are deferred.
	size_t pages_evicted = 0;
	// Counts every time a page is written to disk.
	size_t pages_written = 0;
	// Counts the number of times a page's changed were extracted and buffered
	// in-memory instead of written to disk.
	size_t pages_write_deferred = 0;

	~Stats() { std::cout << *this << std::endl; }

	void clear();
	void print() { std::cout << *this << std::endl; }
};

extern Stats stats;
} // namespace bbbtree