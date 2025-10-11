#pragma once

#include <cstddef>
#include <iostream>
#include <unordered_map>

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
	// The number of bytes that were actually changed physically in storage.
	size_t bytes_written_physically = 0;

	// Counts every time a new page is created in the system.
	size_t pages_created = 0;
	// Counts every time a slotted page is created in the system.
	size_t slotted_pages_created = 0;
	// Counts every time a page is loaded from disk.
	size_t pages_loaded = 0;
	// Counts every time a page is removed from the buffer. May have been
	// written to disk or not. E.g. a clean page is removed but not written or a
	// dirty page whose writes are deferred.
	size_t pages_evicted = 0;
	// Counts every time a page is written to disk.
	size_t pages_written = 0;
	// Counts the number of times a page's changed were extracted and buffered
	// in-memory instead of written to disk.
	size_t pages_write_deferred = 0;

	// Counts the number of buffer hits.
	size_t buffer_hits = 0;
	// Counts the number of buffer misses.
	size_t buffer_misses = 0;

	// Tracks the maximum height of the B-Tree.
	size_t b_tree_height = 0;
	// Tracks the maximum height of the Delta Tree.
	size_t delta_tree_height = 0;

	// Threshold to trigger write-out of a dirty page.
	size_t wa_threshold = 0;
	// The pages' size in bytes.
	size_t page_size = 0;
	// The number of pages in the buffer pool.
	size_t num_pages = 0;

	// The number of insertions performed on the database.
	size_t num_insertions_db = 0;
	// The number of updates performed on the database.
	size_t num_updates_db = 0;
	// The number of lookups performed on the database.
	size_t num_lookups_db = 0;
	// The number of deletions performed on the database.
	size_t num_deletions_db = 0;
	// The number of insertions performed on the index.
	size_t num_insertions_index = 0;
	// The number of deletions performed on the index.
	size_t num_deletions_index = 0;
	// The number of lookups performed on the index.
	size_t num_lookups_index = 0;
	// The number of updates performed on the index.
	size_t num_updates_index = 0;

	// Resets all stats to zero.
	void clear();
	// Returns map of all stats.
	std::unordered_map<std::string, size_t> get_stats() const;
};

extern Stats stats;
} // namespace bbbtree