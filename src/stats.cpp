#include "bbbtree/stats.h"
#include <cmath>
// -----------------------------------------------------------------
namespace bbbtree {
// -----------------------------------------------------------------
Stats stats{};
// -----------------------------------------------------------------
void Stats::clear() {
	inner_node_splits = 0;
	leaf_node_splits = 0;
	bytes_written_logically = 0;
	bytes_written_physically = 0;
	pages_evicted = 0;
	pages_written = 0;
	pages_write_deferred = 0;
	b_tree_height = 0;
	delta_tree_height = 0;
	pages_created = 0;
	slotted_pages_created = 0;
	pages_loaded = 0;
	num_insertions_db = 0;
	num_insertions_index = 0;
	num_deletions_index = 0;
	buffer_hits = 0;
	buffer_misses = 0;
	num_updates_db = 0;
	num_lookups_db = 0;
	num_lookups_index = 0;
	num_updates_index = 0;
	num_deletions_db = 0;
}
// -----------------------------------------------------------------
std::unordered_map<std::string, size_t> Stats::get_stats() const {
	return {{"inner_node_splits", inner_node_splits},
			{"leaf_node_splits", leaf_node_splits},
			{"node_splits", inner_node_splits + leaf_node_splits},
			{"bytes_written_logically", bytes_written_logically},
			{"bytes_written_physically", bytes_written_physically},
			{"write_amplification",
			 bytes_written_logically == 0
				 ? 0
				 : std::round(static_cast<double>(bytes_written_physically) /
							  bytes_written_logically)},
			{"pages_evicted", pages_evicted},
			{"pages_written", pages_written},
			{"pages_write_deferred", pages_write_deferred},
			{"b_tree_height", b_tree_height},
			{"delta_tree_height", delta_tree_height},
			{"pages_created", pages_created},
			{"slotted_pages_created", slotted_pages_created},
			{"pages_loaded", pages_loaded},
			{"wa_threshold", wa_threshold},
			{"page_size", page_size},
			{"num_pages", num_pages},
			{"num_insertions_db", num_insertions_db},
			{"num_insertions_index", num_insertions_index},
			{"num_deletions_index", num_deletions_index},
			{"buffer_accesses", buffer_hits + buffer_misses},
			{"buffer_hits", std::round(static_cast<double>(buffer_hits) /
									   (buffer_hits + buffer_misses) * 100)},
			{"buffer_misses", std::round(static_cast<double>(buffer_misses) /
										 (buffer_hits + buffer_misses) * 100)},
			{"num_updates_db", num_updates_db},
			{"num_lookups_db", num_lookups_db},
			{"num_lookups_index", num_lookups_index},
			{"num_updates_index", num_updates_index},
			{"num_deletions_db", num_deletions_db}};
}
// -----------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const Stats &stats) {
	for (const auto &[key, value] : stats.get_stats())
		os << key << ": " << value << std::endl;

	return os;
}
// -----------------------------------------------------------------
} // namespace bbbtree
// -----------------------------------------------------------------