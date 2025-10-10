#include "bbbtree/stats.h"

namespace bbbtree {

Stats stats{};

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
	wa_threshold = 0;
	page_size = 0;
	num_pages = 0;
	num_insertions_db = 0;
	num_insertions_index = 0;
	num_deletions_index = 0;
}

std::unordered_map<std::string, size_t> Stats::get_stats() const {
	return {
		{"inner_node_splits", inner_node_splits},
		{"leaf_node_splits", leaf_node_splits},
		{"bytes_written_logically", bytes_written_logically},
		{"bytes_written_physically", bytes_written_physically},
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
	};
}

std::ostream &operator<<(std::ostream &os, const Stats &stats) {
	os << "inner_node_splits: " << stats.inner_node_splits << std::endl;
	os << "leaf_node_splits: " << stats.leaf_node_splits << std::endl;
	os << "bytes_written_logically: " << stats.bytes_written_logically
	   << std::endl;
	os << "bytes_written_physically: " << stats.bytes_written_physically
	   << std::endl;
	os << "pages_evicted: " << stats.pages_evicted << std::endl;
	os << "pages_written: " << stats.pages_written << std::endl;
	os << "pages_write_deferred: " << stats.pages_write_deferred << std::endl;
	os << "b_tree_height: " << stats.b_tree_height << std::endl;
	os << "delta_tree_height: " << stats.delta_tree_height << std::endl;
	os << "pages_created: " << stats.pages_created << std::endl;
	os << "slotted_pages_created: " << stats.slotted_pages_created << std::endl;
	os << "pages_loaded: " << stats.pages_loaded << std::endl;
	os << "wa_threshold: " << stats.wa_threshold << std::endl;
	os << "page_size: " << stats.page_size << std::endl;
	os << "num_pages: " << stats.num_pages << std::endl;
	os << "num_insertions_db: " << stats.num_insertions_db << std::endl;
	os << "num_insertions_index: " << stats.num_insertions_index << std::endl;
	os << "num_deletions_index: " << stats.num_deletions_index << std::endl;

	return os;
}

} // namespace bbbtree