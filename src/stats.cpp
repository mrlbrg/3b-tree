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

	return os;
}

} // namespace bbbtree