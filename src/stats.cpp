#include "bbbtree/stats.h"
namespace bbbtree {
Stats stats{};

std::ostream &operator<<(std::ostream &os, const Stats &stats) {
	os << "inner_node_splits: " << stats.inner_node_splits << std::endl;
	os << "leaf_node_splits: " << stats.leaf_node_splits << std::endl;
	return os;
}
} // namespace bbbtree