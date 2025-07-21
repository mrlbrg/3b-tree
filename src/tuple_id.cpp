#include "bbbtree/tuple_id.h"

namespace bbbtree {
std::ostream &operator<<(std::ostream &os, const TID &tid) {
	os << tid.value << std::endl;
	return os;
}
} // namespace bbbtree