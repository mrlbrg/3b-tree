#include "bbbtree/types.h"

namespace bbbtree {

std::ostream &operator<<(std::ostream &os, const UInt64 &type) {
	os << type.value;
	return os;
}
std::ostream &operator<<(std::ostream &os, const TID &type) {
	os << type.value;
	return os;
}
std::ostream &operator<<(std::ostream &os, const String &type) {
	os << type.view.substr(0, 5);
	return os;
}
} // namespace bbbtree