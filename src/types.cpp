#include "bbbtree/types.h"

namespace bbbtree {

std::ostream &operator<<(std::ostream &os, const UInt64 &type) {
	os << type.value << std::endl;
	return os;
}
std::ostream &operator<<(std::ostream &os, const String &type) {
	os << type.view << std::endl;
	return os;
}
} // namespace bbbtree