#include "bbbtree/delta.h"
#include <cstring>

namespace bbbtree {
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
uint16_t Delta<KeyT, ValueT>::size() const {
	auto key_size = entry.key.size();
	auto value_size = entry.value.size();
	assert(key_size > 0 && value_size > 0);
	return sizeof(op) + sizeof(key_size) + key_size + sizeof(value_size) +
		   value_size;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void Delta<KeyT, ValueT>::serialize(std::byte *dst) const {
	// Serialize the operation type.
	size_t n = sizeof(op);
	std::memcpy(dst, &op, n);
	dst += n;
	// Serialize the key size.
	auto key_size = entry.key.size();
	std::memcpy(dst, &key_size, sizeof(key_size));
	dst += sizeof(key_size);
	// Serialize the value size.
	auto value_size = entry.value.size();
	std::memcpy(dst, &value_size, sizeof(value_size));
	dst += sizeof(value_size);
	// Serialize the key.
	entry.key.serialize(dst);
	dst += entry.key.size();
	// Serialize the value.
	entry.value.serialize(dst);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
Delta<KeyT, ValueT> Delta<KeyT, ValueT>::deserialize(const std::byte *src,
													 uint16_t n) {
	Delta delta;

	// Deserialize the operation type.
	std::memcpy(&delta.op, src, sizeof(delta.op));
	src += sizeof(delta.op);
	// Deserialize the key size.
	uint16_t key_size;
	std::memcpy(&key_size, src, sizeof(key_size));
	src += sizeof(key_size);
	assert(key_size > 0);
	// Deserialize the value size.
	uint16_t value_size;
	std::memcpy(&value_size, src, sizeof(value_size));
	src += sizeof(value_size);
	assert(value_size > 0);
	// Deserialize the key.
	delta.entry.key = KeyT::deserialize(src, key_size);
	src += key_size;
	// Deserialize the value.
	delta.entry.value = ValueT::deserialize(src, value_size);

	assert(n == delta.size());

	return delta;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Delta<KeyT, ValueT> &type) {
	switch (type.op) {
	case OperationType::Insert:
		os << "Insert";
		break;
	case OperationType::Update:
		os << "Update";
		break;
	case OperationType::Delete:
		os << "Delete";
		break;
	default:
		os << "Unknown";
		break;
	}

	os << ": [" << type.entry.key << ", " << type.entry.value << "]";

	return os;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void Deltas<KeyT, ValueT>::serialize(std::byte *dst) const {
	assert(deltas.size() <= std::numeric_limits<uint16_t>::max());
	uint16_t num_deltas = deltas.size();
	// Serialize the number of deltas.
	std::memcpy(dst, &num_deltas, sizeof(num_deltas));
	dst += sizeof(num_deltas);
	// Serialize the deltas.
	for (const auto &delta : deltas) {
		delta.serialize(dst);
		dst += delta.size();
	}
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
Deltas<KeyT, ValueT> Deltas<KeyT, ValueT>::deserialize(const std::byte *src,
													   uint16_t n) {
	Deltas<KeyT, ValueT> deltas;
	assert(n >= sizeof(uint16_t));
	// Deserialize the number of deltas.
	uint16_t num_deltas;
	std::memcpy(&num_deltas, src, sizeof(num_deltas));
	deltas.deltas.reserve(num_deltas);
	deltas.cached_size = sizeof(num_deltas);
	// Deserialize the deltas.
	for (uint16_t i = 0; i < num_deltas; i++) {
		auto delta =
			Delta<KeyT, ValueT>::deserialize(src + deltas.cached_size, n);
		auto delta_size = delta.size();
		deltas.cached_size += delta_size;
		deltas.deltas.push_back(std::move(delta));
	}

	return deltas;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Deltas<KeyT, ValueT> &type) {
	os << "Deltas:" << std::endl;
	for (const auto &delta : type.deltas)
		os << delta << std::endl;

	return os;
}
// -----------------------------------------------------------------
// Explicit instantiations
template struct Delta<UInt64, TID>;
template struct Delta<String, TID>;
template struct Deltas<UInt64, TID>;
template struct Deltas<String, TID>;
template std::ostream &operator<<(std::ostream &, const Delta<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &, const Delta<String, TID> &);
template std::ostream &operator<<(std::ostream &, const Deltas<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &, const Deltas<String, TID> &);
// -----------------------------------------------------------------
} // namespace bbbtree
