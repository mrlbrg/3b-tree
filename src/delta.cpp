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
void Delta<KeyT, ValueT>::deserialize(const std::byte *src) {
	// Deserialize the operation type.
	std::memcpy(&op, src, sizeof(op));
	src += sizeof(op);
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
	entry.key = KeyT::deserialize(src, key_size);
	src += key_size;
	// Deserialize the value.
	entry.value = ValueT::deserialize(src, value_size);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Delta<KeyT, ValueT> &type) {
	os << type.op;
	os << ": [" << type.entry.key << ", " << type.entry.value << "]";

	return os;
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
Deltas<KeyT, ValueT>::Deltas(std::variant<LeafDeltas, InnerNodeDeltas> &&deltas)
	: deltas(std::move(deltas)) {
	// Add size of header to serialized size.
	cached_size = sizeof(num_deltas());
	// Calculate the size of the deltas.
	std::visit(
		[&](auto &&arg) {
			for (const auto &delta : arg)
				cached_size += delta.size();
		},
		this->deltas);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
Deltas<KeyT, ValueT>::Deltas(std::variant<LeafDeltas, InnerNodeDeltas> &&deltas,
							 uint16_t size)
	: deltas(std::move(deltas)), cached_size(size) {}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
void Deltas<KeyT, ValueT>::serialize(std::byte *dst) const {
	// Serialize the number of deltas.
	auto n = num_deltas();
	std::memcpy(dst, &n, sizeof(n));
	dst += sizeof(n);
	// Serialize the deltas.
	std::visit(
		[&](auto &&arg) {
			for (const auto &delta : arg) {
				delta.serialize(dst);
				dst += delta.size();
			}
		},
		this->deltas);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
Deltas<KeyT, ValueT> Deltas<KeyT, ValueT>::deserialize(const std::byte *src,
													   uint16_t n) {
	// TODO: We deserialize LeafDeltas here. Serialize a bool to indicate if
	// this is a leaf or inner node's deltas.
	assert(n >= sizeof(uint16_t));
	std::variant<LeafDeltas, InnerNodeDeltas> deltas;
	// Deserialize the number of deltas.
	uint16_t num_deltas;
	std::memcpy(&num_deltas, src, sizeof(num_deltas));
	uint16_t cached_size = sizeof(num_deltas);
	// Deserialize the deltas.
	std::visit(
		[&](auto &&arg) {
			arg.resize(num_deltas);
			for (auto &delta : arg) {
				delta.deserialize(src + cached_size);
				cached_size += delta.size();
			}
		},
		deltas);

	return {std::move(deltas), cached_size};
}

// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
uint16_t Deltas<KeyT, ValueT>::num_deltas() const {
	return std::visit(
		[](const auto &deltas) {
			assert(deltas.size() <= std::numeric_limits<uint16_t>::max());
			return deltas.size();
		},
		this->deltas);
}
// -----------------------------------------------------------------
template <KeyIndexable KeyT, ValueIndexable ValueT>
std::ostream &operator<<(std::ostream &os, const Deltas<KeyT, ValueT> &type) {
	os << " Deltas: [" << type.size() << "B] [";
	std::visit(
		[&](auto &&arg) {
			for (const auto &delta : arg) {
				os << delta;
				os << " ";
			}
		},
		type.deltas);
	os << "]";
	return os;
}
// -----------------------------------------------------------------
// Explicit instantiations
template struct Delta<UInt64, TID>;
template struct Delta<UInt64, PID>;
template struct Delta<String, TID>;
template struct Delta<String, PID>;
template struct Deltas<UInt64, TID>;
template struct Deltas<String, TID>;
template std::ostream &operator<<(std::ostream &, const Delta<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &, const Delta<String, TID> &);
template std::ostream &operator<<(std::ostream &, const Deltas<UInt64, TID> &);
template std::ostream &operator<<(std::ostream &, const Deltas<String, TID> &);
// -----------------------------------------------------------------
} // namespace bbbtree
