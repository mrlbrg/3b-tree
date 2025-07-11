#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"
#include "bbbtree/types.h"

#include <cassert>
#include <optional>

// TODO: We must support variable sized values as well, not only variable sized
// keys Since we want to support a Delta-BTree later, which does not hold TIDs
// as values but a whole update/tuple.

namespace bbbtree {
/// Requirements for the keys of the tree. TODO.
template <typename T>
concept LessEqualComparable = requires(T a, T b) {
	{ a <= b } -> std::convertible_to<bool>;
	{ a == b } -> std::convertible_to<bool>;
	sizeof(T);
};

/// External Storage index that maps from unique, possibly variable-length keys
/// to TID identifying the tuple's location on the slotted pages. Due to
/// variable-size key-support, we implement slots that map to <key, TID> pairs
/// within a tree node.
/// Keys and Values cannot be bigger than 64 KB (Slots have 16 bits for the size
/// of each).
/// TODO: Single threaded for now.
/// TODO: Does not implement delete yet. When deleting keys, we do not
/// re-use/compactify the space nor merge nodes. We leave nodes fragmented.
/// TODO: We cannot use one file per index if we want to have several trees
/// later.
template <LessEqualComparable KeyT, typename ValueT>
struct BTree final : public Segment {
	/// A header that keeps track of the metadata. Always located at page 0 of
	/// this segment. Read and written out to at construction and destruction
	/// time of the B-Tree.
	struct Header {
		PageID root_page;
		PageID next_free_page;
	};

	/// Header of a generic tree node. Always consists of a header, a slot
	/// section and the data section.
	struct Node {
		/// Upper end of data. Where new data can be prepended.
		uint32_t data_start;
		/// The level in the tree.
		uint16_t level;
		/// The number of entries.
		uint16_t slot_count;

		/// Constructor.
		Node(uint32_t page_size, uint16_t level, uint16_t slot_count = 0)
			: data_start(page_size), level(level), slot_count(slot_count) {}

		/// Is the node a leaf node?
		bool is_leaf() const { return level == 0; }

		/// Is other Node self?
		bool operator==(const Node &other) const { return this == &other; }

	  protected:
		/// Get data.
		std::byte *get_data() { return reinterpret_cast<std::byte *>(this); }
		/// Get constant data.
		const std::byte *get_data() const {
			return reinterpret_cast<const std::byte *>(this);
		}

		/// Casts a page's data into a Node.
		static Node &get_node_from_page(char *page) {
			return *reinterpret_cast<Node *>(page);
		}
	};

	/// Specialization of a node that is internal, not a leaf. Its entries are
	/// keys pivoting to other nodes. Entries are <KeyT, PageID>. They always
	/// start out with at least two children, since they are only created upon
	/// node splits.
	struct InnerNode final : public Node {
		/// Indicates the position and length of the key within the page.
		/// Contains the key's corresponding child (PageID).
		struct Slot {
			/// Default Constructor.
			Slot() = delete;
			/// The child of the pivot.
			PageID child;
			/// The offset within the page.
			uint32_t offset;
			/// The number of bytes from offset to end of key.
			uint16_t key_size;
			/// The number of bytes from end of key to end of entry.
			/// TODO: Add padding to ensure memory alignment for keys as well.
			uint16_t padding;
		};

	  public:
		/// Default Constructor.
		InnerNode() = delete;
		/// Constructor.
		InnerNode(uint32_t page_size, uint16_t level, PageID first_child,
				  PageID upper);

		/// Returns true if this leaf has enough space for the given key/value
		/// pair.
		bool has_space(const KeyT &key) {
			return get_free_space() >= (sizeof(key) + sizeof(Slot));
		}

		/// Returns the appropriate PageID for a given key.
		/// Returns `upper` if all keys are smaller.
		PageID lower_bound(const KeyT &key);

		[[nodiscard]] const KeyT &split(const KeyT &key, const ValueT &value);

	  private:
		/// Right-most child. Pivot for all keys bigger than the biggest pivot.
		/// Must be set lazily upon retrieval. Zero is invalid.
		PageID upper;

		/// Get begin of slots section.
		Slot *slots_begin() {
			return reinterpret_cast<Slot *>(this->get_data() +
											sizeof(InnerNode));
		}
		/// Get end of slots section.
		Slot *slots_end() {
			return slots_begin() + this->slot_count * sizeof(Slot);
		}

		/// Get free space in bytes. Equals the space between the header + slots
		/// and data section.
		size_t get_free_space() {
			return this->data_start - sizeof(InnerNode) -
				   this->slot_count * sizeof(Slot);
		};
	};

	/// Specialization of a node that is a leaf.
	/// Entries are <KeyT, ValueT> where ValueT usually is a TID in an index.
	struct LeafNode final : public Node {
		/// Indicates the position and length of the key/value pair within the
		/// node.
		struct Slot {
			/// Default Constructor.
			Slot() = delete;

			Slot(uint32_t offset, uint16_t key_size, uint16_t value_size)
				: offset(offset), key_size(key_size), value_size(value_size) {};

			/// The offset within the page.
			uint32_t offset;
			/// The number of bytes from offset to end of key.
			uint16_t key_size;
			/// The number of bytes from end of key to end of entry.
			uint16_t value_size;

			/// Returns a const reference to the key this slot is pointing to.`
			const KeyT &get_key(const std::byte *begin) const {
				assert(key_size);
				return *reinterpret_cast<const KeyT *>(begin + offset);
			}
			/// Returns a const reference to the value this slot is pointing
			/// to.`
			const ValueT &get_value(const std::byte *begin) const {
				assert(value_size);
				return *reinterpret_cast<const ValueT *>(begin + offset +
														 key_size);
			}
			/// Returns a reference to the key this slot is pointing to.`
			KeyT &get_key(std::byte *begin) {
				assert(key_size);
				return *reinterpret_cast<KeyT *>(begin + offset);
			}
			/// Returns a reference to the value this slot is pointing to.`
			ValueT &get_value(std::byte *begin) {
				assert(value_size);
				return *reinterpret_cast<ValueT *>(begin + offset + key_size);
			}
		};
		/// Default Constructor.
		LeafNode() = delete;
		/// Constructor.
		explicit LeafNode(uint32_t page_size) : Node(page_size, 0) {}

		/// Returns true if this leaf has enough space for the given key/value
		/// pair.
		bool has_space(const KeyT &key, const ValueT &value) {
			return get_free_space() >=
				   (sizeof(key) + sizeof(value) + sizeof(Slot));
		}

		/// Get the index of the first key that is not less than than a provided
		/// key.
		std::optional<ValueT> lookup(const KeyT &key);

		/// Inserts a key, value pair into this leaf. Returns true if key was
		/// actually inserted. Returns false if key already exists. Caller must
		/// ensure that there is enough space.
		bool insert(const KeyT &key, const ValueT &value);

		/// Splits the leaf and returns the new pivot key for the parent.
		[[nodiscard]] const KeyT &split(const KeyT &key, const ValueT &value);

	  private:
		/// Get free space in bytes. Equals the space between the header + slots
		/// and data section.
		size_t get_free_space() {
			return this->data_start - sizeof(LeafNode) -
				   this->slot_count * sizeof(Slot);
		};
		/// Get the slot whose key is not smaller than the given key.
		/// If no such key is found, returns pointer to end of slot section.
		Slot *lower_bound(const KeyT &key);

		/// Get start of slots section.
		Slot *slots_begin() {
			return reinterpret_cast<Slot *>(this->get_data() +
											sizeof(LeafNode));
		}
		/// get end of slots section.
		Slot *slots_end() { return slots_begin() + this->slot_count; }
	};

	/// Constructor. Not thread-safe.
	BTree(SegmentID segment_id, BufferManager &buffer_manager);

	/// Destructor.
	~BTree();

	/// Lookup an entry in the tree.
	std::optional<ValueT> lookup(const KeyT &key);

	/// Erase an entry in the tree.
	void erase(const KeyT &key);

	/// Inserts a new entry into the tree.
	void insert(const KeyT &key, const ValueT &value);

  private:
	/// The page of the current root.
	PageID root;
	/// The next free, unique page ID.
	PageID next_free_page;

	/// Returns the appropriate leaf page for a given key.
	/// Potentially splits nodes if full.
	BufferFrame &get_leaf(const KeyT &key, bool exclusive);

	/// Traverses tree for given key and splits corresponding leaf.
	/// Only splits if leaf is full. Another thread might have triggered split
	/// already. Holds all locks on the path for cascading splits.
	void split(const KeyT &key, const ValueT &value);
};
} // namespace bbbtree
