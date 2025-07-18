#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"
#include "bbbtree/types.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <vector>

// TODO: We currently do not persist state through the buffer manager while
// running the B-Tree. We only persist the state on destruction. Might want to
// maintain state through the buffer manager during runtime though.

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
		Node(uint32_t page_size, uint16_t level)
			: data_start(page_size), level(level), slot_count(0) {}

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
	/// keys pivoting to other nodes. Entries are <KeyT, PageID>. They are
	/// created upon node splits.
	struct InnerNode final : public Node {
		/// Indicates the position and length of the key within the page.
		/// Contains the key's corresponding child (PageID).
		struct Slot {
			/// Default Constructor.
			Slot() = delete;
			/// Constructor.
			Slot(PageID child, uint32_t offset, uint16_t key_size)
				: child(child), offset(offset), key_size(key_size) {}
			/// The child of the pivot.
			PageID child;
			/// The offset within the page.
			uint32_t offset;
			/// The number of bytes from offset to end of key.
			uint16_t key_size;
			/// The number of bytes from end of key to end of entry.
			/// TODO: Add padding to ensure memory alignment for keys as well.
			uint16_t padding{0};

			/// Returns a const reference to the key this slot is pointing to.
			const KeyT &get_key(const std::byte *begin) const {
				assert(key_size);
				return *reinterpret_cast<const KeyT *>(begin + offset);
			}

			/// Returns a const reference to the key this slot is pointing to.
			KeyT &get_key(std::byte *begin) {
				assert(key_size);
				return *reinterpret_cast<KeyT *>(begin + offset);
			}
		};

	  public:
		/// Default Constructor.
		InnerNode() = delete;
		/// Constructor. Used when needing a new node for a node split.
		InnerNode(uint32_t page_size, uint16_t level);

		/// Returns true if this leaf has enough space for the given key/value
		/// pair.
		bool has_space(const KeyT &pivot) {
			return get_free_space() >= (sizeof(pivot) + sizeof(Slot));
		}

		/// Returns the appropriate child pointer for a given pivot.
		/// Returns `upper` if all pivots are smaller and `upper` is a valid
		/// page. Returns nullopt if `upper` is not initialized.
		PageID lookup(const KeyT &pivot);

		/// Splits the node in two.
		/// TODO: Set upper correctly when splitting/creating a new root.
		/// When splitting, upper of old page must get page id of pivot slot.
		[[nodiscard]] const KeyT &split(InnerNode &new_node);

		/// Updates the pivot of a split child. Must be called before
		/// inserting the new pivot that resulted from the split.
		void update(const KeyT &pivot, const PageID child);

		/// Inserts a new pivot/child pair resulting from a split. Pivot must be
		/// unique. Must have enough space. Returns true if key was inserted.
		[[nodiscard]] bool insert(const KeyT &pivot, const PageID child);

		/// Returns all children of this node.
		std::vector<PageID> get_children();

		/// Return the right-most child of this node.
		PageID get_upper() { return upper; }

		/// Print to standard output.
		void print();

	  private:
		/// Right-most child. Pivot for all keys bigger than the biggest pivot.
		/// Must be set during node splitting. Zero is invalid.
		PageID upper;

		/// Returns the first slot whose key is not smaller than the given
		/// pivot. Returns pointer to `slots_end` if no such slot is found.
		/// Caller then must usually handle the `upper` of the node.
		Slot *lower_bound(const KeyT &pivot);

		/// Get begin of slots section.
		Slot *slots_begin() {
			return reinterpret_cast<Slot *>(this->get_data() +
											sizeof(InnerNode));
		}
		/// Get end of slots section.
		Slot *slots_end() { return slots_begin() + this->slot_count; }

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
		[[nodiscard]] bool insert(const KeyT &key, const ValueT &value);

		/// Splits the leaf and returns the new pivot key for the parent.
		[[nodiscard]] const KeyT &split(LeafNode &new_node, size_t page_size);

		/// Print leaf to standard output.
		void print();

	  private:
		/// Get free space in bytes. Equals the space between the header + slots
		/// and data section.
		size_t get_free_space() {
			assert(this->data_start >=
				   (sizeof(LeafNode) + this->slot_count * sizeof(Slot)));
			return this->data_start - sizeof(LeafNode) -
				   this->slot_count * sizeof(Slot);
		};
		/// Get the slot whose key is not smaller than the given key.
		/// If no such key is found, returns pointer to end of slot section.
		Slot *lower_bound(const KeyT &key);

		/// Get beginning of slots section.
		Slot *slots_begin() {
			return reinterpret_cast<Slot *>(this->get_data() +
											sizeof(LeafNode));
		}
		/// Get end of slots section.
		Slot *slots_end() { return slots_begin() + this->slot_count; }
	};

	/// Constructor. Not thread-safe.
	BTree(SegmentID segment_id, BufferManager &buffer_manager);

	/// Destructor.
	~BTree();

	/// Lookup an entry in the tree. Returns `nullopt` if key was not found.
	std::optional<ValueT> lookup(const KeyT &key);

	/// Erase an entry in the tree.
	void erase(const KeyT &key);

	/// Inserts a new entry into the tree.
	void insert(const KeyT &key, const ValueT &value);

	/// Print tree. Not thread-safe.
	void print();

	/// Returns the number of key/value pairs stored in the tree.
	/// Do not use for production, only for testing. Traverses whole tree.
	/// Not thread-safe.
	size_t size();

  private:
	/// TODO: Find a more elegant solution for persistency:
	/// State is persisted at page 0 of this segment.
	/// Read and written out at construction/destruction time.

	/// The page of the current root.
	/// Important: Whenever someone tries to acquire a lock on this page,
	/// make sure after acquisition that this page is still the root.
	PageID root;
	/// The next free, unique page ID.
	PageID next_free_page;

	/// Returns the appropriate leaf page for a given key.
	/// Potentially splits nodes if full.
	BufferFrame &get_leaf(const KeyT &key, bool exclusive);

	/// Traverses tree for given key and splits corresponding leaf.
	/// Only splits if leaf is full. Another thread might have triggered split
	/// already. Holds all locks on the path for cascading splits.
	void split(const KeyT &key);

	PageID get_new_page();
};
} // namespace bbbtree
