#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"
#include "bbbtree/types.h"

#include <cassert>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

// TODO: We currently do not persist the tree object while
// running the B-Tree (e.g. root page id). We only persist the state on
// destruction. Might want to maintain state through the buffer manager during
// runtime though.
// TODO: Made everything public for the delta tree to access intrinsics of the
// BTree. Friend declaration did not work, because it instantiated illegal code
// of the DeltaTree expecting the slot to have the `state` member. Try with
// `template <typename U> friend class DeltaTree;` later.
// TODO: Store key and value sizes in the page, not the slot.

namespace bbbtree {
// -----------------------------------------------------------------
/// Requirements for the keys and values of the index.
template <typename T>
concept Serializable =
	requires(T a, std::byte *dest, const std::byte *src, uint16_t n) {
		/// Returns the number of bytes of the serialized object.
		{ a.size() } -> std::same_as<uint16_t>;
		/// Serializes the object into the target position with `size()` bytes.
		{ a.serialize(dest) };
		/// Deserializes the object from the given data and size.
		/// The returned object can contain pointers into the src pointer (e.g.
		/// for string views)! Therefore do not use this object after releasing
		/// memory pointer to by `src`.
		{ T::deserialize(src, n) } -> std::same_as<T>;
	};
// -----------------------------------------------------------------
/// Keys and value need to be equality comparable for testing.
template <typename T>
concept Testable = requires(T a, T b) {
	{ a == b } -> std::convertible_to<bool>;
};
// -----------------------------------------------------------------
/// Requirements to search for keys in the B-Tree.
template <typename T>
concept LowerBoundable = requires(T a, T b) {
	{ a <= b } -> std::convertible_to<bool>;
};
// -----------------------------------------------------------------
/// Requires printing to std::cout.
template <typename T>
concept Printable = requires(T a) {
	{ std::cout << a } -> std::same_as<std::ostream &>;
};
// -----------------------------------------------------------------
/// Requirements for the values of the index.
template <typename T>
concept ValueIndexable = Serializable<T> && Testable<T> && Printable<T>;
// -----------------------------------------------------------------
/// Requirements for the keys of the index.
template <typename T>
concept KeyIndexable = ValueIndexable<T> && LowerBoundable<T>;
// -----------------------------------------------------------------
/// Types of operations performed on the index.
enum class OperationType : uint8_t {
	None = 0, // Default initialized value
	Insert = 1,
	Update = 2,
	Delete = 3
};
std::ostream &operator<<(std::ostream &os, const OperationType &type);
// -----------------------------------------------------------------
/// If we track deltas, we need to store additional state in the Slot.
/// Template specializations must be at namespace scope, so we put it here.
/// TODO: Adding 1B for state brings the size of the slot to 12B. This affects
/// fanout. Try something better.
template <bool TrackDeltas> struct SlotBase {};
template <> struct SlotBase<true> {};
// -----------------------------------------------------------------
/// External Storage index that maps from unique, possibly variable-length keys
/// to TID identifying the tuple's location on the slotted pages. Due to
/// variable-size key-support, we implement slots that map to <key, TID> pairs
/// within a tree node.
/// Keys and Values cannot be bigger than 64 KB (Slots have 16 bits for the size
/// of each).
/// Values can also be deltas of a delta tree.
/// TODO: Single threaded for now.
/// TODO: Does not implement delete yet. When deleting keys, we do not
/// re-use/compactify the space nor merge nodes. We leave nodes fragmented.
/// TODO: We should not use one file per index if we want to have several trees
/// later.
template <KeyIndexable KeyT, ValueIndexable ValueT, bool UseDeltaTree = false>
struct BTree : public Segment {

	/// Constructor. Not thread-safe.
	BTree(SegmentID segment_id, BufferManager &buffer_manager,
		  PageLogic *page_logic = nullptr);

	/// Destructor.
	~BTree();

	/// Lookup an entry in the tree. Returns `nullopt` if key was not found.
	/// TODO: Not thread-safe, bc. the returns `ValueT` is a view into the
	/// node's data. Use with care and copy the value if necessary e.g. when
	/// multithreading. Only works single-threaded, when using the value before
	/// modfying the tree again.
	std::optional<ValueT> lookup(const KeyT &key);

	/// Erase an entry in the tree.
	void erase(const KeyT &key);

	/// Inserts a new entry into the tree. Returns false if key already exists.
	[[nodiscard]] bool insert(const KeyT &key, const ValueT &value);

	/// Print tree. Not thread-safe.
	void print();

	/// Returns the number of key/value pairs stored in the tree.
	/// Do not use for production, only for testing. Traverses whole tree.
	/// Not thread-safe.
	size_t size();

	/// Returns the number of levels in the tree.
	/// Not thread-safe.
	size_t height();

	/// TODO: Find a more elegant solution for persistency:
	/// State is persisted at page 0 of this segment.
	/// Read and written out at construction/destruction time.

	/// A generic tree node. Always consists of a header, a slot
	/// section and the data section. Only construct these on buffered pages
	/// provided by the `buffer_manager`.
	struct Node {
		/// Upper end of data. Where new data can be prepended.
		uint32_t data_start;
		/// The level in the tree. Leafs are level 0.
		const uint16_t level;
		/// The number of entries.
		uint16_t slot_count;

		/// Constructor.
		Node(uint32_t page_size, uint16_t level)
			: data_start(page_size), level(level), slot_count(0) {}

		/// Is the node a leaf node?
		bool is_leaf() const { return level == 0; }

		/// Is other Node self?
		bool operator==(const Node &other) const { return this == &other; }

		/// A slot in a node. Contains position and size of the key.
		struct Slot : public SlotBase<UseDeltaTree> {
			struct EmptySlotState {};

			/// Default Constructor.
			Slot() = delete;
			/// Constructor.
			Slot(uint32_t offset, uint16_t key_size)
				: offset(offset), key_size(key_size) {}

			/// Returns the key stored in this slot. `KeyT` is only a shallow
			/// copy from the node. Manage lifetime carefully.
			const KeyT get_key(const std::byte *begin) const;

			/// The offset within the page.
			uint32_t offset;
			/// The number of bytes from offset to end of key.
			uint16_t key_size;
			/// A conditional state for a BTree that needs to track deltas.
			/// `no_unique_address` ensures that this does not increase the size
			/// of a slot without this member.
			[[no_unique_address]] std::conditional_t<
				UseDeltaTree, OperationType, EmptySlotState> state;
		};
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
		/// Default Constructor.
		InnerNode() = delete;
		/// Constructor. Used when needing a new node for a node split.
		InnerNode(uint32_t page_size, uint16_t level, PageID upper);

		/// Returns true if this leaf has enough space for the given key/value
		/// pair.
		bool has_space(const KeyT &pivot) const {
			return get_free_space() >= (pivot.size() + sizeof(Pivot));
		}

		/// Returns the appropriate child pointer for a given pivot.
		/// Returns `upper` if all pivots are smaller and `upper` is a valid
		/// page. Returns nullopt if `upper` is not initialized.
		PageID lookup(const KeyT &pivot);

		/// Splits the node in two.
		/// TODO: Set upper correctly when splitting/creating a new root.
		/// When splitting, upper of old page must get page id of pivot slot.
		/// Returns reference to uppermost key on left node, therefore this node
		/// must not be released while the returned key is used. Otherwise we
		/// have a dangling reference.
		const KeyT split(InnerNode &new_node, size_t page_size);

		/// Inserts a new pivot/child pair resulting from a split.
		/// Pivot must be unique. Must have enough space.
		/// Returns true if key was inserted successfully. False otherwise.
		[[nodiscard]] bool insert_split(const KeyT &new_pivot,
										PageID new_child);

		/// Returns all children of this node.
		std::vector<PageID> get_children();

		/// Return the right-most child of this node.
		PageID get_upper() { return upper; }

		/// Print to standard output.
		void print();

		/// Indicates the position and length of the key within the page.
		/// Contains the key's corresponding child (PageID).
		struct Pivot final : public Node::Slot {
			/// Constructor.
			Pivot(std::byte *page_begin, uint32_t offset, const KeyT &key,
				  PageID child);

			PageID get_value(const std::byte * /*begin*/) const {
				return child;
			}

			/// The child of this pivot.
			PageID child;

			/// Print the slot to std output.
			void print(const std::byte *begin) const;
		};
		/// Right-most child. Pivot for all keys bigger than the biggest pivot.
		/// Must be set during node splitting. Zero is invalid.
		PageID upper;

		/// Returns the first slot whose key is not smaller than the given
		/// pivot. Returns pointer to `slots_end` if no such slot is found.
		/// Caller then must usually handle the `upper` of the node.
		Pivot *lower_bound(const KeyT &pivot);

		/// Insert a new slot. Returns false if key alredy exists. Caller must
		/// ensure that there is enough space. Otherwise its undefined behavior.
		[[nodiscard]] bool insert(const KeyT &pivot, PageID child);

		/// Get begin of slots section.
		Pivot *slots_begin() {
			return reinterpret_cast<Pivot *>(this->get_data() +
											 sizeof(InnerNode));
		}
		/// Get begin of slots section.
		const Pivot *slots_begin() const {
			return reinterpret_cast<const Pivot *>(this->get_data() +
												   sizeof(InnerNode));
		}
		/// Get end of slots section.
		Pivot *slots_end() { return slots_begin() + this->slot_count; }
		/// Get end of slots section.
		const Pivot *slots_end() const {
			return slots_begin() + this->slot_count;
		}

		/// Get free space in bytes. Equals the space between the header + slots
		/// and data section.
		size_t get_free_space() const {
			return this->data_start - sizeof(InnerNode) -
				   this->slot_count * sizeof(Pivot);
		};

	  public:
		static const constexpr size_t min_space =
			sizeof(InnerNode) + sizeof(Pivot);
	};

	/// Specialization of a node that is a leaf.
	/// Entries are <KeyT, ValueT> where ValueT is typically a TID in an index.
	struct LeafNode final : public Node {
		/// Indicates the position and length of the key/value pair within the
		/// node.
		struct LeafSlot final : public Node::Slot {
			/// Constructor.
			LeafSlot(std::byte *page_begin, uint32_t offset, const KeyT &key,
					 const ValueT &value);

			/// Returns a const reference to the value this slot is pointing
			/// to.
			const ValueT get_value(const std::byte *begin) const;

			void print(const std::byte *begin) const;

		  private:
			/// The number of bytes from end of key to end of entry.
			uint16_t value_size;
		};
		/// Default Constructor.
		LeafNode() = delete;
		/// Constructor.
		explicit LeafNode(uint32_t page_size) : Node(page_size, 0) {}

		/// Returns true if this leaf has enough space for the given key/value
		/// pair.
		bool has_space(const KeyT &key, const ValueT &value) const {
			return get_free_space() >=
				   (key.size() + value.size() + sizeof(LeafSlot));
		}

		/// Get the index of the first key that is not less than than a provided
		/// key. TODO: Some `ValueT` like `String` only return a view onto the
		/// node. This can become a dangling reference if the node is released.
		/// Use with care and copy the value if necessary e.g. when
		/// multithreading.
		std::optional<ValueT> lookup(const KeyT &key);

		/// Inserts a key, value pair into this leaf. Returns true if key was
		/// actually inserted. Returns false if key already exists. Caller must
		/// ensure that there is enough space.
		[[nodiscard]] bool insert(const KeyT &key, const ValueT &value);

		/// Splits the leaf and returns the resulting pivotal key to be inserted
		/// into the parent. `this` leaf is guaranteed to be the left node and
		/// `new_node` the right node after splitting.
		[[nodiscard]] const KeyT split(LeafNode &new_node, const KeyT &key,
									   size_t page_size);

		/// Print leaf to standard output.
		void print();

		/// Get free space in bytes. Equals the space between the header + slots
		/// and data section.
		size_t get_free_space() const {
			assert(this->data_start >=
				   (sizeof(LeafNode) + this->slot_count * sizeof(LeafSlot)));
			return this->data_start - sizeof(LeafNode) -
				   this->slot_count * sizeof(LeafSlot);
		};
		/// Get the slot whose key is not smaller than the given key.
		/// If no such key is found, returns pointer to end of slot section.
		LeafSlot *lower_bound(const KeyT &key);

		/// Get beginning of slots section.
		LeafSlot *slots_begin() {
			return reinterpret_cast<LeafSlot *>(this->get_data() +
												sizeof(LeafNode));
		}
		/// Get beginning of slots section.
		const LeafSlot *slots_begin() const {
			return reinterpret_cast<const LeafSlot *>(this->get_data() +
													  sizeof(LeafNode));
		}
		/// Get end of slots section.
		LeafSlot *slots_end() { return slots_begin() + this->slot_count; }
		/// Get end of slots section.
		const LeafSlot *slots_end() const {
			return slots_begin() + this->slot_count;
		}

		/// The minimum of space required on a page to store a single entry.
		static const constexpr size_t min_space =
			sizeof(LeafNode) + sizeof(LeafSlot);
	};

	/// The page of the current root.
	/// Important TODO: When multithreading, whenever someone tries to acquire a
	/// lock on this page, make sure after acquisition that this page is still
	/// the root.
	PageID root;
	/// The next free, unique page ID.
	PageID next_free_page;
	/// The page logic specific to this tree. Called back by the buffer manager
	/// when loading/unloading pages.
	PageLogic *page_logic;

	/// Returns the appropriate leaf page for a given key.
	/// Potentially splits nodes if full.
	BufferFrame &get_leaf(const KeyT &key, bool exclusive);

	/// Traverses tree for given key and splits corresponding leaf.
	/// Only splits if leaf is full. Another thread might have triggered split
	/// already. Holds all locks on the path for cascading splits.
	void split(const KeyT &key, const ValueT &value);

	PageID get_new_page();
};
// -----------------------------------------------------------------
} // namespace bbbtree