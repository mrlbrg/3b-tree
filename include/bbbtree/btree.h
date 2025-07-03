#pragma once

#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"
#include "bbbtree/types.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <optional>

namespace bbbtree
{

    /// External Storage index that maps from unique, possibly variable-length keys to TID identifying the tuple's location
    /// on the slotted pages.
    /// Due to variable-size key-support, we implement slots that map to <key, TID> pairs within a tree node.
    /// TODO: Single threaded for now.
    /// TODO: Does not implement delete yet. When deleting keys, we do not re-use/compactify the space nor merge nodes.
    /// We leave nodes fragmented.
    /// TODO: We cannot use one file per index if we want to have several trees later.
    template <typename KeyT, typename ValueT>
    struct BTree final : public Segment
    {
        /// A header that keeps track of the metadata. Always located at page 0 of this segment.
        struct Header
        {
            PageID root_page;
            PageID next_free_page;
        };

        /// A slot indicates the offset and length of the corresponding entry. No redirects for now.
        struct Slot
        {
            /// Constructor
            Slot() = default;

            /// Clear the slot.
            void clear() { value = 0; }
            /// Get the size.
            [[nodiscard]] uint32_t get_size() const { return value & 0xFFFFFFull; }
            /// Get the offset.
            [[nodiscard]] uint32_t get_offset() const { return (value >> 24) & 0xFFFFFFull; }
            /// Is empty?
            [[nodiscard]] bool is_empty() const { return value == 0; }

            /// Set the slot.
            void set_slot(uint32_t offset, uint32_t size)
            {
                value = 0;
                value ^= size & 0xFFFFFFull;
                value ^= (offset & 0xFFFFFFull) << 24;
            }

        private:
            /// The slot value: T (1 byte) | S (1 byte) | O (3 byte) | L (3 byte)
            /// T: If != 0xFF, the slot points to another record. The value is the TID.
            /// S: If = 0, the tuple is at offset O with length L.
            /// Otherwise, the tuple was moved from another page, placed at offset O with length L. The first 8 bytes contain the original TID.
            uint64_t value{0};
        };

        /// Header of a generic tree node. Always consists of a header, a slot section and the data section.
        struct Node
        {
            /// Upper end of data. Where new data can be prepended.
            uint32_t data_start;
            /// The number of entries.
            uint16_t slot_count;
            /// The level in the tree.
            uint16_t level;

            /// Constructor.
            Node(uint32_t page_size, uint16_t level, uint16_t slot_count = 0)
                : data_start(page_size), level(level), slot_count(slot_count) {}

            /// Is the node a leaf node?
            bool is_leaf() const { return level == 0; }

            /// Is other Node self?
            bool operator==(const Node &other) const { return this == &other; }

            /// Casts a page's data into a Node.
            static Node &get_node_from_page(char *page) { return *reinterpret_cast<Node *>(page); }
        };

        // Specialization of a node that is internal, not a leaf. Its entries are keys pivoting to other nodes.
        struct InnerNode final : public Node
        {
            /// Constructor.
            InnerNode(uint32_t page_size, uint16_t level) : Node(page_size, level, 0) {}

            /// Get free space in bytes. Equals the space between the header + slots and data section.
            size_t get_free_space() { return this->data_start - sizeof(InnerNode) - this->slot_count * sizeof(Slot); };
        };

        /// Specialization of a node that is a leaf. Its entries are keys and corresponding tuple IDs (`TID`s).
        struct LeafNode final : public Node
        {
            /// Constructor.
            LeafNode() = delete;
            /// Constructor.
            explicit LeafNode(uint32_t page_size) : Node(page_size, 0) {}

            /// Get free space in bytes. Equals the space between the header + slots and data section.
            size_t get_free_space() { return this->data_start - sizeof(LeafNode) - this->slot_count * sizeof(Slot); };
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
        BufferFrame &lookup_leaf(const KeyT &key);
    };
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    BTree<KeyT, ValueT>::BTree(SegmentID segment_id, BufferManager &buffer_manager) : Segment(segment_id, buffer_manager)
    {
        // TODO: only read from page at startup-time, then load it into memory and write it out on destruction again.
        // TODO: Create some meta-data segment that stores information like the root's page id on file.
        auto &frame = buffer_manager.fix_page(segment_id, 0, false);
        auto &header = *(reinterpret_cast<Header *>(frame.get_data()));

        // Load meta-data into memory.
        root = header.root_page;
        next_free_page = header.next_free_page;

        // Start a new tree?
        if (next_free_page == 0)
        {
            root = 1;
            next_free_page = 2;
            // Intialize root node.
            auto &root_page = buffer_manager.fix_page(segment_id, header.root_page, true);
            *(new (root_page.get_data()) LeafNode(buffer_manager.get_page_size()));
            buffer_manager.unfix_page(root_page, true);
        }

        buffer_manager.unfix_page(frame, false);
    }
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    BTree<KeyT, ValueT>::~BTree()
    {
        // Write out all meta-data needed to pick up index again later.
        // Caller must make sure that Buffer Manager is destroyed after this.
        auto &frame = buffer_manager.fix_page(segment_id, 0, true);
        auto &header = *(reinterpret_cast<Header *>(frame.get_data()));
        header.root_page = root;
        header.next_free_page = header.next_free_page;
        buffer_manager.unfix_page(frame, true);
    }
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    std::optional<ValueT> BTree<KeyT, ValueT>::lookup(const KeyT &key)
    {
        throw std::logic_error("Database::erase(): Not implemented yet.");
    }
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    void BTree<KeyT, ValueT>::erase(const KeyT &key)
    {
        throw std::logic_error("Database::erase(): Not implemented yet.");
    }
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    BufferFrame &BTree<KeyT, ValueT>::lookup_leaf(const KeyT &key)
    {
        auto &root_frame = buffer_manager.fix_page(segment_id, root, false);
        auto &root_node = *reinterpret_cast<InnerNode *>(root_frame.get_data());

        if (root_node.is_leaf())
            return root_frame;

        // Find the first slot which is not smaller than the key.

        // CONTINUE HERE
    }
    // -----------------------------------------------------------------
    template <typename KeyT, typename ValueT>
    void BTree<KeyT, ValueT>::insert(const KeyT &key, const ValueT &value)
    {
        throw std::logic_error("Database::erase(): Not implemented yet.");

        auto &leaf_page = lookup_leaf(key);
        auto &leaf_node = LeafNode::page_to_leaf(leaf_page.get_data());
        assert(!leaf_node.is_full() && "leaf_node must have enough space for new entry");
        leaf_node.insert(key, value);
        buffer_manager.unfix_page(leaf_page, true);
    }
    // -----------------------------------------------------------------
} // namespace bbbtree
