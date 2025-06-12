#ifndef INCLUDE_BBBTREE_DATABASE_H_
#define INCLUDE_BBBTREE_DATABASE_H_

#include "bbbtree/buffer_manager.h"
#include "bbbtree/tuple_id.h"
#include "bbbtree/segment.h"

#include <vector>
#include <cstdint>

// TODO: Rethink `logic_errors` and which error makes more sense to throw.

namespace bbbtree
{
    static const constexpr size_t PAGE_SIZE = 1024;
    static const constexpr size_t NUM_PAGES = 10;
    static const constexpr SegmentID FSI_SEGMENT_ID = 0;
    static const constexpr SegmentID SP_SEGMENT_ID = 1;

    /// A Tuple with key and value that are stored in the database.
    struct Tuple
    {
        using Key = uint64_t;
        using Value = uint64_t;

        Key key;
        Value value;

        /// Equality operator.
        bool operator==(const Tuple &other) const { return key == other.key && value == other.value; }
    };

    /// A Database maintains a single table of keys and values. The schema is fixated at compile-time.
    class Database
    {
    public:
        /// Constructor.
        Database() : buffer_manager(PAGE_SIZE, NUM_PAGES), space_inventory(FSI_SEGMENT_ID, buffer_manager), records(SP_SEGMENT_ID, buffer_manager, space_inventory) {}

        /// Inserts a tuple into the database.
        void insert(Tuple &tuple);
        /// Inserts tuples into the database.
        /// Tuple keys must not be already present in database.
        void insert(std::vector<Tuple> tuples);
        /// Reads a value by key from the database.
        Tuple get(Tuple::Key key);
        /// Deletes a tuple by key from the database.
        void erase(Tuple::Key key);
        /// Updates a tuple by key. Key must already be present in database.

    private:
        /// The buffer manager.
        BufferManager buffer_manager;
        /// The Free-Space-Inventory segment.
        FSISegment space_inventory;
        /// The Slotted Pages segment.
        SPSegment records;
        /// The access path. Maps keys to their tuple IDs.
        /// TODO: Template the database on the index.
        std::unordered_map<Tuple::Key, TID> index;
    };
}

#endif // INCLUDE_BBBTREE_DATABASE_H_