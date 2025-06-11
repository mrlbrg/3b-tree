#ifndef INCLUDE_BBBTREE_DATABASE_H_
#define INCLUDE_BBBTREE_DATABASE_H_

#include "bbbtree/buffer_manager.h"
#include "bbbtree/tuple_id.h"
#include "bbbtree/segment.h"

#include <vector>
#include <cstdint>

namespace bbbtree
{
    static const constexpr size_t PAGE_SIZE = 1024;
    static const constexpr size_t NUM_PAGES = 10;
    static const constexpr SegmentID FSI_SEGMENT_ID = 0;
    static const constexpr SegmentID SP_SEGMENT_ID = 1;

    /// A Database maintains a single table of keys and values. The schema is fixated at compile-time.
    class Database
    {
        using Key = uint64_t;
        using Value = uint64_t;
        using Tuple = std::pair<Key, Value>;

    public:
        /// Constructor.
        Database() : buffer_manager(PAGE_SIZE, NUM_PAGES), space_inventory(FSI_SEGMENT_ID, buffer_manager), slotted_pages(SP_SEGMENT_ID, buffer_manager, space_inventory) {}

        /// Inserts into the database.
        void insert(const std::vector<Tuple> tuples);
        /// Reads a tuple by TID from the database.
        Tuple get(TID tid);
        /// Deletes a tuple by TID from the database.
        void erase(TID tid);

    private:
        /// The buffer manager.
        BufferManager buffer_manager;
        /// The Free-Space-Inventory segment.
        FSISegment space_inventory;
        /// The Slotted Pages segment.
        SPSegment slotted_pages;
    };
}

#endif // INCLUDE_BBBTREE_DATABASE_H_