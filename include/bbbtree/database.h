#ifndef INCLUDE_BBBTREE_DATABASE_H_
#define INCLUDE_BBBTREE_DATABASE_H_

#include "bbbtree/buffer_manager.h"
#include "bbbtree/tuple_id.h"

#include <vector>
#include <cstdint>

namespace bbbtree
{
    /// A Database maintains a single table of keys and values. The schema is fixated at compile-time.
    class Database
    {
        using Key = uint64_t;
        using Value = uint64_t;
        using Tuple = std::pair<Key, Value>;

    public:
        /// Constructor.
        // Database() : buffer_manager(1024, 10) {}

        /// Inserts into the database.
        void insert(const std::vector<Tuple> tuples);
        /// Reads a tuple by TID from the database.
        Tuple get(TID tid);
        /// Deletes a tuple by TID from the database.
        void erase(TID tid);

    private:
        /// The buffer manager.
        /// The Free-Space-Inventory segment.
        /// The Slotted Pages segment.
    }

}

#endif // INCLUDE_BBBTREE_DATABASE_H_