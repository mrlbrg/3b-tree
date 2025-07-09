#pragma once

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
    /// TODO: Make keys variable-sized.
    struct Tuple
    {
        using Key = uint64_t;
        using Value = uint64_t;

        Key key;
        Value value;

        /// Equality operator.
        bool operator==(const Tuple &other) const { return key == other.key && value == other.value; }
    };

    /// A concept that requires some member functions for an index.
    // template <typename IndexT>
    // concept Indexable = requires(IndexT index, Tuple::Key key, Tuple::Value value) {
    //     { index.lookup(key) } -> std::same_as<std::optional<Tuple::Value>>;
    //     { index.erase(key) } -> std::same_as<void>;
    //     { index.insert(key, value) } -> std::same_as<void>;
    // };

    /// A Database maintains a single table of keys and values. The schema is fixated at compile-time.
    /// It is templated on its access path to tuples identified through their keys.
    template <typename IndexT>
    class Database
    {
    public:
        /// Constructor.
        Database(size_t page_size = PAGE_SIZE, size_t num_pages = NUM_PAGES, bool reset = false);

        /// Inserts a tuple into the database.
        /// TODO: Allow insert of variable sized tuples.
        void insert(Tuple &tuple);
        /// Inserts tuples into the database.
        /// Tuple keys must not be already present in database.
        void insert(std::vector<Tuple> tuples);
        /// Reads a value by key from the database.
        Tuple get(Tuple::Key key);
        /// Deletes a tuple by key from the database.
        /// TODO: Implement erase.
        void erase(Tuple::Key key);
        /// Returns the number of tuples stored in the database.
        size_t size() { return index.size(); }

    private:
        /// The Free-Space-Inventory segment.
        FSISegment space_inventory;
        /// The Slotted Pages segment.
        SPSegment records;
        /// The access path. Maps keys to their tuple IDs (`TID`).
        /// TODO: Template the database on the index.
        IndexT index{};
        /// The buffer manager. Note that the buffer manager must be the last member
        /// To ensure that it is destructed last. Other members might have pages to persist in their destructor.
        BufferManager buffer_manager;
    };
    // -----------------------------------------------------------------
    template <typename IndexT>
    Database<IndexT>::Database(size_t page_size, size_t num_pages, bool reset) : buffer_manager(page_size, num_pages), space_inventory(FSI_SEGMENT_ID, buffer_manager), records(SP_SEGMENT_ID, buffer_manager, space_inventory)
    {
        if (reset)
        {
            buffer_manager.reset(FSI_SEGMENT_ID);
            buffer_manager.reset(SP_SEGMENT_ID);
        }
    }
    // -----------------------------------------------------------------
    template <typename IndexT>
    void Database<IndexT>::insert(Tuple &tuple)
    {
        // Get a new TID
        auto tid = records.allocate(sizeof(tuple));
        // Add TID to index
        auto [it, success] = index.try_emplace(tuple.key, tid);
        if (!success)
        {
            records.erase(tid);
            throw std::logic_error("Database<IndexT>::insert(): Key already in database.");
        }
        // Insert tuple in records
        records.write(tid, reinterpret_cast<const std::byte *>(&tuple), sizeof(tuple));
    }
    // -----------------------------------------------------------------
    template <typename IndexT>
    void Database<IndexT>::insert(std::vector<Tuple> tuples)
    {
        // TODO: Detect sequential inserts?
        for (auto &tuple : tuples)
            insert(tuple);
    }
    // -----------------------------------------------------------------
    template <typename IndexT>
    Tuple Database<IndexT>::get(Tuple::Key key)
    {
        // Get TID for key
        auto it = index.find(key);
        if (it == index.end())
            throw std::out_of_range("Database<IndexT>::get(): Key not found.");
        // Get Tuple
        Tuple tuple{};
        auto bytes_read = records.read(it->second, reinterpret_cast<std::byte *>(&tuple), sizeof(tuple));
        if (bytes_read != sizeof(tuple))
            throw std::logic_error("Database<IndexT>::get(): Read corrupted.");

        return tuple;
    }
    // -----------------------------------------------------------------
    template <typename IndexT>
    void Database<IndexT>::erase(Tuple::Key key)
    {
        // TODO
        throw std::logic_error("Database<IndexT>::erase(): Not implemented yet.");
    }
}
