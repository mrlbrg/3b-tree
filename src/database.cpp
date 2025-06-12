#include "bbbtree/database.h"

#include <stdexcept>

namespace bbbtree
{

    void Database::insert(Tuple &tuple)
    {
        // Get a new TID
        auto tid = records.allocate(sizeof(tuple));
        // Add TID to index
        auto [it, success] = index.try_emplace(tuple.key, tid);
        if (!success)
            throw std::logic_error("Database::insert(): Key already in database.");
        // Insert tuple in records
        records.write(tid, reinterpret_cast<const std::byte *>(&tuple), sizeof(tuple));
    }

    void Database::insert(std::vector<Tuple> tuples)
    {
        for (auto &tuple : tuples)
            insert(tuple);
    }

    Tuple Database::get(Tuple::Key key)
    {
        // Get TID for key
        auto it = index.find(key);
        if (it == index.end())
            throw std::out_of_range("Database::get(): Key not found.");
        // Get Tuple
        Tuple tuple{};
        auto bytes_read = records.read(it->second, reinterpret_cast<std::byte *>(&tuple), sizeof(tuple));
        if (bytes_read != sizeof(tuple))
            throw std::logic_error("Database::get(): Read corrupted.");

        return tuple;
    }

    void Database::erase(Tuple::Key key)
    {
        // TODO
        throw std::logic_error("Database::erase(): Not implemented yet.");
    }

} // namespace bbbtree}