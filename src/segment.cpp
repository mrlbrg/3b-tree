#include "bbbtree/segment.h"

#include <stdexcept>

namespace bbbtree
{

    std::optional<uint64_t> FSISegment::find(uint32_t required_space)
    {
        // TODO: actually store all this information on a page to persist it.

        // Enough space?
        if (free_space < required_space)
            return {};

        return {allocated_pages - 1};
    }

    void FSISegment::update(uint64_t target_page, uint32_t new_free_space)
    {
        if (target_page != (allocated_pages - 1))
            throw std::logic_error("FSISegment::update(): Cannot update a page's free space which is not the last.");

        if (free_space < new_free_space)
            throw std::logic_error("FSISegment::update(): Free space on a slotted page can only shrink.");

        free_space = new_free_space;
    }

    size_t FSISegment::create_new_page()
    {
        ++allocated_pages;
        free_space = buffer_manager.get_page_size();
        return allocated_pages - 1;
    }
}