#ifndef INCLUDE_BBBTREE_BUFFER_MANAGER_H_
#define INCLUDE_BBBTREE_BUFFER_MANAGER_H_

#include "bbbtree/types.h"
#include <unordered_map>

namespace bbbtree
{

    class BufferFrame
    {
    };

    class BufferManager
    {
    public:
        /// Get a page from the buffer by page ID.
        BufferFrame &fix_page(PageID page_id);

        /// Releases a page. If dirty, its written to disk eventually.
        void unfix_page(BufferFrame &frame, bool is_dirty);

    private:
        /// The size of each page in the buffer.
        size_t page_size;
        /// The map from page IDs to the corrensponding pages.
        std::unordered_map<PageID, BufferFrame> pages;
    }:
}
#endif