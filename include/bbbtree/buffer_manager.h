#ifndef INCLUDE_BBBTREE_BUFFER_MANAGER_H_
#define INCLUDE_BBBTREE_BUFFER_MANAGER_H_

#include "bbbtree/types.h"

#include <unordered_map>
#include <vector>
#include <exception>

namespace bbbtree
{
    enum class State
    {
        UNDEFINED,
        CLEAN,
        DIRTY
    };

    class BufferFrame
    {
    private:
        /// The page's ID.
        PageID page_id;
        /// The data of the page this frame maintains.
        /// The pointer remains constant.
        /// The pages are swapped though, overwriting the data.
        char *const data;
        /// The state of the page. Undefined by default.
        State state = State::UNDEFINED;

        /// Loads a page into the frame.
        void load_page(SegmentID segmentID, PageID page_id, size_t page_size);

        friend class BufferManager;

    public:
        /// Constructor.
        BufferFrame() = delete;
        /// Constructor.
        explicit BufferFrame(char *const data) : data(data) {}

        /// Returns a pointer to this page's data.
        char *get_data() const { return data; }
    };

    class buffer_full_error
        : public std::exception
    {
    public:
        [[nodiscard]] const char *what() const noexcept override
        {
            return "buffer is full";
        }
    };

    /// Manages all pages in memory.
    /// Transparently swaps pages between storage and memory when buffer becomes full.
    /// TODO: For now, does not actually perform I/O.
    /// TODO: For now, it runs in a single thread.
    /// TODO: For now, it cannot evict pages.
    class BufferManager
    {
    public:
        /// Constructor.
        /// @param[in] page_size  Size in bytes that all pages will have.
        /// @param[in] page_count Maximum number of pages that should reside in memory at most.
        explicit BufferManager(size_t page_size, size_t page_count);
        /// Destructor. Writes all dirty pages to disk.
        ~BufferManager();

        /// Copy Constructor.
        BufferManager(const BufferManager &) = delete;
        /// Move Constructor.
        BufferManager(BufferManager &&) = delete;
        /// Copy Assignment.
        BufferManager &operator=(const BufferManager &) = delete;
        /// Move Assignment.
        BufferManager &operator=(BufferManager &&) = delete;

        /// Get a page from the buffer by page ID and segment ID.
        /// Expects a pure page ID, not a tuple ID (TID).
        BufferFrame &fix_page(SegmentID segment_id, PageID page_id, bool exclusive);

        /// Releases a page. If dirty, its written to disk eventually.
        void unfix_page(BufferFrame &frame, bool is_dirty);

        size_t get_page_size() { return page_size; }

    private:
        /// Gets a free buffer frame. Evicts another page when buffer is full.
        BufferFrame &get_free_frame();

        /// The size of each page in the buffer.
        size_t page_size;
        /// The pages' data.
        std::vector<char> page_data;
        /// The pages' frames.
        std::vector<BufferFrame> page_frames;
        /// The map from page IDs to the corrensponding pages.
        std::unordered_map<PageID, BufferFrame *> id_to_frame;
        // Tracks pointers to unused BufferFrames
        std::vector<BufferFrame *> free_buffer_frames;
    };
}
#endif // INCLUDE_BBBTREE_BUFFER_MANAGER_H_