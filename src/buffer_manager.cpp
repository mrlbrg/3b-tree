// -----------------------------------------------------------------
#include "bbbtree/buffer_manager.h"
// -----------------------------------------------------------------
#include <cstring>
// -----------------------------------------------------------------
namespace bbbtree
{
    // -----------------------------------------------------------------
    void BufferFrame::load_page(SegmentID SegmentID, PageID page_id, size_t page_size)
    {
        // TODO: Do we need to store SegmentID on the page as well?
        this->page_id = page_id;
        this->state = State::CLEAN;
        // TODO: actually load data from file.
        std::memset(data, 0x00, page_size);
    }
    // -----------------------------------------------------------------
    BufferManager::BufferManager(size_t page_size, size_t page_count) : page_size(page_size)
    {
        // Allocate memory for Pages
        page_data.resize(page_count * page_size);
        // Reserve memory for Buffer Frames
        page_frames.reserve(page_count);
        // Reserve memory for free Buffer Frame pointers
        free_buffer_frames.reserve(page_count);
        // Reserve memory in HT
        id_to_frame.reserve(page_count);

        // Create Buffer Frames and
        // assign a constant Buffer ptr to each Buffer Frame
        for (auto data = page_data.begin(); data < page_data.end(); data += page_size)
        {
            page_frames.emplace_back(&(*data));
            free_buffer_frames.push_back(&(page_frames.back()));
        }
    }
    // -----------------------------------------------------------------
    BufferManager::~BufferManager()
    {
        // TODO: Write out all dirty pages.
    }
    // -----------------------------------------------------------------
    BufferFrame &BufferManager::fix_page(SegmentID segment_id, PageID page_id, bool exclusive)
    {
        auto segment_page_id = page_id ^ (static_cast<uint64_t>(segment_id) << 48);
        auto frame_it = id_to_frame.find(segment_page_id);
        // Page already buffered?
        if (frame_it != id_to_frame.end())
            return *(frame_it->second);

        // Load page into buffer
        auto &frame = get_free_frame();
        id_to_frame[segment_page_id] = &frame;
        frame.load_page(segment_id, page_id, page_size);

        return frame;
    }
    // -----------------------------------------------------------------
    void BufferManager::unfix_page(BufferFrame &frame, bool is_dirty)
    {
        if (is_dirty)
            frame.state = State::DIRTY;
    }
    // ----------------------------------------------------------------
    BufferFrame &BufferManager::get_free_frame()
    {
        // Buffer not full yet
        if (!free_buffer_frames.empty())
        {
            auto &frame = *(free_buffer_frames.back());
            free_buffer_frames.pop_back();
            return frame;
        }

        // TODO: Evict a random page when buffer is full.
        throw buffer_full_error();
    }
    // ------------------------------------------------------------------
} // namespace bbbtree