// -----------------------------------------------------------------
#include "bbbtree/buffer_manager.h"
// -----------------------------------------------------------------
#include <cstring>
#include <cassert>
#include <cstdlib>
// -----------------------------------------------------------------
namespace bbbtree
{
    // ----------------------------------------------------------------
    void BufferManager::reset(BufferFrame &frame)
    {
        frame.state = State::UNDEFINED;
        assert(frame.in_use == false);
    }
    // ----------------------------------------------------------------
    void BufferManager::unload(BufferFrame &frame)
    {
        switch (frame.state)
        {
        case State::UNDEFINED:
        case State::CLEAN:
        case State::DIRTY:
            size_t page_begin = frame.page_id * page_size;
            size_t page_end = page_begin + page_size;
            auto &file = get_segment(frame.segment_id);
            if (file.size() < page_end)
                file.resize(page_end);
            // TODO: Make sure everything was written out.
            file.write_block(frame.data, page_begin, page_size);
        }
    }
    // -----------------------------------------------------------------
    void BufferManager::load(BufferFrame &frame, SegmentID segment_id, PageID page_id)
    {
        frame.segment_id = segment_id;
        frame.page_id = page_id;
        frame.state = State::CLEAN;

        size_t page_begin = page_id * page_size;
        size_t page_end = page_begin + page_size;

        // Page is new. Resize file on write out.
        auto &file = get_segment(segment_id);
        if (file.size() < page_end)
        {
            // Set the page to dirty to make sure its written to disk later.
            frame.state = State::DIRTY;
            return;
        }

        // TODO: Throw an error when not enough was read/written.
        file.read_block(page_begin, page_size, frame.data);
    }
    // -----------------------------------------------------------------
    BufferManager::BufferManager(size_t page_size, size_t page_count) : page_size(page_size)
    {
        // Sanity checks
        assert(page_count > 0);
        assert(page_size > 0);

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
        for (auto &frame : page_frames)
        {
            // TODO: Must also be written when page is new.
            if (frame.state == State::DIRTY)
                unload(frame);
        }
    }
    // -----------------------------------------------------------------
    BufferFrame &BufferManager::fix_page(SegmentID segment_id, PageID page_id, bool exclusive)
    {
        auto segment_page_id = page_id ^ (static_cast<uint64_t>(segment_id) << 48);
        auto frame_it = id_to_frame.find(segment_page_id);
        // Page already buffered?
        if (frame_it != id_to_frame.end())
        {
            auto &frame = frame_it->second;
            frame->in_use = true;
            return *(frame_it->second);
        }

        // Load page into buffer
        auto &frame = get_free_frame();
        id_to_frame[segment_page_id] = &frame;
        load(frame, segment_id, page_id);
        frame.in_use = true;

        return frame;
    }
    // -----------------------------------------------------------------
    void BufferManager::unfix_page(BufferFrame &frame, bool is_dirty)
    {
        // Sanity check
        assert(frame.in_use);

        if (is_dirty)
            frame.state = State::DIRTY;
        frame.in_use = false;
    }
    // ----------------------------------------------------------------
    bool BufferManager::evict()
    {
        // Select random page for eviction.
        int i = std::rand() % page_frames.size();
        auto *frame = &(page_frames[i]);
        uint8_t num_frames_tested = 0;

        // Find a page that is not in use from there.
        while (frame->in_use)
        {
            // Stop when having scanned all frames already.
            ++num_frames_tested;
            if (num_frames_tested == page_frames.size())
                return false;

            // Otherwise try next page.
            i = (i + 1) % page_frames.size();
            frame = &(page_frames[i]);
        }

        // Remove frame from directory
        auto segment_page_id = frame->page_id ^ (static_cast<uint64_t>(frame->segment_id) << 48);
        id_to_frame.erase(segment_page_id);

        // TODO: Must also be written when page is new.
        // Write dirty pages out.
        if (frame->state == State::DIRTY)
            unload(*frame);

        // Free the frame from ownership.
        assert(frame->in_use == false);
        frame->state = State::UNDEFINED;
        free_buffer_frames.emplace_back(frame);

        return true;
    }
    // ----------------------------------------------------------------
    BufferFrame &BufferManager::get_free_frame()
    {
        // TODO: Synchronize when multi-threading.

        // Buffer full?
        if (free_buffer_frames.empty())
        {
            auto success = evict();
            if (!success)
                throw buffer_full_error();
        }

        // Sanity check
        assert(!free_buffer_frames.empty());

        auto &frame = *(free_buffer_frames.back());
        free_buffer_frames.pop_back();

        return frame;
    }
    // ------------------------------------------------------------------
    File &BufferManager::get_segment(SegmentID segment_id)
    {
        auto it = segment_to_file.find(segment_id);

        // File open.
        if (it != segment_to_file.end())
            return *(it->second);

        // Open/create file if not present yet.
        auto file_name = std::to_string(segment_id);
        auto [new_it, success] = segment_to_file.emplace(segment_id, File::open_file(file_name.data(), File::Mode::WRITE));

        return *(new_it->second);
    }
    // ------------------------------------------------------------------
} // namespace bbbtree