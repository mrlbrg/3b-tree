// -----------------------------------------------------------------
#include "bbbtree/buffer_manager.h"
#include "bbbtree/logger.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"
// -----------------------------------------------------------------
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
// -----------------------------------------------------------------
namespace bbbtree {
// -----------------------------------------------------------------
BufferManager::BufferManager(size_t page_size, size_t page_count, bool clear)
	: page_size(page_size), clear(clear) {
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
	for (auto data = page_data.begin(); data < page_data.end();
		 data += page_size) {
		page_frames.emplace_back(&(*data));
		free_buffer_frames.push_back(&(page_frames.back()));
	}

	stats.page_size = page_size;
	stats.num_pages = page_count;
}
// -----------------------------------------------------------------
BufferManager::~BufferManager() { clear_all(); }
// ----------------------------------------------------------------
void BufferManager::reset(BufferFrame &frame) {
	assert(!frame.in_use_by);
	frame.state = State::UNDEFINED;
	frame.page_logic = nullptr;
}
// ----------------------------------------------------------------
bool BufferManager::unload(BufferFrame &frame) {
	// Sanity Check: Caller must ensure that page needs to be unloaded.
	assert(frame.state == State::DIRTY || frame.state == State::NEW);

	auto [success, continue_unload] =
		frame.page_logic
			? frame.page_logic->before_unload(frame.data, frame.state,
											  frame.page_id, page_size)
			: std::make_pair(true, true);

	if (!success)
		return false; // Unload is not allowed for this frame. Probably because
					  // the delta tree is locked currently.

	if (!continue_unload)
		return true; // Unload is not continued.

	size_t page_begin = frame.page_id * page_size;
	size_t page_end = page_begin + page_size;
	auto &file = get_segment(frame.segment_id);
	// TODO: Resizing is not thread safe. Must lock the whole file before
	// doing so.
	if (file.size() < page_end)
		// Sets new bytes to 0
		file.resize(page_end);
	// TODO: Make sure everything was written out by getting bytes.
	file.write_block(frame.data, page_begin, page_size);
	stats.bytes_written_physically += page_size;
	stats.pages_written += 1;

	return true;
}
// -----------------------------------------------------------------
void BufferManager::load(BufferFrame &frame, SegmentID segment_id,
						 PageID page_id) {
	frame.segment_id = segment_id;
	frame.page_id = page_id;
	frame.state = State::CLEAN;

	size_t page_begin = page_id * page_size;
	size_t page_end = page_begin + page_size;

	// Page is new. Resize file on write out.
	auto &file = get_segment(segment_id);
	if (file.size() < page_end) {
		// Set the page to new to make sure its written to disk later.
		frame.state = State::NEW;
		return;
	}

	// TODO: Throw an error when not enough was read/written.
	file.read_block(page_begin, page_size, frame.data);
	stats.pages_loaded++;

	if (frame.page_logic)
		// Call the page logic after loading.
		frame.page_logic->after_load(frame.data, frame.page_id);
}
// -----------------------------------------------------------------
BufferFrame &BufferManager::fix_page(SegmentID segment_id, PageID page_id,
									 bool /*exclusive*/,
									 PageLogic *page_logic) {
#ifndef NDEBUG
	logger.log("Fixing page " + std::to_string(segment_id) + "." +
			   std::to_string(page_id) + " {");
	++logger;
#endif
	// Sanity Check
	assert((page_id & 0xFFFF000000000000ULL) == 0);

	auto segment_page_id = page_id ^ (static_cast<uint64_t>(segment_id) << 48);
	auto frame_it = id_to_frame.find(segment_page_id);
	// Page already buffered?
	if (frame_it != id_to_frame.end()) {
		//  TODO: Already used by someone else?
		auto &frame = frame_it->second;
		++(frame->in_use_by);
#ifndef NDEBUG
		logger.log("Page already in buffer.");
		--logger;
		logger.log("}");
#endif
		return *(frame_it->second);
	}

	// Load page into buffer
	auto &frame = get_free_frame();
	assert(frame.in_use_by == 0);
	assert(frame.page_logic == nullptr);
	id_to_frame[segment_page_id] = &frame;
	frame.in_use_by = 1;
	frame.page_logic = page_logic;

#ifndef NDEBUG
	logger.log("Loading page into buffer.");
#endif
	load(frame, segment_id, page_id);
#ifndef NDEBUG
	logger.log(*this);
#endif

	assert(validate());

#ifndef NDEBUG
	--logger;
	logger.log("}");
#endif

	return frame;
}
// -----------------------------------------------------------------
void BufferManager::unfix_page(BufferFrame &frame, bool is_dirty) {
	// TODO: Check if is_dirty, lock must have been exclusive.
	assert(frame.in_use_by > 0);

	if (is_dirty)
		frame.set_dirty(); // Does not overwrite NEW state.

	--frame.in_use_by;
}
// ----------------------------------------------------------------
bool BufferManager::remove(BufferFrame &frame, bool write_back) {
	// Sanity Check: Must not be in use.
	assert(frame.in_use_by == 0);
	if (write_back) {
		if (frame.state == State::DIRTY || frame.state == State::NEW) {
			frame.in_use_by = 1; // Prevent recursive eviction.
			auto success = unload(frame);
			frame.in_use_by = 0;
			if (!success)
				return false;
		}
	}
	// Remove from directory.
	auto segment_page_id =
		frame.page_id ^ (static_cast<uint64_t>(frame.segment_id) << 48);
	auto num_removed = id_to_frame.erase(segment_page_id);
	assert(num_removed == 1);
	// Release frame.
	reset(frame);
	free_buffer_frames.push_back(&frame);
	++stats.pages_evicted;

	return true;
}
// ----------------------------------------------------------------
bool BufferManager::evict() {
#ifndef NDEBUG
	logger.log("Buffer full, evicting a page...");
	logger.log(*this);
#endif
	assert(validate());
	// Select random page for eviction.
	size_t i = std::rand() % page_frames.size();
	auto *frame = &(page_frames[i]);
	uint8_t num_frames_tested = 0;

	// Find a page that is not in use from there.
	while (true) {
		if (!frame->in_use_by) {
#ifndef NDEBUG
			logger.log("Evicting page " + std::to_string(frame->segment_id) +
					   "." + std::to_string(frame->page_id));
			logger.log(*frame);
#endif

			// Try to remove the page.
			auto success = remove(*frame);
			if (success)
				break;
#ifndef NDEBUG
			logger.log("Could not evict page because unload was not allowed by "
					   "page logic.");
#endif
		}

		// Stop when having scanned all frames already.
		++num_frames_tested;
		if (num_frames_tested == page_frames.size())
			return false;

		// Otherwise try next page.
		i = ((i + 1) < page_frames.size()) ? (i + 1) : 0;
		frame = &(page_frames[i]);
	}

	return true;
}
// ----------------------------------------------------------------
BufferFrame &BufferManager::get_free_frame() {
	// TODO: Synchronize when multi-threading.

	// Buffer full?
	if (free_buffer_frames.empty()) {
		auto success = evict();
		if (!success) {
			throw buffer_full_error();
		}
	}

	// Sanity check
	assert(validate());
	assert(!free_buffer_frames.empty());

	auto &frame = *(free_buffer_frames.back());
	free_buffer_frames.pop_back();

	return frame;
}
// ------------------------------------------------------------------
File &BufferManager::get_segment(SegmentID segment_id) {
	auto it = segment_to_file.find(segment_id);

	// File open.
	if (it != segment_to_file.end())
		return *(it->second);

	// Open/create file if not present yet.
	auto file_name = std::to_string(segment_id);
	auto [new_it, success] = segment_to_file.emplace(
		segment_id, File::open_file(file_name.data(), File::Mode::WRITE));

	// Reset file?
	if (clear)
		new_it->second->resize(0);

	return *(new_it->second);
}
// ------------------------------------------------------------------
void BufferManager::clear_all(bool write_back) {
	if (!write_back) {
		segment_to_file.clear();
		clear = true;
		// Setting to clear makes sure that files are reset when
		// reopening them.
	}
restart:
	for (const auto &[page_id, frame] : id_to_frame) {
		auto success = remove(*frame, write_back);
		// assert(success);
	}
	// During `unload` of BTree nodes, some pages might have been loaded
	// into the buffer to store the deltas. Therefore we might have to go
	// another round to also clear all delta tree pages from the buffer.
	if (!id_to_frame.empty())
		goto restart;
	assert(free_buffer_frames.size() == page_frames.size());
}
// ------------------------------------------------------------------
bool BufferManager::validate() const {

	// Check that the number of free frames and used frames adds up to the
	// total number of frames.
	if (free_buffer_frames.size() + id_to_frame.size() != page_frames.size()) {
		// logger.log("Validating BufferManager...");
		// logger.log("Inconsistent state: free_buffer_frames.size() + "
		// 		   "id_to_frame.size() != page_frames.size()");
		// logger.log(*this);
		return false;
	}

	// Check that all frames in mapping table are defined and have the
	// correct segment_id and page_id as the frames their mapping to.
	for (auto [page_id, frame_ptr] : id_to_frame) {
		if (frame_ptr->state == State::UNDEFINED) {
			// logger.log("Validating BufferManager...");
			// logger.log("Inconsistent state: page " + std::to_string(page_id)
			// +
			//    " is in id_to_frame but UNDEFINED");
			// logger.log(*this);
			return false;
		}
		SegmentID segment_id = static_cast<SegmentID>(page_id >> 48);
		PageID pid = page_id & 0x0000FFFFFFFFFFFFULL;
		if (frame_ptr->segment_id != segment_id || frame_ptr->page_id != pid) {
			// logger.log("Validating BufferManager...");
			// logger.log("Inconsistent state: page " +
			// 		   std::to_string(segment_id) + "." + std::to_string(pid) +
			// 		   " has wrong segment_id or page_id");
			// logger.log(*this);
			return false;
		}
	}
	return true;
}
// ------------------------------------------------------------------
} // namespace bbbtree