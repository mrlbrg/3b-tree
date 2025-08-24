// -----------------------------------------------------------------
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"
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
void BufferManager::unload(BufferFrame &frame) {
	// Sanity Check: Caller must ensure that page needs to be unloaded.
	assert(frame.state == State::DIRTY || frame.state == State::NEW);

	auto continue_unload = frame.page_logic
							   ? frame.page_logic->before_unload(
									 frame.data, frame.state, frame.page_id)
							   : true;

	if (!continue_unload)
		return; // Unload is cancelled.

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

	if (frame.page_logic)
		// Call the page logic after loading.
		frame.page_logic->after_load(frame.data, frame.page_id);
}
// -----------------------------------------------------------------
BufferFrame &BufferManager::fix_page(SegmentID segment_id, PageID page_id,
									 bool /*exclusive*/,
									 PageLogic *page_logic) {
	// Sanity Check
	assert((page_id & 0xFFFF000000000000ULL) == 0);

	auto segment_page_id = page_id ^ (static_cast<uint64_t>(segment_id) << 48);
	auto frame_it = id_to_frame.find(segment_page_id);
	// Page already buffered?
	if (frame_it != id_to_frame.end()) {
		// TODO: Already used by someone else?
		auto &frame = frame_it->second;
		++(frame->in_use_by);
		return *(frame_it->second);
	}

	// Load page into buffer
	auto &frame = get_free_frame();
	assert(frame.in_use_by == 0);
	id_to_frame[segment_page_id] = &frame;
	frame.in_use_by = 1;
	frame.page_logic = page_logic;
	load(frame, segment_id, page_id);

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
void BufferManager::remove(BufferFrame &frame) {
	// Sanity Check: Must not be in use.
	assert(frame.in_use_by == 0);
	auto segment_page_id =
		frame.page_id ^ (static_cast<uint64_t>(frame.segment_id) << 48);
	// Write dirty pages out.
	if (frame.state == State::DIRTY || frame.state == State::NEW)
		unload(frame);
	// Remove from directory.
	id_to_frame.erase(segment_page_id);
	// Release frame.
	reset(frame);
	free_buffer_frames.push_back(&frame);
	++stats.pages_evicted;
}
// ----------------------------------------------------------------
bool BufferManager::evict() {
	// Select random page for eviction.
	int i = std::rand() % page_frames.size();
	auto *frame = &(page_frames[i]);
	uint8_t num_frames_tested = 0;

	// Find a page that is not in use from there.
	while (frame->in_use_by) {
		// Stop when having scanned all frames already.
		++num_frames_tested;
		if (num_frames_tested == page_frames.size())
			return false;

		// Otherwise try next page.
		i = (i + 1) % page_frames.size();
		frame = &(page_frames[i]);
	}

	remove(*frame);

	return true;
}
// ----------------------------------------------------------------
BufferFrame &BufferManager::get_free_frame() {
	// TODO: Synchronize when multi-threading.

	// Buffer full?
	if (free_buffer_frames.empty()) {
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
void BufferManager::clear_all() {
restart:
	free_buffer_frames.clear();
	for (auto &frame : page_frames)
		remove(frame);
	// During `unload` of BTree nodes, some pages might have been loaded
	// into the buffer to store the deltas. Therefore we might have to go
	// another round to also clear all delta tree pages from the buffer.
	if (free_buffer_frames.size() != page_frames.size())
		goto restart;
	assert(id_to_frame.empty());
}
// ------------------------------------------------------------------
} // namespace bbbtree