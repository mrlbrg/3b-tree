#pragma once

#include "bbbtree/file.h"
#include "bbbtree/types.h"

#include <exception>
#include <map>
#include <unordered_map>
#include <vector>

namespace bbbtree {
enum class State {
	UNDEFINED, // Not owning a particular page.
	CLEAN,	   // Owns an unchanged page.
	DIRTY	   // Owns a changed page or a new page.
};

class BufferFrame {
  private:
	/// The segment's ID.
	SegmentID segment_id;
	/// The page's ID within its segment.
	PageID page_id;
	/// The data of the page this frame maintains.
	/// The pointer remains constant.
	/// The pages are swapped though, overwriting the data.
	char *const data;
	/// The state of the page. Undefined by default.
	State state = State::UNDEFINED;
	/// How many use the page currently.
	size_t in_use_by = 0;

	friend class BufferManager;

  public:
	/// Constructor.
	BufferFrame() = delete;
	/// Constructor.
	explicit BufferFrame(char *const data) : data(data) {}

	/// Returns a pointer to this page's data.
	char *get_data() const { return data; }
	/// Set frame as dirty.
	void set_dirty() { state = State::DIRTY; }
};

class buffer_full_error : public std::exception {
  public:
	[[nodiscard]] const char *what() const noexcept override {
		return "buffer is full";
	}
};

/// Manages all pages in memory.
/// Transparently swaps pages between storage and memory when buffer becomes
/// full.
/// TODO: For now, it runs in a single thread.
class BufferManager {
  public:
	/// Constructor.
	/// @param[in] page_size  Size in bytes that all pages will have.
	/// @param[in] page_count Maximum number of pages that should reside in
	/// memory at most.
	/// @param[in] clear Resets all files before loading.
	explicit BufferManager(size_t page_size, size_t page_count,
						   bool clear = false);
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

	/// The size of each page in the buffer.
	const size_t page_size;

  private:
	/// Gets a free buffer frame. Evicts another page when buffer is full.
	BufferFrame &get_free_frame();
	/// Evicts a page from the buffer. Assumes that no frame is free. Returns if
	/// a page was evicte successfully. Eviction could fail e.g. when all pages
	/// are currently fixed.
	bool evict();
	/// Write a page to disk.
	void write();

	/// Get the segment's file from a segment ID.
	/// Opens/creates the file if not present yet.
	File &get_segment(SegmentID segment_id);
	/// Loads a page into a frame.
	void load(BufferFrame &frame, SegmentID segment_id, PageID page_id);
	/// Unloads a frame's page do disk.
	void unload(BufferFrame &frame);
	/// Resets a frame.
	inline void reset(BufferFrame &frame);

	/// The pages' data.
	std::vector<char> page_data;
	/// The pages' frames.
	std::vector<BufferFrame> page_frames;
	/// Maps page IDs (including segment ID) to the corrensponding pages.
	std::unordered_map<PageID, BufferFrame *> id_to_frame;
	// Tracks pointers to unused BufferFrames.
	std::vector<BufferFrame *> free_buffer_frames;
	// Maps a Segment to its corresponding file. We use a `map` for pointer
	// stability.
	std::map<SegmentID, std::unique_ptr<File>> segment_to_file;
	// Whether a file is reset before loaded.
	bool clear;
};
} // namespace bbbtree
