#pragma once
// -----------------------------------------------------------------
#include "bbbtree/file.h"
#include "bbbtree/types.h"
// -----------------------------------------------------------------
#include <exception>
#include <map>
#include <sstream>
#include <unordered_map>
#include <vector>
// -----------------------------------------------------------------
namespace bbbtree {
// -----------------------------------------------------------------
/// The state of a page in the buffer manager.
enum class State {
	UNDEFINED, // Not owning a particular page.
	CLEAN,	   // Owns an unchanged page.
	DIRTY,	   // Owns a changed page or a new page.
	NEW,	   // A dirty state which must be written to disk on eviction. Never
			   // overwrite NEW state.
};
// -----------------------------------------------------------------
class BufferFrame;
// -----------------------------------------------------------------
/// User-specific page logic that is called when a page is loaded or unloaded by
/// the buffer manager.
class PageLogic {
  public:
	/// The function to call before a dirty page is unloaded.
	/// Is not called when the page is new.
	/// Returns true when the unload should proceed to disk.
	virtual std::pair<bool, bool> before_unload(char *data, const State &state,
												PageID page_id,
												size_t page_size) = 0;
	/// The function to call after the page was loaded from disk.
	virtual void after_load(char *data, PageID page_id) = 0;
	/// Virtual destructor.
	virtual ~PageLogic() = default;
};
// -----------------------------------------------------------------
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
	/// The page logic that is called when the page is (un)loaded.
	PageLogic *page_logic = nullptr;

	friend class BufferManager;

  public:
	/// Constructor.
	BufferFrame() = delete;
	/// Constructor.
	explicit BufferFrame(char *const data) : data(data) {}

	/// Returns a pointer to this page's data.
	inline char *get_data() const { return data; }
	/// Returns the page ID.
	inline PageID get_page_id() const { return page_id; }
	// /// Returns the segment ID.
	inline SegmentID get_segment_id() const { return segment_id; }
	/// Returns true if frame is dirty.
	inline bool is_dirty() const { return state == State::DIRTY; }
	/// Returns true if frame is new.
	inline bool is_new() const { return state == State::NEW; }
	/// Returns true if frame is clean.
	inline bool is_clean() const { return state == State::CLEAN; }
	/// Returns true if frame is defined.
	inline bool is_defined() const { return state != State::UNDEFINED; }
	/// Set frame as dirty.
	inline void set_dirty() {
		if (state != State::NEW)
			state = State::DIRTY;
	}
	/// Sets frame as clean.
	inline void set_clean() {
		assert(is_dirty());
		state = State::CLEAN;
	}
	/// Returns how many users the page currently has.
	inline size_t get_in_use_by() const { return in_use_by; }

	friend std::ostream &operator<<(std::ostream &os, const BufferFrame &frame);
	/// Converts the frame to a string.
	operator std::string() const {
		std::stringstream ss;
		ss << *this;
		return ss.str();
	}
};
// -----------------------------------------------------------------
class buffer_full_error : public std::exception {
  public:
	[[nodiscard]] const char *what() const noexcept override {
		return "buffer is full";
	}
};
// -----------------------------------------------------------------
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
	/// If given, page_logic is stored in the frame and called after
	/// loading/before unloading the page again.
	BufferFrame &fix_page(SegmentID segment_id, PageID page_id, bool exclusive,
						  PageLogic *page_logic);

	/// Releases a page. If dirty, its written to disk eventually.
	void unfix_page(BufferFrame &frame, bool is_dirty);

	/// Clears the buffer.
	/// If write_back is true, all dirty pages are written to disk first.
	/// Otherwise, all data is lost, e.g. for benchmarking.
	void clear_all(bool write_back = true);
	/// The size of each page in the buffer.
	const size_t page_size;

	// Print operator.
	friend std::ostream &operator<<(std::ostream &os, const BufferManager &bm);
	/// Converts the bm to a string.
	operator std::string() const {
		std::stringstream ss;
		ss << *this;
		return ss.str();
	}

  private:
	/// Gets a free buffer frame. Evicts another page when buffer is full. TODO:
	/// Not thread-safe.
	BufferFrame &get_free_frame();
	/// Evicts a page from the buffer. Assumes that no frame is free. Returns
	/// true if a page was evicted successfully. Eviction could fail e.g. when
	/// all pages are currently fixed. TODO: Not thread-safe.
	bool evict();
	/// Write a page to disk.
	void write();

	/// Get the segment's file from a segment ID.
	/// Opens/creates the file if not present yet.
	File &get_segment(SegmentID segment_id);
	/// Loads a page into a frame.
	void load(BufferFrame &frame, SegmentID segment_id, PageID page_id);
	/// Unloads a frame's page do disk.
	/// Returns false when unloading was not allowed by the page logic.
	bool unload(BufferFrame &frame);
	/// Resets a frame.
	void reset(BufferFrame &frame);
	/// Removes a frame from the buffer and frees up the space. Potentially
	/// writes page to disk.
	/// Returns false if frame could not be removed because unload was not
	/// allowed by page logic.
	bool remove(BufferFrame &frame, bool write_back = true);
	/// Validates the internal state of the buffer manager.
	bool validate() const;

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
// -----------------------------------------------------------------
inline std::ostream &operator<<(std::ostream &os, const BufferFrame &frame) {
	os << "SegmentID: " << frame.segment_id;
	os << ", PageID: " << frame.page_id;
	os << ", State: ";
	switch (frame.state) {
	case State::UNDEFINED:
		os << "UNDEFINED";
		break;
	case State::CLEAN:
		os << "CLEAN";
		break;
	case State::DIRTY:
		os << "DIRTY";
		break;
	case State::NEW:
		os << "NEW";
		break;
	}
	os << ", InUseBy: " << frame.in_use_by;

	return os;
}
// -----------------------------------------------------------------
inline std::ostream &operator<<(std::ostream &os, const BufferManager &bm) {
	os << "BufferManager: page_size=" << bm.page_size
	   << ", page_count=" << bm.page_frames.size()
	   << ", free_frames=" << bm.free_buffer_frames.size() << "\n";
	os << "Buffer Pool:\n";
	for (const auto &[segment_page_id, frame_ptr] : bm.id_to_frame) {
		assert(frame_ptr->is_defined());
		SegmentID segment_id = static_cast<SegmentID>(segment_page_id >> 48);
		PageID page_id = segment_page_id & 0x0000FFFFFFFFFFFFULL;
		os << "[" << segment_id << "." << page_id
		   << "]:" << frame_ptr - bm.page_frames.data();
		os << " -> " << *frame_ptr << "\n";
	}
	return os;
}
// -----------------------------------------------------------------
} // namespace bbbtree
