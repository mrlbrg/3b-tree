#ifndef INCLUDE_BBBTREE_SEGMENT_H_
#define INCLUDE_BBBTREE_SEGMENT_H_

#include "bbbtree/buffer_manager.h"
#include "bbbtree/tuple_id.h"
#include "bbbtree/slotted_page.h"

#include <optional>

namespace bbbtree
{

   /// A segment manages a collection of corresponding pages, e.g. a collection of slotted pages.
   /// It currently maps to a single file.
   class Segment
   {
   public:
      /// Constructor.
      /// @param[in] segment_id       ID of the segment.
      /// @param[in] buffer_manager   The buffer manager that should be used by the segment.
      Segment(SegmentID segment_id, BufferManager &buffer_manager)
          : segment_id(segment_id), buffer_manager(buffer_manager) {}

      /// The segment id
      SegmentID segment_id;

   protected:
      /// The buffer manager
      BufferManager &buffer_manager;
   };

   /// Encodes the free space left in a slotted page segment. Tracks the amount of existing slotted pages.
   /// TODO: For now, we append-only. Therefore we merely track the
   ///       last slotted page's space and the total amount of allocated pages.
   /// TODO: For now, we do not persist the FSI. We always start from an empty FSI on start-up.
   class FSISegment : public Segment
   {
   public:
      /// Constructor. TODO: When segment (file) was not allocated so far, set the header of the first page to 0.
      FSISegment(SegmentID segment_id, BufferManager &buffer_manager) : Segment(segment_id, buffer_manager) {}
      /// Find a free page. Optionally returns the page ID.
      std::optional<uint64_t> find(uint32_t required_space);
      /// Updates the amount of free space on the target page. Updated by the Slotted Pages Segment.
      void update(uint64_t target_page, uint32_t free_space);
      /// Creates a new page's inventory. Returns the new page's ID.
      PageID create_new_page(uint32_t initial_free_space);

   private:
      /// The number of allocated slotted pages so far.
      size_t allocated_pages = 0;
      /// The free space on the last slotted page.
      size_t free_space = 0;
   };

   /// A segment (here equivalent to a file) containing all the slotted pages.
   class SPSegment : public Segment
   {
   public:
      /// Constructor. The segment_id is also the name of the file containing the pages.
      SPSegment(SegmentID segment_id, BufferManager &buffer_manager, FSISegment &fsi) : Segment(segment_id, buffer_manager), space_inventory(fsi) {}

      /// Uses the free-space inventory to allocate space for a new tuple on a page.
      TID allocate(uint32_t size);
      /// Reads data of the tuple into the buffer.
      uint32_t read(TID tid, std::byte *record, uint32_t capacity) const;
      /// Writes a tuple into the given tuple id.
      /// Only allows writes of the exact size allocated for this tuple id.
      uint32_t write(TID tid, std::byte *record, uint32_t record_size);
      /// TODO: Resizes a tuple.
      void resize(TID tid, uint32_t new_length);
      /// TODO: Erases a tuple from the slotted page.
      void erase(TID tid);

   private:
      // Cast a Page to a SlottedPage
      static SlottedPage &get_slotted_page(BufferFrame &buffer_frame) { return *(reinterpret_cast<SlottedPage *>(buffer_frame.get_data())); }

      /// Free space inventory
      FSISegment &space_inventory;
   };
}

#endif // INCLUDE_BBBTREE_SEGMENT_H_