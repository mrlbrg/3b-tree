#ifndef INCLUDE_BBBTREE_SEGMENT_H_
#define INCLUDE_BBBTREE_SEGMENT_H_

#include "bbbtree/buffer_manager.h"

#include <optional>

namespace bbbtree
{

   class Segment
   {
   public:
      /// Constructor.
      /// @param[in] segment_id       ID of the segment.
      /// @param[in] buffer_manager   The buffer manager that should be used by the segment.
      Segment(uint16_t segment_id, BufferManager &buffer_manager)
          : segment_id(segment_id), buffer_manager(buffer_manager) {}

      /// The segment id
      uint16_t segment_id;

   protected:
      /// The buffer manager
      BufferManager &buffer_manager;
   };

   /// Encodes the free space left in all slotted pages. Tracks the amount of existing slotte pages.
   /// TODO:   For now, we append-only. Therefore we merely track the
   ///         last slotted page's space and the total amount of allocated pages
   /// TODO: For now, we do not persist the FSI. We always start from an empty FSI on start-up.
   class FSISegment : public Segment
   {
   public:
      /// Constructor. TODO: When segment (file) was not allocated so far, set the header of the first page to 0.
      FSISegment(uint16_t segment_id, BufferManager &buffer_manager) : Segment(segment_id, buffer_manager) {}
      /// Find a free page. Optionally returns the page ID.
      std::optional<uint64_t> find(uint32_t required_space);
      /// Updates the amount of free space on the target page. Updated by the Slotted Pages Segment.
      void update(uint64_t target_page, uint32_t free_space);
      /// Creates a new page's inventory. Returns the new page's ID.
      size_t create_new_page();

   private:
      /// The number of allocated slotted pages so far.
      size_t allocated_pages = 0;
      /// The free space on the last slotted page.
      size_t free_space = 0;
   };

   class SPSegment : public Segment
   {
      /// Free space inventory
      FSISegment &fsi;
   };
}

#endif // INCLUDE_BBBTREE_SEGMENT_H_