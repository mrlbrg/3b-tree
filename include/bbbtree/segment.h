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

   class FSISegment : public Segment
   {
   public:
      /// Find a free page
      std::optional<uint64_t> find(uint32_t required_space);
   };

   class SPSegment : public Segment
   {
      /// Free space inventory
      FSISegment &fsi;
   };
}

#endif // INCLUDE_BBBTREE_SEGMENT_H_