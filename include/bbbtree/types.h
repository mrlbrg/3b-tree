#ifndef INCLUDE_BBBTREE_TYPES_H_
#define INCLUDE_BBBTREE_TYPES_H_

#include <cstdint>

namespace bbbtree
{
    using SegmentID = uint16_t;
    using PageID = uint64_t;
    using SlotID = uint16_t;
}

#endif // INCLUDE_BBBTREE_TYPES_H_