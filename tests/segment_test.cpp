#include "bbbtree/segment.h"
#include "bbbtree/buffer_manager.h"

#include <gtest/gtest.h>
#include <cstring>

namespace
{
    TEST(FSISegment, Empty)
    {
        bbbtree::BufferManager buffer_manager{1024, 10};
        bbbtree::FSISegment fsi{0, buffer_manager};

        // Empty FSI cannot find/update anything.
        {
            EXPECT_FALSE(fsi.find(1).has_value());
            EXPECT_THROW(fsi.update(0, 1), std::logic_error);
        }

        // Can find/update page after creating one.
        {
            EXPECT_EQ(fsi.create_new_page(buffer_manager.get_page_size()), 0);
            auto page_id = fsi.find(1024);
            EXPECT_TRUE(page_id.has_value());
            EXPECT_EQ(page_id.value(), 0);
            fsi.update(0, 0);

            EXPECT_FALSE(fsi.find(1).has_value());
        }

        // Can append to last page only.
        {
            EXPECT_EQ(fsi.create_new_page(buffer_manager.get_page_size()), 1);
            EXPECT_EQ(fsi.find(10).value(), 1);
            // Cannot udpate non-last page
            EXPECT_THROW(fsi.update(0, 0), std::logic_error);
            // Cannot update to more space
            EXPECT_THROW(fsi.update(1, 1025), std::logic_error);
        }
    }

    TEST(FSISegment, Persistency)
    {
        // TODO: Destroy FSI segment and create one again. The data should be persisted.
    }

    TEST(SPSegment, Allocate)
    {
        bbbtree::BufferManager buffer_manager{1024, 10};
        bbbtree::FSISegment fsi{0, buffer_manager};
        bbbtree::SPSegment sp_segment{1, buffer_manager, fsi};

        // Allocate tuple of size bigger than page size
        EXPECT_THROW(sp_segment.allocate(1024), std::logic_error);

        // Allocate tuples spanning multiple pages
        auto tid1 = sp_segment.allocate(1000);
        auto tid2 = sp_segment.allocate(1000);
        EXPECT_GT(tid2.get_page_id(), tid1.get_page_id());
    }
    TEST(SPSegment, WriteRead)
    {
        bbbtree::BufferManager buffer_manager{1024, 10};
        bbbtree::FSISegment fsi{0, buffer_manager};
        bbbtree::SPSegment sp_segment{1, buffer_manager, fsi};

        // Allocate
        auto tid = sp_segment.allocate(sizeof(uint64_t));

        // Write
        uint64_t value = 923485;
        sp_segment.write(tid, reinterpret_cast<std::byte *>(&value), sizeof(value));

        // Read
        std::vector<std::byte> buffer{sizeof(value)};
        auto bytes_read = sp_segment.read(tid, &buffer[0], buffer.size());
        EXPECT_EQ(bytes_read, sizeof(uint64_t));
        EXPECT_EQ(*(reinterpret_cast<uint64_t *>(&buffer[0])), value);
    }
    TEST(SPSegment, WriteDifferentSize)
    {
        bbbtree::BufferManager buffer_manager{1024, 10};
        bbbtree::FSISegment fsi{0, buffer_manager};
        bbbtree::SPSegment sp_segment{1, buffer_manager, fsi};

        // Allocate
        auto tid = sp_segment.allocate(sizeof(uint64_t));

        // Write
        uint64_t value = 923485;
        EXPECT_THROW(sp_segment.write(tid, reinterpret_cast<std::byte *>(&value), 1), std::logic_error);
        EXPECT_THROW(sp_segment.write(tid, reinterpret_cast<std::byte *>(&value), sizeof(value) + 1), std::logic_error);
    }

    TEST(SPSegment, Persistency)
    {
        // TODO: Destroy SPSegment and create one again. The data should be persisted.
    }
}