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
            EXPECT_EQ(fsi.create_new_page(), 0);
            auto page_id = fsi.find(1024);
            EXPECT_TRUE(page_id.has_value());
            EXPECT_EQ(page_id.value(), 0);
            fsi.update(0, 0);

            EXPECT_FALSE(fsi.find(1).has_value());
        }

        // Can append to last page only.
        {
            EXPECT_EQ(fsi.create_new_page(), 1);
            EXPECT_EQ(fsi.find(10).value(), 1);
            // Cannot udpate non-last page
            EXPECT_THROW(fsi.update(0, 0), std::logic_error);
            // Cannot update to more space
            EXPECT_THROW(fsi.update(1, 1025), std::logic_error);
        }
    }
}