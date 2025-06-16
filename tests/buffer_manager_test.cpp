#include "bbbtree/buffer_manager.h"

#include <gtest/gtest.h>
#include <cstring>

namespace
{

    /// A page can be fixed.
    TEST(BufferManager, WriteBufferedPage)
    {
        size_t page_size = 1024;
        bbbtree::BufferManager buffer_manager{page_size, 10};
        std::vector<uint64_t> expected_values(page_size / sizeof(uint64_t), 123);

        // Write to page
        {
            auto &page = buffer_manager.fix_page(348, 1, true);
            ASSERT_TRUE(page.get_data());
            std::memcpy(page.get_data(), expected_values.data(), page_size);
            buffer_manager.unfix_page(page, true);
        }
        // Read from page
        {
            std::vector<uint64_t> values(page_size / sizeof(uint64_t));
            auto &page = buffer_manager.fix_page(348, 1, false);
            std::memcpy(values.data(), page.get_data(), page_size);
            buffer_manager.unfix_page(page, true);
            ASSERT_EQ(expected_values, values);
        }
    }
    /// A page can be fixed also when the buffer is full.
    TEST(BufferManager, EvictPage)
    {
        size_t page_size = 1024;
        bbbtree::BufferManager buffer_manager{page_size, 1};

        auto &page = buffer_manager.fix_page(348, 1, true);

        // Cannot evict a page when its fixed.
        EXPECT_THROW(buffer_manager.fix_page(348, 2, true), bbbtree::buffer_full_error);

        // Can evict a page when its not fixed.
        buffer_manager.unfix_page(page, false);
        EXPECT_NO_THROW(buffer_manager.fix_page(248, 2, true));
    }
    // TODO: Fill in tests.
    /// A page can be fixed exclusively. Someone else cannot fix that page.
    TEST(BufferManager, ExclusiveFlag) {}
    /// A page does not loose state on eviction.
    TEST(BufferManager, PersistentEviction) {}
    /// When the buffer manager is destroyed, all pages are persisted.
    TEST(BufferManager, PersistentRestart) {}
    /// When the buffer is full and all pages are exclusively locked, an error is thrown.
    TEST(BufferManager, BufferFull) {}

}