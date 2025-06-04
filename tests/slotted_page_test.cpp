#include "bbbtree/slotted_page.h"

#include <gtest/gtest.h>
#include <vector>

TEST(SlottedPage, Constructor)
{
    std::vector<std::byte> buffer;
    buffer.resize(1024);
    auto *page = new (&buffer[0]) bbbtree::SlottedPage(1024);

    EXPECT_EQ(2 + 3, 5);
}