#include "bbbtree/btree.h"

#include <gtest/gtest.h>

namespace
{

    /// A Tree is bootstrapped correctly at initialization.
    TEST(BTree, Startup)
    {
        size_t page_size = 1024;
        bbbtree::BufferManager buffer_manager{page_size, 10};
        bbbtree::BTree<uint64_t, uint64_t> index{834, buffer_manager};

        EXPECT_THROW(index.lookup(59834), std::logic_error);
    }
    /// A Tree is picked up from storage when it exists already.
    TEST(BTree, Pickup) {}
    /// A key and value can be retrieved from the tree after insertion.
    TEST(BTree, Get) {}
    /// A B-Tree can split its nodes.
    TEST(BTree, NodeSplit) {}
}