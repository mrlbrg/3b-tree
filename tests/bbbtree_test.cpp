#include "bbbtree/bbbtree.h"
#include <gtest/gtest.h>

using namespace bbbtree;

namespace {
// -----------------------------------------------------------------
class BBBTreeTest : public ::testing::Test {};
// -----------------------------------------------------------------
/// When a node of the BBBTree is evicted, its deltas are stored in the delta
/// tree and not persisted on disk.
/// TODO: Later only when write amplification was high.
TEST_F(BBBTreeTest, EvictNode) {}
/// When a node of the BBBTree is loaded, its deltas are loaded from the delta
/// tree and applied to the node.
TEST_F(BBBTreeTest, LoadNode) {}
// -----------------------------------------------------------------
} // namespace