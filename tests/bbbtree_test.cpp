#include "bbbtree/bbbtree.h"
#include "bbbtree/buffer_manager.h"
#include "bbbtree/stats.h"
#include "bbbtree/types.h"

#include <gtest/gtest.h>
#include <memory>
#include <random>

using namespace bbbtree;

namespace {
// -----------------------------------------------------------------
using BBBTreeInt = BBBTree<UInt64, TID>;
using BTreeInt = BTree<UInt64, TID, true>;
using DeltaTreeInt = DeltaTree<UInt64, TID>;
// -----------------------------------------------------------------
static const constexpr SegmentID TEST_SEGMENT_ID = 834;
static const constexpr size_t TEST_PAGE_SIZE = 128;
static const constexpr size_t TEST_NUM_PAGES = 5;
// -----------------------------------------------------------------
/// A shared buffer manager for all tests.
class BBBTreeTest : public ::testing::Test {};
// -----------------------------------------------------------------
// A dummy page logic that is used to test the callbacks of the buffer manager.
template <bool ContinueUnload> class TestPageLogic : public PageLogic {
  public:
	bool before_unload(char * /*data*/, const State & /*state*/,
					   PageID /*page_id*/) override {
		unload_called = true;
		return ContinueUnload;
	}
	void after_load(char * /*data*/, PageID /*page_id*/) override {
		load_called = true;
	}

	bool unload_called = false;
	bool load_called = false;
};
// -----------------------------------------------------------------
/// Tests that the buffer manager calls the page logic on unload and load.
TEST_F(BBBTreeTest, BufferManagerCallsBack) {
	SegmentID segment_id = 0;
	PageID page_id = 0;

	TestPageLogic<true> page_logic;
	BufferManager buffer_manager(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);

	// Create a new page. Does not call load.
	{
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, &page_logic);
		buffer_manager.unfix_page(frame, true);
		EXPECT_FALSE(page_logic.load_called);
		EXPECT_FALSE(page_logic.unload_called);
	}

	// Write page out. Calls unload.
	{
		buffer_manager.clear_all();
		EXPECT_TRUE(page_logic.unload_called);
		EXPECT_FALSE(page_logic.load_called);
	}

	// Load page again. Calls load.
	{
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, &page_logic);
		buffer_manager.unfix_page(frame, true);
		EXPECT_TRUE(page_logic.unload_called);
		EXPECT_TRUE(page_logic.load_called);
	}
}
// -----------------------------------------------------------------
/// Tests that a page is persisted when page logic returns true on unload.
TEST_F(BBBTreeTest, BufferManagerContinuesUnload) {
	SegmentID segment_id = 0;
	PageID page_id = 0;

	TestPageLogic<true> persisting_page_logic;
	BufferManager buffer_manager(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);

	std::vector<char> initial_data(TEST_PAGE_SIZE, 'C');
	std::vector<char> data(TEST_PAGE_SIZE, 'A');

	{
		// Write some initial data to page and write it out.
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, nullptr);
		std::memcpy(frame.get_data(), initial_data.data(), TEST_PAGE_SIZE);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(initial_data.data(), initial_data.size()));
		buffer_manager.unfix_page(frame, true);
		buffer_manager.clear_all();
	}
	{
		// Test that initial data is still there.
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, nullptr);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(initial_data.data(), initial_data.size()));
		buffer_manager.unfix_page(frame, false);
		buffer_manager.clear_all();
	}
	// Write some data to page and write it out.
	{
		auto &frame = buffer_manager.fix_page(segment_id, page_id, true,
											  &persisting_page_logic);
		EXPECT_NE(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		std::memcpy(frame.get_data(), data.data(), TEST_PAGE_SIZE);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		buffer_manager.unfix_page(frame, true);
		// Write out all pages. Should persist the page.
		buffer_manager.clear_all();
	}
	// Test that the data is persisted.
	{
		auto &frame = buffer_manager.fix_page(segment_id, page_id, true,
											  &persisting_page_logic);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		buffer_manager.unfix_page(frame, true);
	}
}
// -----------------------------------------------------------------
/// Tests that a page is not persisted when page logic returns false on unload.
TEST_F(BBBTreeTest, BufferManagerStopsUnload) {
	SegmentID segment_id = 0;
	PageID page_id = 0;

	TestPageLogic<false> non_persisting_page_logic;
	BufferManager buffer_manager(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);

	std::vector<char> initial_data(TEST_PAGE_SIZE, 'C');
	std::vector<char> data(TEST_PAGE_SIZE, 'B');

	{
		// Write some initial data to page and write it out.
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, nullptr);
		std::memcpy(frame.get_data(), initial_data.data(), TEST_PAGE_SIZE);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(initial_data.data(), initial_data.size()));
		buffer_manager.unfix_page(frame, true);
		buffer_manager.clear_all();
	}
	{
		// Test that initial data is still there.
		auto &frame =
			buffer_manager.fix_page(segment_id, page_id, true, nullptr);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(initial_data.data(), initial_data.size()));
		buffer_manager.unfix_page(frame, false);
		buffer_manager.clear_all();
	}
	{
		// Write some new data to page and write it out. Should not be persisted
		// due to page logic.
		auto &frame = buffer_manager.fix_page(segment_id, page_id, true,
											  &non_persisting_page_logic);
		EXPECT_NE(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		std::memcpy(frame.get_data(), data.data(), TEST_PAGE_SIZE);
		EXPECT_EQ(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		buffer_manager.unfix_page(frame, true);
		buffer_manager.clear_all();
	}

	{
		// Test that the data is not persisted.
		auto &frame = buffer_manager.fix_page(segment_id, page_id, true,
											  &non_persisting_page_logic);
		EXPECT_NE(std::string_view(frame.get_data(), TEST_PAGE_SIZE),
				  std::string_view(data.data(), data.size()));
		buffer_manager.unfix_page(frame, false);
	}
}
// -----------------------------------------------------------------
TEST_F(BBBTreeTest, DeltaTree) {
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);
	std::unique_ptr<DeltaTreeInt> delta_tree =
		std::make_unique<DeltaTreeInt>(TEST_SEGMENT_ID, *buffer_manager);

	// Create a superficial BTree node.
	// When the state is new, all slots should be cleaned.
	// When the state is dirty, all deltas should be stored in the delta tree.
	// When calling `after_load`, the deltas should be applied to the node.
	// After applying deltas, the page's state is still clean.
}
// -----------------------------------------------------------------
/// When a leaf of the BBBTree is evicted, its deltas are stored in the delta
/// tree and not on disk.
/// TODO: Later only when write amplification was high.
TEST_F(BBBTreeTest, BufferLeafDeltasInDeltaTree) {
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(TEST_PAGE_SIZE, TEST_NUM_PAGES, true);
	std::unique_ptr<BBBTreeInt> bbbtree_int =
		std::make_unique<BBBTreeInt>(TEST_SEGMENT_ID, *buffer_manager);

	// Initialize a new node in the BTree with a key.
	EXPECT_TRUE(bbbtree_int->insert(1, 2));
	EXPECT_TRUE(bbbtree_int->lookup(1).has_value());
	EXPECT_EQ(bbbtree_int->lookup(1), 2);
	buffer_manager->clear_all();

	{
		// Verify that the delta is on disk using another btree
		// that has no real page logic to apply on load.
		TestPageLogic<false> non_applying_page_logic;
		BTreeInt btree_int{TEST_SEGMENT_ID, *buffer_manager,
						   &non_applying_page_logic};
		EXPECT_TRUE(btree_int.lookup(1).has_value());
		EXPECT_EQ(btree_int.lookup(1), 2);
		buffer_manager->clear_all();
	}

	// Create a delta on the node.
	EXPECT_TRUE(bbbtree_int->insert(2, 2));
	EXPECT_TRUE(bbbtree_int->insert(3, 2));
	EXPECT_TRUE(bbbtree_int->lookup(2).has_value());
	EXPECT_TRUE(bbbtree_int->lookup(3).has_value());
	EXPECT_EQ(bbbtree_int->lookup(2), 2);
	EXPECT_EQ(bbbtree_int->lookup(3), 2);

	// Evict the BTree. Should trigger the eviction handler `before_unload` and
	// discard the page.
	buffer_manager->clear_all();

	{
		// Verify that the delta is not on disk.
		// Using another btree that has no real page logic to apply on load.
		TestPageLogic<false> non_applying_page_logic;
		BTreeInt btree_int{TEST_SEGMENT_ID, *buffer_manager,
						   &non_applying_page_logic};
		EXPECT_FALSE(btree_int.lookup(2).has_value());
		EXPECT_FALSE(btree_int.lookup(3).has_value());
		buffer_manager->clear_all();
	}

	// When loading the node again, the deltas should be applied.
	EXPECT_TRUE(bbbtree_int->lookup(2).has_value());
	EXPECT_TRUE(bbbtree_int->lookup(3).has_value());

	// Destroy the Buffer Manager first, as some frames reference the
	// BBBTree.
	buffer_manager.reset();
	bbbtree_int.reset();
}
// -----------------------------------------------------------------
/// Load a node from the disk.
TEST_F(BBBTreeTest, NodeSplitsInDeltaTree) {
	size_t page_size = TEST_PAGE_SIZE;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<BBBTreeInt> bbbtree_int =
		std::make_unique<BBBTreeInt>(TEST_SEGMENT_ID, *buffer_manager);
	std::unordered_map<UInt64, TID> expected_map{};

	size_t tuples_per_leaf = page_size / (sizeof(UInt64) + sizeof(TID) + 12);

	// Fill up the single node.
	size_t i = 1;
	for (; i <= tuples_per_leaf; i++) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	}
	// Force node to disk.
	buffer_manager->clear_all();

	// Split the node.
	auto node_splits_before = stats.inner_node_splits;
	EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
	EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
	EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	auto node_splits_after = stats.inner_node_splits;
	EXPECT_TRUE(node_splits_before < node_splits_after);

	bbbtree_int->print();
	buffer_manager->clear_all();

	// Loaded into memory, the BTree is complete because deltas are being
	// applied.
	for (size_t j = 1; j <= i; j++) {
		EXPECT_TRUE(bbbtree_int->insert(j, j + 2));
		EXPECT_TRUE(bbbtree_int->lookup(j).has_value());
		EXPECT_EQ(bbbtree_int->lookup(j), j + 2);
	}

	{
		// Verify that the left node's changes are not on disk.
		// Using another btree that has no real page logic to apply on load.
		TestPageLogic<false> non_applying_page_logic;
		BTreeInt btree_int{TEST_SEGMENT_ID, *buffer_manager,
						   &non_applying_page_logic};
		EXPECT_TRUE(btree_int.lookup(0).has_value());
		EXPECT_FALSE(btree_int.lookup(1).has_value());
		buffer_manager->clear_all();
	}
	// Check that left node and new root is written out.
	// Right node is not written out but applied correctly.
}
// TODO: When we extract deltas and apply them again, the page remains in clean
// state. When no changes add on, the page is discarded on eviction and deltas
// applied at load. When changes add on, we delete the deltas from the tree and
// add new ones. That means we need to keep the slot state when applying deltas.
// When we actually write out the page, we clean all slots of tracking state.
// -----------------------------------------------------------------
} // namespace