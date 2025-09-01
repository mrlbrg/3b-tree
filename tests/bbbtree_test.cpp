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
static std::vector<std::byte> get_random_bytes(size_t num_bytes) {
	static std::random_device rd;  // Seed
	static std::mt19937 gen(rd()); // Mersenne Twister engine
	static std::uniform_int_distribution<uint8_t> dist(65, 90);

	std::vector<std::byte> res;
	res.resize(num_bytes);
	for (auto &byte : res)
		byte = std::byte(dist(gen));

	return res;
}
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

	// TODO: Create a superficial BTree node.
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
TEST_F(BBBTreeTest, LeafSplitsInDeltaTree) {
	size_t page_size = TEST_PAGE_SIZE;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<BBBTreeInt> bbbtree_int =
		std::make_unique<BBBTreeInt>(TEST_SEGMENT_ID, *buffer_manager);

	const size_t tuples_per_leaf =
		(page_size - sizeof(BTreeInt::LeafNode)) /
		(sizeof(UInt64) + sizeof(TID) + sizeof(BTreeInt::LeafNode::Slot));

	// Fill up the single node.
	size_t i = 1;
	for (; i < tuples_per_leaf; i++) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	}
	// Force node to disk.
	buffer_manager->clear_all();

	// Split the node.
	auto node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		++i;
	}
	buffer_manager->clear_all();

	// Loaded into memory, the BTree is complete because deltas are being
	// applied.
	for (size_t j = 1; j < i; j++) {
		EXPECT_TRUE(bbbtree_int->lookup(j).has_value());
		EXPECT_EQ(bbbtree_int->lookup(j), j + 2);
	}
	buffer_manager->clear_all();

	// Check nodes' state on disk.
	{
		TestPageLogic<false> non_applying_page_logic;
		auto &frame1 = buffer_manager->fix_page(TEST_SEGMENT_ID, 1, true,
												&non_applying_page_logic);
		auto *node1 = reinterpret_cast<BTreeInt::LeafNode *>(frame1.get_data());
		// Node 1 does not have its node split on disk. So all keys are still
		// present.
		EXPECT_TRUE(node1->slot_count == 4);
		EXPECT_TRUE(node1->lookup(UInt64{1}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{2}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{3}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{4}).has_value());
		buffer_manager->unfix_page(frame1, false);

		auto &frame2 = buffer_manager->fix_page(TEST_SEGMENT_ID, 2, true,
												&non_applying_page_logic);
		auto *node2 = reinterpret_cast<BTreeInt::LeafNode *>(frame2.get_data());
		// Node 2 was created newly so all its inserted keys are also on disk.
		EXPECT_FALSE(node2->lookup(UInt64{1}).has_value());
		EXPECT_FALSE(node2->lookup(UInt64{2}).has_value());
		EXPECT_TRUE(node2->lookup(UInt64{3}).has_value());
		EXPECT_TRUE(node2->lookup(UInt64{4}).has_value());
		buffer_manager->unfix_page(frame2, false);
	}
}
// -----------------------------------------------------------------
// Inserts following a split are handled by the delta tree.
TEST_F(BBBTreeTest, SplitAndInserts) {
	class TestBBBTree : public BBBTreeInt {
	  public:
		TestBBBTree(SegmentID segment_id, BufferManager &buffer_manager)
			: BBBTreeInt(segment_id, buffer_manager) {}

		DeltaTreeInt *get_delta_tree() { return &(this->delta_tree); }
	};

	size_t page_size = TEST_PAGE_SIZE;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<TestBBBTree> bbbtree_int =
		std::make_unique<TestBBBTree>(TEST_SEGMENT_ID, *buffer_manager);

	size_t tuples_per_leaf =
		(page_size - sizeof(BTreeInt::LeafNode)) /
		(sizeof(UInt64) + sizeof(TID) + sizeof(BTreeInt::LeafNode::Slot));

	// Fill up the single node.
	size_t i = 1;
	for (; i < tuples_per_leaf; i++) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	}
	// Force node to disk.
	buffer_manager->clear_all();

	// Split the node.
	auto node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		++i;
	}

	// Insert into the left node again.
	EXPECT_TRUE(bbbtree_int->insert(0, 2));
	EXPECT_TRUE(bbbtree_int->lookup(0).has_value());
	EXPECT_EQ(bbbtree_int->lookup(0), 2);
	buffer_manager->clear_all();

	// Inserted value exists in memory due to applying deltas from the delta
	// tree.
	EXPECT_TRUE(bbbtree_int->lookup(0).has_value());
	buffer_manager->clear_all();

	// Check node state in memory: Inserted value and split exist in memory.
	{
		auto &frame1 = buffer_manager->fix_page(TEST_SEGMENT_ID, 1, true,
												bbbtree_int->get_delta_tree());
		auto *node1 = reinterpret_cast<BTreeInt::LeafNode *>(frame1.get_data());
		EXPECT_TRUE(node1->slot_count == 3);
		EXPECT_TRUE(node1->lookup(UInt64{0}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{1}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{2}).has_value());
		EXPECT_FALSE(node1->lookup(UInt64{3}).has_value());
		EXPECT_FALSE(node1->lookup(UInt64{4}).has_value());
		buffer_manager->unfix_page(frame1, false);
		buffer_manager->clear_all();
	}

	// Check node state on disk: Inserted value and split does not exist on
	// disk.
	{
		TestPageLogic<false> non_applying_page_logic;
		auto &frame1 = buffer_manager->fix_page(TEST_SEGMENT_ID, 1, true,
												&non_applying_page_logic);
		auto *node1 = reinterpret_cast<BTreeInt::LeafNode *>(frame1.get_data());
		// Node 1 does not have its node split on disk. So all keys are still
		// present.
		EXPECT_TRUE(node1->slot_count == 4);
		EXPECT_FALSE(node1->lookup(UInt64{0}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{1}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{2}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{3}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{4}).has_value());
		buffer_manager->unfix_page(frame1, false);
	}
}
// -----------------------------------------------------------------
// An insert of a new slot and an update to upper is extracted and applied
// correctly.
TEST_F(BBBTreeTest, InnerNodeInsert) {
	class TestBBBTree : public BBBTreeInt {
	  public:
		TestBBBTree(SegmentID segment_id, BufferManager &buffer_manager)
			: BBBTreeInt(segment_id, buffer_manager) {}

		DeltaTreeInt *get_delta_tree() { return &(this->delta_tree); }

		BTreeInt *get_btree() { return &this->btree; }
	};

	size_t page_size = TEST_PAGE_SIZE;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<TestBBBTree> bbbtree_int =
		std::make_unique<TestBBBTree>(TEST_SEGMENT_ID, *buffer_manager);

	// Create a tree of three nodes and write all out.
	size_t i = 0;
	auto node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		++i;
	}
	EXPECT_EQ(bbbtree_int->get_btree()->height(), size_t(2));
	buffer_manager->clear_all();

	// Force another split. Four nodes in tree. Three leafs.
	node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		++i;
	}
	buffer_manager->clear_all();

	// In memory, all three leaf nodes are found.
	for (size_t j = 0; j < i; ++j) {
		EXPECT_TRUE(bbbtree_int->lookup(j).has_value());
		EXPECT_EQ(bbbtree_int->lookup(j), j + 2);
	}
	buffer_manager->clear_all();

	// On disk: Root does not have the newly inserted node. Upper is outdated.
	{
		TestPageLogic<false> non_applying_page_logic;
		auto &frame = buffer_manager->fix_page(TEST_SEGMENT_ID, 3, true,
											   &non_applying_page_logic);
		auto *node = reinterpret_cast<BTreeInt::InnerNode *>(frame.get_data());

		EXPECT_EQ(node->slot_count, 1);
		EXPECT_EQ(node->upper, PageID(2));
		buffer_manager->unfix_page(frame, false);
	}
}
// -----------------------------------------------------------------
// A split child results in an insert of a new slot in the parent and an update
// of the split slot in the parent (if not the upper child was split). Those
// must be tracked correctly.
TEST_F(BBBTreeTest, InnerNodeUpdate) {
	class TestBBBTree : public BBBTreeInt {
	  public:
		TestBBBTree(SegmentID segment_id, BufferManager &buffer_manager)
			: BBBTreeInt(segment_id, buffer_manager) {}

		DeltaTreeInt *get_delta_tree() { return &(this->delta_tree); }

		BTreeInt *get_btree() { return &this->btree; }
	};

	size_t page_size = 128;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<TestBBBTree> bbbtree_int =
		std::make_unique<TestBBBTree>(TEST_SEGMENT_ID, *buffer_manager);

	// Create a tree of three nodes and write all out.
	uint64_t start = 245'321;
	uint64_t i = start;
	auto node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		--i;
	}
	EXPECT_EQ(bbbtree_int->get_btree()->height(), size_t(2));
	buffer_manager->clear_all();

	// Force another split on the left-most node to make sure we're not
	// splitting the `upper` child of the root.
	node_splits_before = stats.leaf_node_splits;
	while (stats.leaf_node_splits == node_splits_before) {
		EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
		EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
		EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
		--i;
	}
	buffer_manager->clear_all();

	// In memory, all keys are found.
	for (size_t j = start; j > i; --j) {
		EXPECT_TRUE(bbbtree_int->lookup(j).has_value());
		EXPECT_EQ(bbbtree_int->lookup(j), j + 2);
	}
	buffer_manager->clear_all();

	// On disk: Root does not have the newly inserted node. Upper is outdated.
	{
		TestPageLogic<false> non_applying_page_logic;
		auto &frame = buffer_manager->fix_page(TEST_SEGMENT_ID, 3, true,
											   &non_applying_page_logic);
		auto *node = reinterpret_cast<BTreeInt::InnerNode *>(frame.get_data());

		EXPECT_EQ(node->upper, PageID(2));
		// The third leaf does not exist.
		EXPECT_EQ(node->slot_count, 1);
		EXPECT_EQ(node->lookup(i), PageID(1));
		buffer_manager->unfix_page(frame, false);
	}
}
// -----------------------------------------------------------------
// A node that is in the delta tree, can become dirtier.
TEST_F(BBBTreeTest, UpdatingDeltaTreeEntries) {
	class TestBBBTree : public BBBTreeInt {
	  public:
		TestBBBTree(SegmentID segment_id, BufferManager &buffer_manager)
			: BBBTreeInt(segment_id, buffer_manager) {}

		DeltaTreeInt *get_delta_tree() { return &(this->delta_tree); }

		BTreeInt *get_btree() { return &this->btree; }
	};

	size_t page_size = 128;
	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	std::unique_ptr<TestBBBTree> bbbtree_int =
		std::make_unique<TestBBBTree>(TEST_SEGMENT_ID, *buffer_manager);

	uint64_t i = 0;

	// Write out the new node.
	EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
	EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
	EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	++i;
	buffer_manager->clear_all();

	// Dirty the node and buffer in delta tree.
	EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
	EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
	EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	++i;
	buffer_manager->clear_all();

	// Dirtier the node and buffer in delta tree. Should update the entry for
	// the node to both deltas.
	EXPECT_TRUE(bbbtree_int->insert(i, i + 2));
	EXPECT_TRUE(bbbtree_int->lookup(i).has_value());
	EXPECT_EQ(bbbtree_int->lookup(i), i + 2);
	++i;
	buffer_manager->clear_all();

	// In memory, all keys are found.
	for (size_t j = 0; j < i; ++j) {
		EXPECT_TRUE(bbbtree_int->lookup(j).has_value());
		EXPECT_EQ(bbbtree_int->lookup(j), j + 2);
	}
	buffer_manager->clear_all();

	// Check node state in memory: Inserted value and split exist in memory.
	{
		auto &frame1 = buffer_manager->fix_page(TEST_SEGMENT_ID, 1, true,
												bbbtree_int->get_delta_tree());
		EXPECT_TRUE(frame1.is_clean());
		auto *node1 = reinterpret_cast<BTreeInt::LeafNode *>(frame1.get_data());
		EXPECT_EQ(node1->slot_count, 3);
		EXPECT_TRUE(node1->lookup(UInt64{0}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{1}).has_value());
		EXPECT_TRUE(node1->lookup(UInt64{2}).has_value());
		buffer_manager->unfix_page(frame1, false);
		buffer_manager->clear_all();
	}
	buffer_manager->clear_all();

	// Check node state on disk: Inserted value and split does not exist on
	// disk.
	{
		TestPageLogic<false> non_applying_page_logic;
		auto &frame1 = buffer_manager->fix_page(TEST_SEGMENT_ID, 1, true,
												&non_applying_page_logic);
		auto *node1 = reinterpret_cast<BTreeInt::LeafNode *>(frame1.get_data());
		// Node 1 does not have the last two entries.
		EXPECT_TRUE(node1->slot_count == 1);
		EXPECT_TRUE(node1->lookup(UInt64{0}).has_value());
		EXPECT_FALSE(node1->lookup(UInt64{1}).has_value());
		EXPECT_FALSE(node1->lookup(UInt64{2}).has_value());
		buffer_manager->unfix_page(frame1, false);
	}
}
// ----------------------------------------------------------------
// A node is cleaned of all its slot states when actually being written out.
TEST_F(BBBTreeTest, PageIsActuallyWrittenOut) {}
// ----------------------------------------------------------------
/// A BBB-Tree that can be seeded with randomly generated tuples. Validated
/// against a B-Tree.
template <template <typename, typename, bool = false> typename IndexT,
		  KeyIndexable KeyT, ValueIndexable ValueT, size_t PageSize>
struct SeedableTree : public IndexT<KeyT, ValueT> {

	static const constexpr size_t SPACE_ON_LEAF =
		PageSize - BTreeInt::LeafNode::min_space;
	static const constexpr size_t SPACE_ON_NODE =
		PageSize - BTreeInt::InnerNode::min_space;

	/// Constructor.
	SeedableTree(SegmentID segment_id, BufferManager &buffer_manager)
		: BBBTree<KeyT, ValueT>(segment_id, buffer_manager) {
		stats.clear();
	}
	/// Destructor.
	~SeedableTree() {
		std::cout << stats << std::endl;
		stats.clear();
	}

	/// Seed the tree with random key/value pairs.
	/// @insert_size: the number of bytes to be inserted in total.
	/// @min_size: the minimum number of bytes an inserted key should have.
	void seed(size_t insert_size, size_t min_size = 1) {
		auto get_kv_size = [min_size]() -> std::pair<uint16_t, uint16_t> {
			static std::random_device rd;
			static std::mt19937 gen(rd());
			uint16_t key_size, value_size;
			auto const constexpr max_size =
				std::min(SPACE_ON_LEAF, SPACE_ON_NODE);
			assert(min_size < max_size);
			// Get Key Size.
			if constexpr (std::is_same_v<KeyT, UInt64>) {
				key_size = sizeof(KeyT);
			} else {
				std::uniform_int_distribution<uint16_t> dist(min_size,
															 max_size - 1);
				key_size = dist(gen);
			}
			// Get Value Size.
			if constexpr (std::is_same_v<ValueT, UInt64>) {
				value_size = sizeof(ValueT);
			} else if constexpr (std::is_same_v<ValueT, TID>) {
				value_size = sizeof(ValueT);
			} else {
				std::uniform_int_distribution<uint16_t> dist(1, max_size -
																	key_size);
				value_size = dist(gen);
			}

			return {key_size, value_size};
		};

		size_t num_bytes = 0;
		while (num_bytes < insert_size) {
			// Get random key and value.
			auto [key_size, value_size] = get_kv_size();
			data.push_back(
				{get_random_bytes(key_size), get_random_bytes(value_size)});
			auto &[key_data, value_data] = data.back();

			KeyT key = KeyT::deserialize(&key_data[0], key_data.size());
			ValueT value =
				ValueT::deserialize(&value_data[0], value_data.size());

			// Insert into tree.
			if (expected_map.count(key) == 0) {
				bool success = this->insert(key, value);
				assert(success);
				expected_map[key] = value;
				assert(expected_map.size() == this->size());
				num_bytes += key_size + value_size;
				assert(this->height() < PageSize);
			} else {
				assert(this->insert(key, value) == false);
				data.pop_back();
			}
		}
	}

	/// Validates that all previously `seed`ed key/value pairs are still
	/// present in the tree.
	/// @return: true if the tree contains all expected key/value pairs.
	/// false otherwise.
	bool validate() {
		// Validate size
		if (this->size() != expected_map.size())
			return false;

		// Validate stored values.
		for (const auto &[key, value] : expected_map) {
			auto res = this->lookup(key);
			if (!res.has_value())
				return false;
			if (res.value() != value)
				return false;
		}

		return true;
	}

	using KeyData = std::vector<std::byte>;
	using ValueData = std::vector<std::byte>;

	/// The buffer that owns the Key/Value data. Simulates page data.
	std::vector<std::pair<KeyData, ValueData>> data;
	/// The key/value pairs that are expected to live in the tree.
	std::unordered_map<KeyT, ValueT> expected_map;
};
// A large tree can handle all kinds of deltas.
TEST_F(BBBTreeTest, LargeIntTree) {
	static const constexpr size_t page_size = 4096;

	std::unique_ptr<BufferManager> buffer_manager =
		std::make_unique<BufferManager>(page_size, TEST_NUM_PAGES, true);
	SeedableTree<BBBTree, UInt64, TID, page_size> tree{TEST_SEGMENT_ID,
													   *buffer_manager};

	tree.seed(1'000);
	EXPECT_TRUE(tree.validate());
}
// ----------------------------------------------------------------
} // namespace