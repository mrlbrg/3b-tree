#include "bbbtree/buffer_manager.h"
#include "bbbtree/segment.h"

#include <cstring>
#include <gtest/gtest.h>

namespace {
static const constexpr size_t FSI_SEGMENT_ID = 534;
static const constexpr size_t SP_SEGMENT_ID = 321;

class SegmentTest : public ::testing::Test {
  protected:
	void Destroy(bool clear) {
		sp_segment.reset();
		fsi.reset();
		buffer_manager.reset();

		buffer_manager =
			std::make_unique<bbbtree::BufferManager>(1024, 10, clear);
		fsi = std::make_unique<bbbtree::FSISegment>(FSI_SEGMENT_ID,
													*buffer_manager);
		sp_segment = std::make_unique<bbbtree::SPSegment>(
			SP_SEGMENT_ID, *buffer_manager, *fsi);
	}

	// Runs *before* each TEST_F
	void SetUp() override { Destroy(true); }

	// Runs *after* each TEST_F
	void TearDown() override {}

	std::unique_ptr<bbbtree::BufferManager> buffer_manager;
	std::unique_ptr<bbbtree::FSISegment> fsi;
	std::unique_ptr<bbbtree::SPSegment> sp_segment;
};

TEST_F(SegmentTest, Empty) {
	// Empty FSI cannot find/update anything.
	{
		EXPECT_FALSE(fsi->find(1).has_value());
		EXPECT_THROW(fsi->update(0, 1), std::logic_error);
	}

	// Can find/update page after creating one.
	{
		EXPECT_EQ(fsi->create_new_page(buffer_manager->page_size), (size_t)0);
		auto page_id = fsi->find(1024);
		EXPECT_TRUE(page_id.has_value());
		EXPECT_EQ(page_id.value(), 0);
		fsi->update(0, 0);

		EXPECT_FALSE(fsi->find(1).has_value());
	}

	// Can append to last page only.
	{
		EXPECT_EQ(fsi->create_new_page(buffer_manager->page_size), 1);
		EXPECT_EQ(fsi->find(10).value(), 1);
		// Cannot udpate non-last page
		EXPECT_THROW(fsi->update(0, 0), std::logic_error);
		// Cannot update to more space
		EXPECT_THROW(fsi->update(1, 1025), std::logic_error);
	}
}

TEST_F(SegmentTest, Persistency) {
	// TODO: Destroy FSI segment and create one again. The data should be
	// persisted.
	fsi->create_new_page(buffer_manager->page_size);
	Destroy(false);

	EXPECT_TRUE(fsi->find(buffer_manager->page_size / 2));
}

TEST_F(SegmentTest, Allocate) {

	// Allocate tuple of size bigger than page size
	EXPECT_THROW(sp_segment->allocate(1024), std::logic_error);

	// Allocate tuples spanning multiple pages
	auto tid1 = sp_segment->allocate(1000);
	auto tid2 = sp_segment->allocate(1000);
	EXPECT_GT(tid2.get_page_id(), tid1.get_page_id());
}

TEST_F(SegmentTest, WriteRead) {

	// Allocate
	auto tid = sp_segment->allocate(sizeof(uint64_t));

	// Write
	uint64_t value = 923485;
	sp_segment->write(tid, reinterpret_cast<std::byte *>(&value),
					  sizeof(value));

	// Read
	std::vector<std::byte> buffer{sizeof(value)};
	auto bytes_read = sp_segment->read(tid, &buffer[0], buffer.size());
	EXPECT_EQ(bytes_read, sizeof(uint64_t));
	EXPECT_EQ(*(reinterpret_cast<uint64_t *>(&buffer[0])), value);
}

TEST_F(SegmentTest, WriteDifferentSize) {
	// Allocate
	auto tid = sp_segment->allocate(sizeof(uint64_t));

	// Write
	uint64_t value = 923485;
	EXPECT_THROW(
		sp_segment->write(tid, reinterpret_cast<std::byte *>(&value), 1),
		std::logic_error);
	EXPECT_THROW(sp_segment->write(tid, reinterpret_cast<std::byte *>(&value),
								   sizeof(value) + 1),
				 std::logic_error);
}

TEST_F(SegmentTest, SPPersistency) {
	// TODO: Destroy SPSegment and create one again. The data should be
	// persisted.
}
} // namespace