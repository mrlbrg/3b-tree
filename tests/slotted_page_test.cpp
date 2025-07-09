#include "bbbtree/slotted_page.h"

#include <gtest/gtest.h>
#include <vector>

/// Slotted Page is initialized correctly.
TEST(SlottedPage, Constructor) {
	static constexpr uint32_t page_size = 1024;
	std::vector<std::byte> buffer;
	buffer.resize(page_size);
	auto *page = new (&buffer[0]) bbbtree::SlottedPage(page_size);

	// Check Header.
	EXPECT_EQ(page->header.slot_count, 0);
	EXPECT_EQ(page->header.data_start, page_size);

	// Check rest of the page is zero'd out.
	for (auto *data = page->get_data() + sizeof(bbbtree::SlottedPage);
		 data < page->get_data() + page_size; ++data) {
		EXPECT_EQ(*(reinterpret_cast<uint8_t *>(data)), 0);
	}
}

/// Slotted Page throws an error when full.
TEST(SlottedPage, PageFull) {

	static constexpr uint32_t page_size = 1024;
	std::vector<std::byte> buffer;
	buffer.resize(page_size);
	auto *page = new (&buffer[0]) bbbtree::SlottedPage(page_size);

	// Allocating space exceeding page space should throw and not allocate
	// anything.
	EXPECT_THROW(page->allocate(page_size, page_size), std::logic_error);
	EXPECT_EQ(page->header.slot_count, 0);
	EXPECT_EQ(page->header.data_start, page_size);
}

/// Slotted Page allocates space.
TEST(SlottedPage, Allocate) {
	static constexpr uint32_t page_size = 1024;
	std::vector<std::byte> buffer;
	buffer.resize(page_size);
	auto *page = new (&buffer[0]) bbbtree::SlottedPage(page_size);

	// Allocating 1 byte
	EXPECT_NO_THROW(page->allocate(1, page_size));
	EXPECT_EQ(page->header.slot_count, 1);
	EXPECT_EQ(page->header.data_start, page_size - 1);

	// Allocating the rest of the page
	EXPECT_NO_THROW(page->allocate(page_size - sizeof(bbbtree::SlottedPage) -
									   sizeof(bbbtree::SlottedPage::Slot) * 2 -
									   1,
								   page_size));
	EXPECT_EQ(page->header.slot_count, 2);
	EXPECT_EQ(page->header.data_start,
			  sizeof(bbbtree::SlottedPage) +
				  sizeof(bbbtree::SlottedPage::Slot) * 2);
	EXPECT_EQ(page->get_free_space(), 0);

	/// Allocating one more should throw, because the page is full.
	EXPECT_THROW(page->allocate(1, page_size), std::logic_error);
}

/// Slotted Page erases space.
TEST(SlottedPage, Erase) {
	static constexpr uint32_t page_size = 1024;
	std::vector<std::byte> buffer;
	buffer.resize(page_size);
	auto *page = new (&buffer[0]) bbbtree::SlottedPage(page_size);

	// Allocate
	auto slot_id = page->allocate(1, 1024);
	EXPECT_EQ(page->header.slot_count, 1);
	page->allocate(1, 1024);
	EXPECT_EQ(page->header.slot_count, 2);
	page->allocate(1, 1024);
	EXPECT_EQ(page->header.slot_count, 3);

	// Erase
	page->erase(slot_id);
	EXPECT_EQ(page->header.slot_count, 3);

	// Erase non-existing slot ID
	EXPECT_THROW(page->erase(52345), std::logic_error);
	EXPECT_EQ(page->header.slot_count, 3);
}