#include "bbbtree/buffer_manager.h"

#include <cstring>
#include <gtest/gtest.h>

namespace {

/// A page can be fixed.
TEST(BufferManager, WriteBufferedPage) {
	size_t page_size = 1024;
	bbbtree::BufferManager buffer_manager{page_size, 10};
	std::vector<uint64_t> expected_values(page_size / sizeof(uint64_t), 123);

	// Write to page
	{
		auto &page = buffer_manager.fix_page(348, 1, true, nullptr);
		ASSERT_TRUE(page.get_data());
		std::memcpy(page.get_data(), expected_values.data(), page_size);
		buffer_manager.unfix_page(page, true);
	}
	// Read from page
	{
		std::vector<uint64_t> values(page_size / sizeof(uint64_t));
		auto &page = buffer_manager.fix_page(348, 1, false, nullptr);
		std::memcpy(values.data(), page.get_data(), page_size);
		buffer_manager.unfix_page(page, true);
		ASSERT_EQ(expected_values, values);
	}
}
/// A page can be fixed also when the buffer is full.
TEST(BufferManager, EvictPage) {
	size_t page_size = 1024;
	bbbtree::BufferManager buffer_manager{page_size, 1};

	// Load page into buffer
	auto &page = buffer_manager.fix_page(348, 1, true, nullptr);
	buffer_manager.unfix_page(page, false);
	// Can evict a page when its not fixed.
	auto &page2 = buffer_manager.fix_page(169, 2, true, nullptr);
	buffer_manager.unfix_page(page2, false);
}
// TODO: Fill in tests.
/// A page can be fixed exclusively. Someone else cannot fix that page.
TEST(BufferManager, ExclusiveFlag) {}
/// A page does not loose state on eviction.
TEST(BufferManager, PersistentEviction) {

	size_t page_size = 1024;
	bbbtree::BufferManager buffer_manager{page_size, 1};
	{
		auto &frame = buffer_manager.fix_page(348, 1, true, nullptr);
		auto *data = frame.get_data();
		*data = 'a';
		buffer_manager.unfix_page(frame, true);
	}
	// Evict page
	{
		auto &frame = buffer_manager.fix_page(169, 1, true, nullptr);
		buffer_manager.unfix_page(frame, false);
	}
	// Load page again
	{
		auto &frame = buffer_manager.fix_page(348, 1, true, nullptr);
		auto *data = frame.get_data();
		EXPECT_EQ(*data, 'a');
		buffer_manager.unfix_page(frame, true);
	}
}
/// When the buffer manager is destroyed, all pages are persisted.
TEST(BufferManager, PersistentRestart) {
	size_t page_size = 1024;
	{
		bbbtree::BufferManager buffer_manager{page_size, 1};
		auto &frame = buffer_manager.fix_page(348, 1, true, nullptr);
		auto *data = frame.get_data();
		*data = 'a';
		buffer_manager.unfix_page(frame, true);
	}
	// Destruct Buffer Manager
	{
		bbbtree::BufferManager buffer_manager{page_size, 1};
		auto &frame = buffer_manager.fix_page(348, 1, true, nullptr);
		auto *data = frame.get_data();
		EXPECT_EQ(*data, 'a');
		buffer_manager.unfix_page(frame, true);
	}
}
/// When the buffer is full and all pages are exclusively locked, an error is
/// thrown.
TEST(BufferManager, BufferFull) {
	bbbtree::BufferManager buffer_manager{1024, 1};

	auto &page = buffer_manager.fix_page(348, 1, true, nullptr);

	// Cannot evict a page when all are in use.
	EXPECT_THROW(buffer_manager.fix_page(169, 2, true, nullptr),
				 bbbtree::buffer_full_error);
	buffer_manager.unfix_page(page, false);
}

} // namespace