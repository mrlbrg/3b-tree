#include "bbbtree/slotted_page.h"

#include <cstring>

using bbbtree::SlottedPage;

SlottedPage::Header::Header(uint32_t page_size)
    : slot_count(0),
      data_start(page_size) {}

SlottedPage::SlottedPage(uint32_t page_size)
    : header(page_size)
{
    // Set all bytes after the header to 0
    std::memset(get_data() + sizeof(SlottedPage), 0x00, page_size - sizeof(SlottedPage));
}