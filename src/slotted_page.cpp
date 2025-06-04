#include "bbbtree/slotted_page.h"

#include <cstring>
#include <stdexcept>

using bbbtree::SlottedPage;
using bbbtree::TID;

SlottedPage::Header::Header(uint32_t page_size)
    : slot_count(0),
      data_start(page_size) {}

SlottedPage::SlottedPage(uint32_t page_size)
    : header(page_size)
{
    // Set all bytes after the header to 0
    std::memset(get_data() + sizeof(SlottedPage), 0x00, page_size - sizeof(SlottedPage));
}

TID::SlotID SlottedPage::allocate(uint32_t data_size, uint32_t page_size)
{
    // Pre-requisite: Caller must ensure that there is enough space on this page to allocate, will throw otherwise

    // Enough space?
    auto total_size = data_size + sizeof(Slot);
    if (get_free_space() < total_size)
        throw std::logic_error("SlottedPage::allocate(): not enough space on page to allocate");

    // Get a free Slot
    auto *slot = get_slots() + header.slot_count;
    auto slot_id = header.slot_count;
    slot->set_slot(header.data_start - data_size, data_size);

    // Update Header accordingly
    ++header.slot_count;
    header.data_start -= data_size;

    return slot_id;
}

void SlottedPage::erase(uint16_t slot_id)
{
    if (header.slot_count <= slot_id)
        throw std::logic_error("SlottedPage::erase(): Slot ID not valid");
    auto *slot = get_slots() + slot_id;
    slot->clear();
}