# ---------------------------------------------------------------------------
# 3B-Tree 
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(
    SRC_CC
    src/slotted_page.cpp
    src/buffer_manager.cpp
    src/segment.cpp
)
if(UNIX)
    set(SRC_CC ${SRC_CC} src/file/posix_file.cc)
elseif(WIN32)
    message(SEND_ERROR "Windows is not supported")
else()
    message(SEND_ERROR "unsupported platform")
endif()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(bbbtree STATIC ${SRC_CC} ${INCLUDE_H})
target_link_libraries(
    bbbtree 
)
