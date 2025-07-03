# ---------------------------------------------------------------------------
# 3B-Tree 
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(TEST_CC
    tests/database_test.cpp
    tests/segment_test.cpp
    tests/buffer_manager_test.cpp
    tests/slotted_page_test.cpp
    tests/btree_test.cpp
)

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester tests/tester.cpp ${TEST_CC})
target_link_libraries(tester bbbtree gtest)

enable_testing()
add_test(bbbtree tester)
