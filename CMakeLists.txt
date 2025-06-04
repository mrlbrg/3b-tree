cmake_minimum_required(VERSION 3.10)

# Project name
project(3BTree)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/include/local.cmake")
include_directories(
    ${CMAKE_SOURCE_DIR}/include
)

include_directories(SYSTEM
    ${GTEST_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/src/local.cmake")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/tests/local.cmake")

# Add the executable
add_executable(3btree main.cpp)
