# ---------------------------------------------------------------------------
# IMLAB - GTest via FetchContent
# ---------------------------------------------------------------------------

include(FetchContent)

# Set FetchContent to use a shallow clone
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_TRY_FIND_PACKAGE FALSE)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.8.0
)

FetchContent_MakeAvailable(googletest)
