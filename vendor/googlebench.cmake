# ---------------------------------------------------------------------------
# 3B-Tree - GTest via FetchContent
# ---------------------------------------------------------------------------

include(FetchContent)

FetchContent_Declare(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG 336bb8db986cc52cdf0cefa0a7378b9567d1afee
)

set(BENCHMARK_ENABLE_TESTING OFF)

FetchContent_MakeAvailable(benchmark)
