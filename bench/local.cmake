set(BENCH_CC
        bench/bm_database.cpp
        # bench/bm_bbbtree_insert.cpp
        bench/bm_database_from_scratch.cpp
        bench/bm_bbbtree_from_scratch.cpp
        bench/bm_pageviews.cpp
        bench/helpers.cpp
)

add_executable(benchmarks bench/benchmark.cpp ${BENCH_CC})
target_link_libraries(benchmarks PRIVATE bbbtree benchmark)

# Pass project root as a macro to your benchmark code
target_compile_definitions(benchmarks PRIVATE
    PROJECT_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
)
