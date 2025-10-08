set(BENCH_CC
        bench/bm_database.cpp
        # bench/bm_bbbtree_insert.cpp
        bench/bm_bbbtree_bulkload.cpp
)

add_executable(benchmarks bench/benchmark.cpp ${BENCH_CC})
target_link_libraries(benchmarks PRIVATE bbbtree benchmark)
