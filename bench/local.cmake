set(BENCH_CC
        bench/bm_database.cpp
)

add_executable(benchmarks bench/benchmark.cpp ${BENCH_CC})
target_link_libraries(benchmarks PRIVATE bbbtree benchmark)
