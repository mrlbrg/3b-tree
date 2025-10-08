import json
import matplotlib.pyplot as plt

# Load benchmark results
with open("/Users/marlenebargou/dev/3b-tree/plots/results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

# Extract num_tuples from the benchmark name (assumes it's the first argument in the name string)
import re


def extract_num_tuples(name):
    match = re.search(r"\/(\d+)", name)
    if match:
        return int(match.group(1))
    return None


# Group benchmarks by num_tuples
from collections import defaultdict

grouped = defaultdict(list)
for b in benchmarks:
    if "name" in b and "pages_written" in b:
        num_tuples = extract_num_tuples(b["name"])
        if num_tuples is not None:
            grouped[num_tuples].append(b)

for num_tuples, group in grouped.items():
    names = [b["name"] for b in group]
    pages_written = [b["pages_written"] for b in group]
    real_times = [b["real_time"] for b in group if "real_time" in b]

    # Assign colors: orange for BBBTree, blue for BTree
    colors = ["#ff7f0e" if "BBBTree" in n else "#1f77b4" for n in names]

    # Plot pages_written
    plt.figure(figsize=(10, 6))
    plt.bar(names, pages_written, color=colors)
    plt.ylabel("Pages written [4 KB]")
    plt.xlabel("Benchmark Name")
    plt.title(f"Pages Written per Benchmark (num_tuples={num_tuples})")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(
        f"/Users/marlenebargou/dev/3b-tree/plots/benchmark_results_{num_tuples}.png"
    )
    plt.close()

    # Plot real_time
    if real_times:
        plt.figure(figsize=(10, 6))
        plt.bar(names, real_times, color=colors)
        plt.ylabel("Real Time (ns)")
        plt.xlabel("Benchmark Name")
        plt.title(f"Real Time per Benchmark (num_tuples={num_tuples})")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(
            f"/Users/marlenebargou/dev/3b-tree/plots/benchmark_results_realtime_{num_tuples}.png"
        )
        plt.close()
