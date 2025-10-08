import json
import matplotlib.pyplot as plt

# Load benchmark results
with open("/Users/marlenebargou/dev/3b-tree/plots/results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

# Benchmark names to select
selected_names = [
    "BM_DatabaseWithBTreeIndex/10000/100/50/4096",
    "BM_DatabaseWithBBBTreeIndex/10000/100/50/4096",
]

# Map for renaming
rename_map = {
    "BM_DatabaseWithBTreeIndex/10000/100/50/4096": "Written physically\nwith B-Tree",
    "BM_DatabaseWithBBBTreeIndex/10000/100/50/4096": "Written physically\nwith BBB-Tree",
}

selected = [
    b
    for b in benchmarks
    if b["name"] in selected_names and "bytes_written_physically" in b
]

x_labels = [rename_map[b["name"]] for b in selected]
y_values = [b["bytes_written_physically"] for b in selected]

# x_labels.insert(0, "Written logically")
# y_values.insert(0, 1.6e5)


# Always use KB for y-axis
y_values_fmt = [v / (1024) for v in y_values]
y_label = "Bytes written (physically) [KB]"

plt.figure(figsize=(6, 6))
plt.bar(x_labels, y_values_fmt, color=["#1f77b4", "#ff7f0e"])
plt.ylabel(y_label)
# plt.xlabel("Index Type")
plt.title("10,000 Tuples inserted, 100 Pages, Threshold=50, 4KB Pages")
plt.tight_layout()
plt.savefig(
    "/Users/marlenebargou/dev/3b-tree/plots/benchmark_btree_vs_bbbtree_10000.png"
)
plt.close()
