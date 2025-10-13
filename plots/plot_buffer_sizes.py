import json
import numpy as np
import matplotlib.pyplot as plt
import os

cwd = os.getcwd()
print(cwd)

# Load benchmark results
with open(os.path.join(cwd, "plots", "pageviews_results.json")) as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

buffer_sizes = [50, 100, 200, 300, 400, 500, 600, 700]

btree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/{size}/4096/5/iterations:1/repeats:1"
    for size in buffer_sizes
]
bbbtree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/{size}/4096/5/iterations:1/repeats:1"
    for size in buffer_sizes
]

btree_values = [f["pages_written"] for f in benchmarks if f["name"] in btree_benchmarks]
bbbtree_values = [
    f["pages_written"] for f in benchmarks if f["name"] in bbbtree_benchmarks
]

x = np.arange(len(buffer_sizes))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
bars1 = ax.bar(x - width / 2, btree_values, width, label="B-Tree", color="#0072B2")
bars2 = ax.bar(x + width / 2, bbbtree_values, width, label="BBB-Tree", color="#E69F00")

ax.set_xticks(x)
ax.set_xticklabels(buffer_sizes)
ax.set_xlabel("Buffer Size")
ax.set_ylabel("Number of Page Writes")
ax.legend()

plt.title("Write Amplification across Buffer Sizes")
plt.savefig(os.path.join(cwd, "plots", "pageviews_buffer_sizes.png"))
plt.close()
