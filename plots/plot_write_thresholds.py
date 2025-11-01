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

write_thresholds = [0, 1, 5, 10, 20, 30, 40, 50]

btree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/400/4096/{threshold}/5/iterations:1/repeats:1"
    for threshold in write_thresholds
]
bbbtree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/400/4096/{threshold}/5/iterations:1/repeats:1"
    for threshold in write_thresholds
]

btree_values = [f["pages_written"] for f in benchmarks if f["name"] in btree_benchmarks]
bbbtree_values = [
    f["pages_written"] for f in benchmarks if f["name"] in bbbtree_benchmarks
]

x = np.arange(len(write_thresholds))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
bars1 = ax.bar(x - width / 2, btree_values, width, label="B-Tree", color="#0072B2")
bars2 = ax.bar(x + width / 2, bbbtree_values, width, label="BBB-Tree", color="#E69F00")

ax.set_xticks(x)
ax.set_xticklabels(write_thresholds)
ax.set_xlabel("Write Threshold (%)")
ax.set_ylabel("Number of Page Writes")
ax.legend()

plt.title("Write Amplification across Write Thresholds")
plt.savefig(os.path.join(cwd, "plots", "pageviews_write_thresholds.png"))
plt.close()
