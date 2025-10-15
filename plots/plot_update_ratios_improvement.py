import json
import numpy as np
import matplotlib.pyplot as plt
import os

cwd = os.getcwd()

# Load benchmark results
with open(os.path.join(cwd, "plots", "pageviews_results.json")) as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

update_ratios = [0, 5, 10, 50, 75, 100]

btree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/500/4096/5/{ratio}/iterations:1/repeats:1"
    for ratio in update_ratios
]
bbbtree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/500/4096/5/{ratio}/iterations:1/repeats:1"
    for ratio in update_ratios
]

# Get values for each threshold, keeping order
btree_values = [
    next((f["pages_written"] for f in benchmarks if f["name"] == name), None)
    for name in btree_benchmarks
]
bbbtree_values = [
    next((f["pages_written"] for f in benchmarks if f["name"] == name), None)
    for name in bbbtree_benchmarks
]

# Calculate percentage improvement: (btree - bbbtree) / btree * 100
improvements = []
for btree, bbbtree in zip(btree_values, bbbtree_values):
    if btree and btree != 0:
        improvements.append((btree - bbbtree) / btree * 100)
    else:
        improvements.append(0)

fig, ax = plt.subplots()
ax.plot(
    update_ratios,
    improvements,
    marker="o",
    color="#E69F00",
    label="% Improvement in BBB-Tree",
)
ax.axhline(
    0, color="#0072B2", linestyle="--", linewidth=1, label="Page Writes in B-Tree"
)
ax.set_xticks(list(range(0, 101, 10)))
ax.set_xlabel("Update Ratio (%)", fontsize=14)
ax.set_ylabel("% Fewer Pages Written", fontsize=14)
ax.set_title("Write Amplification Reduction across Workloads", fontsize=16)
ax.set_ylim(top=100)
ax.legend(fontsize=12)
plt.tight_layout()
plt.savefig(os.path.join(cwd, "plots", "pageviews_update_ratios_improvement.png"))
plt.close()
