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

update_ratios = [0, 5, 10, 50, 75, 100]

btree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/500/4096/5/{ratio}/iterations:1/repeats:1"
    for ratio in update_ratios
]
bbbtree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/500/4096/5/{ratio}/iterations:1/repeats:1"
    for ratio in update_ratios
]

btree_values = [
    f["write_amplification"] for f in benchmarks if f["name"] in btree_benchmarks
]
bbbtree_values = [
    f["write_amplification"] for f in benchmarks if f["name"] in bbbtree_benchmarks
]

x = np.arange(len(update_ratios))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
bars1 = ax.bar(x - width / 2, btree_values, width, label="B-Tree", color="#0072B2")
bars2 = ax.bar(x + width / 2, bbbtree_values, width, label="BBB-Tree", color="#E69F00")

ax.set_xticks(x)
ax.tick_params(axis="y", labelsize=12)
ax.set_xticklabels(update_ratios, fontsize=12)
ax.set_xlabel("Write Ratio (%)", fontsize=14)
ax.set_ylabel("Write Amplification", fontsize=14)
ax.legend(fontsize=14)

plt.title("Write Amplification", fontsize=14)
plt.tight_layout()
plt.savefig(os.path.join(cwd, "plots", "pageviews_update_ratios.png"))
plt.close()
