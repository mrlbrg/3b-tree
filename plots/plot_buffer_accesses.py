import json
import matplotlib.pyplot as plt
import numpy as np

with open("plots/pageviews_results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

selected_names = [
    "BM_PageViews_Mixed_Index<BTreeIndex>/300/4096/5/5/iterations:1/repeats:1",
    "BM_PageViews_Mixed_Index<BBBTreeIndex>/300/4096/5/5/iterations:1/repeats:1",
]

stats = [
    "btree_pages_hit",
    "btree_pages_missed",
    "delta_pages_hit",
    "delta_pages_missed",
]
stat_colors = [
    "#009E73",  # btree_pages_hit
    "#BB2646",  # btree_pages_missed
    "#00D39BFF",  # delta_pages_hit
    "#FB3560",  # delta_pages_missed
]
index_labels = ["BTreeIndex", "BBBTreeIndex"]
stat_labels = {
    "btree_pages_hit": "B-Tree Hits",
    "delta_pages_hit": "Delta-Tree Hits",
    "btree_pages_missed": "B-Tree Misses",
    "delta_pages_missed": "Delta-Tree Misses",
}

selected = [bm for bm in benchmarks if bm["name"] in selected_names]
if len(selected) != 2:
    raise ValueError("Could not find both selected benchmarks.")

btree = next(b for b in selected if "BTreeIndex" in b["name"])
bbbtree = next(b for b in selected if "BBBTreeIndex" in b["name"])

values = np.array(
    [[btree.get(stat, 0), bbbtree.get(stat, 0)] for stat in stats]
)  # shape: (num_stats, 2)
values = np.cumsum(values, axis=0)

x = np.arange(len(index_labels))
width = 0.13

fig, ax = plt.subplots(figsize=(8, 5))


for idx, idx_label in enumerate(index_labels):
    # Get values for this index
    vals = [values[i, idx] for i in range(len(stats))]
    # Sort stats and colors by value (descending)
    sorted_stats = sorted(zip(vals, stats, stat_colors), reverse=True)
    for val, stat, color in sorted_stats:
        ax.bar(
            idx,
            val,
            # width,
            label=f"{stat_labels[stat]}" if idx == 0 else None,
            color=color,
            zorder=2,  # Ensure bars are drawn above grid
        )

handles, labels = ax.get_legend_handles_labels()
desired_order = ["B-Tree Hits", "B-Tree Misses", "Delta-Tree Hits", "Delta-Tree Misses"]
order = [labels.index(lab) for lab in desired_order if lab in labels]
ax.legend(
    [handles[i] for i in order],
    [labels[i] for i in order],
    fontsize=11,
    loc="upper left",  # Top left corner
    bbox_to_anchor=(0, 1),
    ncol=2,
)

ax.set_xticks(x)
ax.set_xticklabels(["B-Tree Index", "BBB-Tree Index"], fontsize=12)
ax.set_ylabel("Number of Buffer Accesses", fontsize=14)
# ax.set_yscale("log")
ax.set_title("All Buffer Accesses across Index Types", fontsize=15)
ax.tick_params(axis="y", labelsize=12)
plt.tight_layout()
plt.show()
