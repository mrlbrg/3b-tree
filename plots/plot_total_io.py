import json
import matplotlib.pyplot as plt
import numpy as np

with open("plots/pageviews_results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

selected_names = [
    "BM_PageViews_Insert_Index<BTreeIndex>/200/4096/5/iterations:1/repeats:1",
    "BM_PageViews_Insert_Index<BBBTreeIndex>/200/4096/5/iterations:1/repeats:1",
]

stats = [
    "pages_loaded",
    "pages_written",
]
stat_colors = [
    "#0072B2",  # pages_loaded
    "#E69F00",  # pages_written
]
index_labels = ["BTreeIndex", "BBBTreeIndex"]
stat_labels = {
    "pages_loaded": "Pages Read",
    "pages_written": "Pages Written",
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
width = 0.18

fig, ax = plt.subplots(figsize=(7, 5))

for idx, idx_label in enumerate(index_labels):
    vals = [values[i, idx] for i in range(len(stats))]
    sorted_stats = sorted(zip(vals, stats, stat_colors), reverse=True)
    for val, stat, color in sorted_stats:
        ax.bar(
            idx,
            val,
            label=f"{stat_labels[stat]}" if idx == 0 else None,
            color=color,
            zorder=2,
        )

handles, labels = ax.get_legend_handles_labels()
desired_order = ["Pages Read", "Pages Written"]
order = [labels.index(lab) for lab in desired_order if lab in labels]
ax.legend(
    [handles[i] for i in order],
    [labels[i] for i in order],
    fontsize=11,
    loc="upper left",
    bbox_to_anchor=(0, 1),
    ncol=1,
)

ax.set_xticks(x)
ax.set_xticklabels(["B-Tree Index", "BBB-Tree Index"], fontsize=12)
ax.set_ylabel("Number of Page I/Os", fontsize=14)
ax.set_title("Total Page I/O across Index Types", fontsize=15)
ax.tick_params(axis="y", labelsize=12)
plt.tight_layout()
plt.show()
