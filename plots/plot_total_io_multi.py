import json
import matplotlib.pyplot as plt
import numpy as np
import re

with open("plots/pageviews_results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

stats = [
    "pages_loaded",
    "pages_written",
]
stat_colors = [
    "#0072B2",  # pages_loaded
    "#E69F00",  # pages_written
]
stat_labels = {
    "pages_loaded": "Pages Read",
    "pages_written": "Pages Written",
}


def base_name(name):
    return name.replace("<BTreeIndex>", "").replace("<BBBTreeIndex>", "")


# Extract all parameter sets from benchmark names
def extract_params(name):
    # Example: BM_PageViews_Mixed_Index<BTreeIndex>/400/4096/5/5/iterations:1/repeats:1
    # Get the part between > and /iterations
    match = re.search(r">/(.*)/iterations", name)
    if match:
        return match.group(1).split("/")
    return []


groups = {}
params_list = []
for bm in benchmarks:
    if "name" not in bm:
        continue
    key = base_name(bm["name"])
    groups.setdefault(key, []).append(bm)
    params = extract_params(bm["name"])
    if params:
        params_list.append(params)

# Find the index of the parameter that differs across groups
params_array = np.array(params_list)
diff_idx = None
if len(params_array) > 1:
    for i in range(params_array.shape[1]):
        col = params_array[:, i]
        if len(set(col)) > 1:
            diff_idx = i
            break

x_labels = []
btree_loaded = []
btree_written = []
bbbtree_loaded = []
bbbtree_written = []

for key, group in groups.items():
    btree = next((b for b in group if "BTreeIndex" in b["name"]), None)
    bbbtree = next((b for b in group if "BBBTreeIndex" in b["name"]), None)
    if btree is None or bbbtree is None:
        continue
    # Extract differing parameter for x-label
    params = extract_params(btree["name"])
    label = (
        params[diff_idx]
        if (params and diff_idx is not None and diff_idx < len(params))
        else key.strip("/")
    )
    x_labels.append(label)
    btree_loaded.append(btree.get("pages_loaded", 0))
    btree_written.append(btree.get("pages_written", 0))
    bbbtree_loaded.append(bbbtree.get("pages_loaded", 0))
    bbbtree_written.append(bbbtree.get("pages_written", 0))

x = np.arange(len(x_labels))
width = 0.35

fig, ax = plt.subplots(figsize=(max(8, len(x_labels) * 1.5), 6))

# B-Tree stacked bars
btree_bars1 = ax.bar(
    x - width / 2,
    btree_loaded,
    width,
    label="Pages Read (B-Tree)",
    color=stat_colors[0],
)
btree_bars2 = ax.bar(
    x - width / 2,
    btree_written,
    width,
    bottom=btree_loaded,
    label="Pages Written (B-Tree)",
    color=stat_colors[1],
)

# BBB-Tree stacked bars
bbbtree_bars1 = ax.bar(
    x + width / 2,
    bbbtree_loaded,
    width,
    label="Pages Read (BBB-Tree)",
    color=stat_colors[0],
    hatch="//",
)
bbbtree_bars2 = ax.bar(
    x + width / 2,
    bbbtree_written,
    width,
    bottom=bbbtree_loaded,
    label="Pages Written (BBB-Tree)",
    color=stat_colors[1],
    hatch="//",
)

ax.set_xticks(x)
ax.set_xticklabels(x_labels, fontsize=12)
ax.set_ylabel("Number of Page I/Os", fontsize=14)
ax.set_xlabel("Update Ratio [in %]", fontsize=14)
ax.set_title("Total Page I/O across Index Types for All Benchmarks", fontsize=15)
ax.legend(fontsize=11, loc="upper left", bbox_to_anchor=(0, 1))
ax.tick_params(axis="y", labelsize=12)
plt.tight_layout()
plt.show()
