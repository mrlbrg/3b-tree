import json
import matplotlib.pyplot as plt
import numpy as np
import re
import matplotlib.ticker as mticker

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
    # Remove any index type (BTreeIndex, BBBTreeIndex, BTreeIndexVar, BBBTreeIndexVar)
    for idx_type in [
        "<BTreeIndex>",
        "<BBBTreeIndex>",
        "<BTreeIndexVar>",
        "<BBBTreeIndexVar>",
    ]:
        name = name.replace(idx_type, "")
    return name


def extract_params(name):
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
    # Support both Index and IndexVar
    btree = next(
        (b for b in group if "BTreeIndex" in b["name"] or "BTreeIndexVar" in b["name"]),
        None,
    )
    bbbtree = next(
        (
            b
            for b in group
            if "BBBTreeIndex" in b["name"] or "BBBTreeIndexVar" in b["name"]
        ),
        None,
    )
    if btree is None or bbbtree is None:
        continue
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
ax.set_xticklabels(
    [
        "uint64_t",
        "variable-sized string with 5% write threshold",
        "variable-sized string with 6% write threshold",
    ],
    fontsize=12,
)  # <-- Set your labels here
# ax.set_xticklabels(x_labels, fontsize=12)
ax.set_ylabel("Number of Page I/Os", fontsize=16)
ax.set_xlabel("Key Type", fontsize=16)
ax.set_title("Total Page I/O across Key Types", fontsize=18)
ax.legend(fontsize=16, loc="lower left", bbox_to_anchor=(0, 0))
ax.tick_params(axis="y", labelsize=16)
ax.tick_params(axis="x", labelsize=16)
plt.tight_layout()
plt.show()
