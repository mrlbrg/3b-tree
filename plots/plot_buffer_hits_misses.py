import json
import matplotlib.pyplot as plt
import numpy as np

with open("plots/pageviews_results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

buffer_sizes = [50, 100, 200, 300, 400, 500]

# Prepare benchmark names for each buffer size
btree_names = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/{size}/4096/5/5/iterations:1/repeats:1"
    for size in buffer_sizes
]
bbbtree_names = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/{size}/4096/5/5/iterations:1/repeats:1"
    for size in buffer_sizes
]

index_labels = ["B-tree Index", "3B-tree Index"]

# Colorblind-friendly palette: blue/orange for B-tree, green/red for BBB-tree
btree_hit_color = "#0072B2"  # blue
btree_miss_color = "#D55E00"  # orange
bbbtree_hit_color = "#009E73"  # green
bbbtree_miss_color = "#CC79A7"  # magenta


def get_stat_per_size(benchmarks, names, stat):
    return [next((b[stat] for b in benchmarks if b["name"] == n), 0) for n in names]


# Gather stats for each buffer size and index type
btree_hits = get_stat_per_size(benchmarks, btree_names, "btree_pages_hit")
btree_delta_hits = get_stat_per_size(benchmarks, btree_names, "delta_pages_hit")
btree_misses = get_stat_per_size(benchmarks, btree_names, "btree_pages_missed")
btree_delta_misses = get_stat_per_size(benchmarks, btree_names, "delta_pages_missed")

bbbtree_hits = get_stat_per_size(benchmarks, bbbtree_names, "btree_pages_hit")
bbbtree_delta_hits = get_stat_per_size(benchmarks, bbbtree_names, "delta_pages_hit")
bbbtree_misses = get_stat_per_size(benchmarks, bbbtree_names, "btree_pages_missed")
bbbtree_delta_misses = get_stat_per_size(
    benchmarks, bbbtree_names, "delta_pages_missed"
)

x = np.arange(len(buffer_sizes))
width = 0.35

# --- Buffer Hits Plot ---
fig, ax = plt.subplots(figsize=(8, 5))
bars1 = ax.bar(
    x - width / 2, btree_hits, width, label="B-tree: B-tree Hits", color=btree_hit_color
)
bars3 = ax.bar(
    x + width / 2,
    bbbtree_hits,
    width,
    label="3B-tree: B-tree Hits",
    color=bbbtree_hit_color,
)
bars4 = ax.bar(
    x + width / 2,
    bbbtree_delta_hits,
    width,
    bottom=bbbtree_hits,
    label="3B-tree: Delta Tree Hits",
    color=bbbtree_hit_color,
    hatch="//",
)

ax.set_xticks(x)
ax.set_xticklabels(buffer_sizes, fontsize=14)
ax.set_xlabel("Buffer Size (Number of Pages)", fontsize=16)
ax.set_ylabel("Number of Buffer Hits", fontsize=16)
ax.set_title("Buffer Hits per Buffer Size", fontsize=16)
ax.legend(
    ["B-tree: B-tree Hits", "3B-tree: B-tree Hits", "3B-tree: Delta Tree Hits"],
    fontsize=14,
    loc="lower left",
    bbox_to_anchor=(0, 0),
)
ax.tick_params(axis="y", labelsize=14)
ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=1)
plt.tight_layout()
plt.savefig("plots/buffer_hits_per_buffer_size.png")
plt.close()

# --- Buffer Misses Plot ---
fig, ax = plt.subplots(figsize=(8, 5))
bars1 = ax.bar(
    x - width / 2,
    btree_misses,
    width,
    label="B-tree: B-tree Misses",
    color=btree_miss_color,
)
bars3 = ax.bar(
    x + width / 2,
    bbbtree_misses,
    width,
    label="3B-tree: B-tree Misses",
    color=bbbtree_miss_color,
)
bars4 = ax.bar(
    x + width / 2,
    bbbtree_delta_misses,
    width,
    bottom=bbbtree_misses,
    label="3B-tree: Delta Tree Misses",
    color=bbbtree_miss_color,
    hatch="//",
)

ax.set_xticks(x)
ax.set_xticklabels(buffer_sizes, fontsize=14)
ax.set_xlabel("Buffer Size (Number of Pages)", fontsize=16)
ax.set_ylabel("Number of Buffer Misses", fontsize=16)
ax.set_title("Buffer Misses per Buffer Size", fontsize=16)
ax.legend(
    ["B-tree: B-tree Misses", "3B-tree: B-tree Misses", "3B-tree: Delta Tree Misses"],
    fontsize=14,
)
ax.tick_params(axis="y", labelsize=14)
ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=1)
plt.tight_layout()
plt.savefig("plots/buffer_misses_per_buffer_size.png")
plt.close()
