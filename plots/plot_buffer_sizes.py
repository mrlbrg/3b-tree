import json
import numpy as np
import matplotlib.pyplot as plt
import os

cwd = os.getcwd()
print(cwd)

# Load benchmark results
with open(os.path.join(cwd, "plots", "pageviews_mixed_buffer_sizes.json")) as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

buffer_sizes = [25, 50, 100, 200, 300, 400, 500]

btree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BTreeIndex>/{size}/4096/5/5/iterations:1/repeats:1"
    for size in buffer_sizes
]
bbbtree_benchmarks = [
    f"BM_PageViews_Mixed_Index<BBBTreeIndex>/{size}/4096/5/5/iterations:1/repeats:1"
    for size in buffer_sizes
]


def get_values(benchmarks, names, key):
    return [next((b[key] for b in benchmarks if b["name"] == n), 0) for n in names]


btree_btree_written = get_values(benchmarks, btree_benchmarks, "btree_pages_written")
btree_delta_written = get_values(benchmarks, btree_benchmarks, "delta_pages_written")
bbbtree_btree_written = get_values(
    benchmarks, bbbtree_benchmarks, "btree_pages_written"
)
bbbtree_delta_written = get_values(
    benchmarks, bbbtree_benchmarks, "delta_pages_written"
)

x = np.arange(len(buffer_sizes))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
# Stacked bars for B-Tree
bars1 = ax.bar(
    x - width / 2,
    btree_btree_written,
    width,
    label="B-Tree: B-Tree Pages Written",
    color="#0072B2",
)
# bars2 = ax.bar(
#     x - width / 2,
#     btree_delta_written,
#     width,
#     bottom=btree_btree_written,
#     label="B-Tree: Delta Pages Written",
#     color="#56B4E9",
# )
# Stacked bars for 3B-Tree
bars3 = ax.bar(
    x + width / 2,
    bbbtree_btree_written,
    width,
    label="3B-Tree: B-Tree Pages Written",
    color="#E69F00",
)
bars4 = ax.bar(
    x + width / 2,
    bbbtree_delta_written,
    width,
    bottom=bbbtree_btree_written,
    label="3B-Tree: Delta Pages Written",
    color="#E69F00",
    hatch="///",
    # edgecolor="#B07D00",
)

ax.set_xticks(x)
ax.set_xticklabels(buffer_sizes)
ax.tick_params(axis="both", which="major", labelsize=14)
ax.set_xlabel("Buffer Size (Number of Pages)", fontsize=16)
ax.set_ylabel("Number of Page Writes", fontsize=16)
ax.legend(fontsize=14)

plt.title("Page Writes across Buffer Sizes", fontsize=16)
# Add space above tallest bar
max_height = max(
    [a + b for a, b in zip(btree_btree_written, btree_delta_written)]
    + [a + b for a, b in zip(bbbtree_btree_written, bbbtree_delta_written)]
)
plt.ylim(top=max_height * 1.15)
plt.tight_layout()
plt.savefig(os.path.join(cwd, "plots", "pageviews_buffer_sizes.png"))
plt.close()
