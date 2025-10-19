import json

with open("plots/pageviews_results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]


def base_name(name):
    return name.replace("<BTreeIndex>", "").replace("<BBBTreeIndex>", "")


groups = {}
for bm in benchmarks:
    key = base_name(bm["name"])
    groups.setdefault(key, []).append(bm)

for key, group in groups.items():
    if len(group) != 2:
        continue  # Only print if both index types are present
    btree = next(b for b in group if "BTreeIndex" in b["name"])
    bbbtree = next(b for b in group if "BBBTreeIndex" in b["name"])
    print(f"\nBenchmark: {key.strip('/')}")
    print(
        f"{'Metric':<30} {'BTreeIndex':>15} {'BBBTreeIndex':>15} {'Abs Diff':>15} {'% Diff':>15}"
    )
    print("-" * 90)
    for metric in btree:
        if metric in ["name"]:
            continue
        v1 = btree[metric]
        v2 = bbbtree[metric]
        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
            abs_diff = v2 - v1
            percent_diff = ((v2 - v1) / v1 * 100) if v1 != 0 else float("nan")
            print(
                f"{metric:<30} {v1:>15.2f} {v2:>15.2f} {abs_diff:>15.2f} {percent_diff:>15.2f}"
            )
        else:
            if v1 != v2:
                print(f"{metric:<30} {str(v1):>15} {str(v2):>15} {'':>15} {'':>15}")
