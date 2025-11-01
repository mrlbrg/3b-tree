import json

with open("plots/pageviews_mixed_buffer_sizes.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]


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


def get_index_type(name):
    for idx_type in ["BTreeIndex", "BBBTreeIndex", "BTreeIndexVar", "BBBTreeIndexVar"]:
        if idx_type in name:
            return idx_type
    return None


groups = {}
for bm in benchmarks:
    key = base_name(bm["name"])
    groups.setdefault(key, []).append(bm)

index_pairs = [
    ("BTreeIndex", "BBBTreeIndex"),
    ("BTreeIndexVar", "BBBTreeIndexVar"),
]

for key, group in groups.items():
    # Try all supported index pairs
    for idx1, idx2 in index_pairs:
        bm1 = next((b for b in group if idx1 in b["name"]), None)
        bm2 = next((b for b in group if idx2 in b["name"]), None)
        if bm1 and bm2:
            print(f"\nBenchmark: {key.strip('/')}")
            print(
                f"{'Metric':<30} {idx1:>15} {idx2:>15} {'Abs Diff':>15} {'% Diff':>15}"
            )
            print("-" * 90)
            for metric in bm1:
                if metric in ["name"]:
                    continue
                v1 = bm1[metric]
                v2 = bm2[metric]
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    abs_diff = v2 - v1
                    percent_diff = ((v2 - v1) / v1 * 100) if v1 != 0 else float("nan")
                    print(
                        f"{metric:<30} {v1:>15.2f} {v2:>15.2f} {abs_diff:>15.2f} {percent_diff:>15.2f}"
                    )
                else:
                    if v1 != v2:
                        print(
                            f"{metric:<30} {str(v1):>15} {str(v2):>15} {'':>15} {'':>15}"
                        )
            break  # Only print one pair per group
