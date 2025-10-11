import json

# Mapping for each benchmark to its display name, caption, and label
benchmark_info = {
    "BM_PageViews_Insert_DB": {
        "caption": "BM_PageViews_Insert_DB",
        "label": "tab:bm_pageviews_insert_db",
    },
    "BM_PageViews_Lookup_DB": {
        "caption": "BM_PageViews_Lookup_DB",
        "label": "tab:bm_pageviews_lookup_db",
    },
    "BM_PageViews_Mixed_DB": {
        "caption": "BM_PageViews_Mixed_DB",
        "label": "tab:bm_pageviews_mixed_db",
    },
}

# Order and grouping of metrics for table rows
row_groups = [
    ["page_size", "num_pages", "wa_threshold"],
    ["real_time", "cpu_time"],
    ["num_lookups_db", "num_insertions_db", "num_updates_db"],
    [
        "num_lookups_index",
        "num_insertions_index",
        "num_updates_index",
        "num_deletions_index",
    ],
    ["b_tree_height", "delta_tree_height", "node_splits"],
    ["bytes_written_logically", "bytes_written_physically", "write_amplification"],
    ["buffer_accesses", "buffer_hits", "buffer_misses"],
    [
        "pages_created",
        "slotted_pages_created",
        "pages_evicted",
        "pages_loaded",
        "pages_write_deferred",
        "pages_written",
    ],
]

# Mapping for pretty metric names
pretty = {
    "page_size": "Page Size [bytes]",
    "wa_threshold": "Write Threshold [\\%]",
    "real_time": "Real Time (ns)",
    "cpu_time": "CPU Time (ns)",
    "num_deletions_index": "Number of Deletions (Index)",
    "num_insertions_db": "Number of Insertions (DB)",
    "num_insertions_index": "Number of Insertions (Index)",
    "num_pages": "Max. Pages in Buffer Pool",
    "num_updates_db": "Number of Updates (DB)",
    "num_lookups_db": "Number of Lookups (DB)",
    "num_lookups_index": "Number of Lookups (Index)",
    "num_updates_index": "Number of Updates (Index)",
    "num_deletions_db": "Number of Deletions (DB)",
    "b_tree_height": "B-Tree Height",
    "delta_tree_height": "Delta Tree Height",
    "node_splits": "Node Splits",
    "bytes_written_logically": "Bytes Written (Logically)",
    "bytes_written_physically": "Bytes Written (Physically)",
    "write_amplification": "Write Amplification",
    "buffer_accesses": "Buffer Accesses",
    "buffer_hits": "Buffer Hits [\\%]",
    "buffer_misses": "Buffer Misses [\\%]",
    "pages_created": "Num. Pages Created",
    "slotted_pages_created": "Num. Slotted Pages Created",
    "pages_evicted": "Num. Pages Evicted",
    "pages_loaded": "Num. Pages Loaded",
    "pages_write_deferred": "Num. Pages Write Deferred",
    "pages_written": "Num. Pages Written",
}


# Helper to extract the index type from the benchmark name
def get_index_type(name):
    if "<BTreeIndexed>" in name:
        return "BTreeIndexed"
    elif "<BBBTreeIndexed>" in name:
        return "BBBTreeIndexed"
    return None


def get_benchmark_base(name):
    if name.startswith("BM_PageViews_Insert_DB"):
        return "BM_PageViews_Insert_DB"
    if name.startswith("BM_PageViews_Lookup_DB"):
        return "BM_PageViews_Lookup_DB"
    if name.startswith("BM_PageViews_Mixed_DB"):
        return "BM_PageViews_Mixed_DB"
    return None


def format_val(val):
    if isinstance(val, float):
        if abs(val) < 1e-2 and val != 0:
            return f"{val:.2e}"
        if val == int(val):
            return str(int(val))
        return f"{val:.2f}"
    return str(val)


def main():
    with open("plots/pageviews_results.json") as f:
        data = json.load(f)
    benchmarks = data["benchmarks"]
    # Organize by benchmark base and index type
    table_data = {}
    for b in benchmarks:
        base = get_benchmark_base(b["name"])
        idx = get_index_type(b["name"])
        if base and idx:
            if base not in table_data:
                table_data[base] = {}
            table_data[base][idx] = b

    # Helper to escape underscores for LaTeX
    def esc(s):
        return s.replace("_", r"\_")

    # Compute node_splits and build tables
    for base, idxs in table_data.items():
        info = benchmark_info[base]
        # Compute node_splits
        for idx in idxs:
            b = idxs[idx]
            b["node_splits"] = b.get("inner_node_splits", 0) + b.get(
                "leaf_node_splits", 0
            )
        print(f"\\begin{{table}}[ht]")
        print("\\centering")
        print(f"\\caption{{{esc(info['caption'])}}}")
        print(f"\\label{{{info['label']}}}")
        print("\\begin{tabular}{lrr}")
        print("\\toprule")
        print("Metric & B-Tree & BBB-Tree \\\\")
        print("\\midrule")
        for i, group in enumerate(row_groups):
            for metric in group:
                v1 = idxs["BTreeIndexed"].get(metric, 0)
                v2 = idxs["BBBTreeIndexed"].get(metric, 0)
                print(
                    f"{esc(pretty[metric])} & {format_val(v1)} & {format_val(v2)} \\\\"
                )
            if i < len(row_groups) - 1:
                print("\\midrule")
        print("\\bottomrule")
        print("\\end{tabular}")
        print("\\end{table}\n")


if __name__ == "__main__":
    main()
