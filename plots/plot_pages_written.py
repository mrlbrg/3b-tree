import json
import matplotlib.pyplot as plt


def main():
    filename = "plots/pageviews_results.json"
    output_path = "plots/pages_written_per_index.png"

    selected_benchmarks = {
        "BM_PageViews_Mixed_Index<BTreeIndex>/400/4096/5/5/iterations:1/repeats:1": "B-Tree",
        "BM_PageViews_Mixed_Index<BBBTreeIndex>/400/4096/5/5/iterations:1/repeats:1": "3B-Tree",
    }

    with open(filename) as f:
        data = json.load(f)
    benchmarks = data["benchmarks"]
    # Filter only selected benchmarks
    filtered = [b for b in benchmarks if b["name"] in selected_benchmarks]
    if not filtered:
        print("No selected benchmarks found.")
        return
    # Mapping for display names
    names = [selected_benchmarks.get(b["name"], b["name"]) for b in filtered]
    btree_pages_written = [b["btree_pages_written"] for b in filtered]
    delta_pages_written = [b["delta_pages_written"] for b in filtered]
    page_sizes = [b.get("page_size", 1) for b in filtered]

    num_pages = filtered[0].get("num_pages", None)
    wa_threshold = filtered[0].get("wa_threshold", None)
    page_size = page_sizes[0] if page_sizes else 1
    assert all(
        ps == page_size for ps in page_sizes
    ), f"Not all page_sizes are the same: {page_sizes}"
    plt.figure(figsize=(8, 5))
    # Color BBB-Tree in orange, others default
    colors = ["#E69F00" if n == "3B-Tree" else "#0072B2" for n in names]
    # Plot stacked bars
    bars1 = plt.bar(names, btree_pages_written, label="B-Tree Pages Written")
    bars2 = plt.bar(
        names,
        delta_pages_written,
        bottom=btree_pages_written,
        color="orange",
        label="Delta Tree Pages Written",
    )
    caption = f"\n[Page Size: {int(page_size / 1024)} KB, Max. Pages in Buffer: {int(num_pages)}, WA Threshold: {wa_threshold}%]"
    plt.ylabel(f"Number of Page Writes", fontsize=16)
    max_height = max([b + d for b, d in zip(btree_pages_written, delta_pages_written)])
    plt.ylim(top=max_height * 1.15)
    plt.title(f"Page Writes per Index", fontsize=16)
    plt.legend(fontsize=14)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


if __name__ == "__main__":
    main()
