import json
import matplotlib.pyplot as plt


def main():
    filename = "plots/results.json"
    output_path = "plots/benchmark_results_pages_written.png"

    selected_benchmarks = {
        "BM_PageViews_Mixed_DB<BTreeIndexed>/100/4096/5/iterations:1/repeats:1": "B-Tree",
        "BM_PageViews_Mixed_DB<BBBTreeIndexed>/100/4096/5/iterations:1/repeats:1": "BBB-Tree",
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
    pages_written = [b["pages_written"] for b in filtered]
    page_sizes = [b.get("page_size", 1) for b in filtered]

    num_pages = filtered[0].get("num_pages", None)
    wa_threshold = filtered[0].get("wa_threshold", None)
    # Use the first page_size for the unit
    page_size = page_sizes[0] if page_sizes else 1
    assert all(
        ps == page_size for ps in page_sizes
    ), f"Not all page_sizes are the same: {page_sizes}"
    plt.figure(figsize=(8, 5))
    # Color BBB-Tree in orange, others default
    colors = ["#E69F00" if n == "BBB-Tree" else "#0072B2" for n in names]
    bars = plt.bar(names, pages_written, color=colors)
    caption = f"\n[Page Size: {int(page_size / 1024)} KB, Max. Pages in Buffer: {num_pages}, WA Threshold: {wa_threshold}]"
    plt.ylabel(f"Number of Page Writes")
    plt.xlabel(caption)
    plt.title(f"Number of Writes per Index")
    plt.figtext(0.5, -0.08, caption, ha="center", fontsize=10)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


if __name__ == "__main__":
    main()
