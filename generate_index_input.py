import csv;
from pathlib import Path;

META_OUT_DIR = Path("dir/output/meta/ocmetacsv_output");
OUT_INDEX_CSV = Path("dir/input/index/generated_index_input.csv");

# How many IDs per group: 10 means:
# ids[0] cites ids[1..9], ids[10] cites ids[11..19], ...
GROUP_SIZE = 10;

# Optional: stop after collecting N IDs total (set to None to use all)
MAX_IDS = 3000;


def first_non_omid_id(id_cell: str) -> str | None:
    """
    Given a cell like:
      "omid:br/0602636 doi:10.1594/pangaea.141285 openalex:W2336472292"
    return the first token that is NOT an OMID, e.g. "doi:10.1594/..."
    """
    if not id_cell:
        return None;
    tokens = str(id_cell).strip().split();
    for t in tokens:
        if not t.lower().startswith("omid:"):
            return t.strip();
    return None;


def main():
    if not META_OUT_DIR.is_dir():
        raise FileNotFoundError(f"Meta output directory not found: {META_OUT_DIR}");

    # Collect IDs (first non-omid token) from all output_*.csv files
    ids: list[str] = [];

    for csv_path in sorted(META_OUT_DIR.glob("output_*.csv")):
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f);
            if "id" not in reader.fieldnames:
                print(f"[WARN] Skipping {csv_path.name}: no 'id' column");
                continue;

            for row in reader:
                anyid = first_non_omid_id(row.get("id", ""));
                if anyid:
                    ids.append(anyid);
                if MAX_IDS is not None and len(ids) >= MAX_IDS:
                    break;

        if MAX_IDS is not None and len(ids) >= MAX_IDS:
            break;

    if len(ids) < GROUP_SIZE:
        raise RuntimeError(
            f"Not enough usable IDs to form groups. Collected {len(ids)} IDs."
        );

    # Generate citations
    citations: list[tuple[str, str]] = []
    for start in range(0, len(ids) - GROUP_SIZE + 1, GROUP_SIZE):
        citing = ids[start];
        cited_list = ids[start + 1 : start + GROUP_SIZE];
        for cited in cited_list:
            citations.append((citing, cited));

    # Write output CSV
    OUT_INDEX_CSV.parent.mkdir(parents=True, exist_ok=True);
    with OUT_INDEX_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f);
        w.writerow(["citing", "cited"]);
        w.writerows(citations);

    print(f"Collected IDs: {len(ids)}");
    print(f"Generated citations: {len(citations)}");
    print(f"Wrote index input CSV: {OUT_INDEX_CSV.resolve()}");


if __name__ == "__main__":
    main();
