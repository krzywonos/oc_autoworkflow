import re;
from pathlib import Path;
import pandas as pd;

META_DIR = Path("dir/input/meta");
INDEX_DIR = Path("dir/input/index");
BRACKET_RE = re.compile(r"\[([^\]]*)\]");
OMID_TOKEN_OUTSIDE_RE = re.compile(r"\bomid:[^\s\]\)\}\>,;:]+",flags=re.IGNORECASE);

def remove_omid_in_brackets(match: re.Match) -> str:
    content = match.group(1);
    tokens = content.split();
    kept = [t for t in tokens if not t.lower().startswith("omid:")];
    if not kept:
        return "";
    return "[" + " ".join(kept) + "]";


def clean_meta_csv_file(csv_path: Path) -> None:
    text = csv_path.read_text(encoding="utf-8", errors="ignore");
    text = BRACKET_RE.sub(remove_omid_in_brackets, text);
    text = re.sub(r"\[\s*\]", "", text);
    text = OMID_TOKEN_OUTSIDE_RE.sub("", text);
    text = re.sub(r"[ \t]{2,}", " ", text);
    text = re.sub(r"\s+\]", "]", text);
    text = re.sub(r"\[\s+", "[", text);
    text = re.sub(r"[ \t]+\r?\n", "\n", text);
    csv_path.write_text(text, encoding="utf-8");

def clean_index_csv_file(csv_path: Path) -> None:
    df = pd.read_csv(csv_path, dtype=str);
    df = df.iloc[:, :3];
    df.to_csv(csv_path, index=False);


def process_meta_dir(meta_dir: Path) -> None:
    if not meta_dir.is_dir():
        print(f"[META] Directory not found: {meta_dir}");
        return;
    for csv_file in sorted(meta_dir.glob("*.csv")):
        print(f"[META] Cleaning {csv_file}");
        clean_meta_csv_file(csv_file);
    print("[META] Done.");


def process_index_dir(index_dir: Path) -> None:
    if not index_dir.is_dir():
        print(f"[INDEX] Directory not found: {index_dir}");
        return;
    for csv_file in sorted(index_dir.glob("*.csv")):
        print(f"[INDEX] Cleaning {csv_file}");
        clean_index_csv_file(csv_file);
    print("[INDEX] Done.");


if __name__ == "__main__":
    process_meta_dir(META_DIR);
    process_index_dir(INDEX_DIR);
    print("All done.");