# src/features/filter_books.py
import os
import sys
import pandas as pd

def main():
    if len(sys.argv) < 3:
        print("usage: python -m src.features.filter_books <input_csv> <output_csv>", file=sys.stderr)
        sys.exit(2)

    in_path, out_path = sys.argv[1], sys.argv[2]
    allow_csv = os.getenv("BOOKS_ALLOWED", "").strip()

    # If no allowlist, just copy through
    if not allow_csv:
        df = pd.read_csv(in_path)
        df.to_csv(out_path, index=False)
        print(f"No BOOKS_ALLOWED set. Copied {in_path} -> {out_path} (rows={len(df)})")
        return

    allowed = [b.strip() for b in allow_csv.split(",") if b.strip()]
    df = pd.read_csv(in_path)
    if "book_key" not in df.columns:
        # Nothing to filter on — just write through
        df.to_csv(out_path, index=False)
        print(f"book_key column not found. Copied {in_path} -> {out_path} (rows={len(df)})")
        return

    out = df[df["book_key"].isin(allowed)].copy()
    out.to_csv(out_path, index=False)
    print(f"Filtered to allowed books: {allowed}. {len(out)}/{len(df)} rows -> {out_path}")

if __name__ == "__main__":
    main()
