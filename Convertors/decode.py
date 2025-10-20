#!/usr/bin/env python3
"""
decode_arabic_robust.py

- Edit INPUT_PATH and OUTPUT_PATH at the top (no prompts).
- Safely raise csv.field_size_limit (handles OverflowError).
- Tries multiple read strategies; falls back to chunked processing if necessary.
- Applies simple mojibake fixes for Arabic text.
"""

from pathlib import Path
import csv
import sys
import math
import re
import pandas as pd
import traceback

# ------------------ EDIT PATHS HERE ------------------
INPUT_PATH = Path("Fixes\Encoded\D5.csv")   
OUTPUT_PATH = Path("Fixes\Decoded\D5.csv")  

CHUNKSIZE = 0   
# ----------------------------------------------------

def safe_set_csv_field_limit():
    """
    Safely try to set csv.field_size_limit to a large value.
    Some platforms raise OverflowError when using sys.maxsize; reduce until accepted.
    Returns the value that was set (or None if failed).
    """
    try_values = [sys.maxsize, 10**9, 10**8, 10**7, 10**6]
    for val in try_values:
        try:
            csv.field_size_limit(val)
            return val
        except OverflowError:
            continue
        except Exception:
            continue
    v = sys.maxsize
    while v > 0:
        try:
            csv.field_size_limit(v)
            return v
        except OverflowError:
            v = v // 10
        except Exception:
            break
    try:
        csv.field_size_limit(10**7)
        return 10**7
    except Exception:
        return None

LIMIT_SET = safe_set_csv_field_limit()
print("csv.field_size_limit set to:", LIMIT_SET)

ARABIC_RE = re.compile(
    r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]'
)

def arabic_score(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return len(ARABIC_RE.findall(text))

def try_decode(text: str, enc_from: str, dec_to: str):
    try:
        return text.encode(enc_from, errors='strict').decode(dec_to, errors='strict')
    except Exception:
        return None

def fix_text(text: str) -> str:
    if not isinstance(text, str):
        return text
    candidates = [text]
    combos = [
        ("latin1", "utf-8"),
        ("cp1252", "utf-8"),
        ("utf-8", "latin1"),
        ("cp1256", "utf-8"),
    ]
    for enc_from, dec_to in combos:
        t = try_decode(text, enc_from, dec_to)
        if t is not None:
            candidates.append(t)
    return max(candidates, key=arabic_score)

def fix_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    obj_cols = [c for c in df.columns if df[c].dtype == 'object' or pd.api.types.is_string_dtype(df[c])]
    for col in obj_cols:
        df[col] = df[col].map(lambda v: fix_text(v) if isinstance(v, str) else v)
    return df

def try_read_csv_with_strategies(path: Path):
    """
    Try reading CSV using a combination of encodings and engines.
    Returns a DataFrame on success or raises the last exception on failure.
    """
    encodings = ["utf-8", "cp1256", "latin1"]
    engines = ["c", "python"]
    last_exc = None

    for enc in encodings:
        for eng in engines:
            try:
                print(f"Trying pd.read_csv(encoding={enc!r}, engine={eng!r}) ...")
                if eng == "c":
                    df = pd.read_csv(path, dtype=str, encoding=enc, engine=eng, low_memory=False)
                else:
                    df = pd.read_csv(path, dtype=str, encoding=enc, engine=eng)
                print("Read successful with", enc, eng)
                return df
            except Exception as e:
                print(f"  Failed with encoding={enc}, engine={eng}: {type(e).__name__}: {e}")
                last_exc = e

    raise last_exc if last_exc is not None else ValueError("Unable to read CSV with available strategies.")

def process_in_chunks(path: Path, out_path: Path, chunksize: int):
    """
    Read CSV in chunks and process each chunk, writing output incrementally.
    Header is written only for the first chunk.
    """
    first = True
    encodings = ["utf-8", "cp1256", "latin1"]
    for enc in encodings:
        try:
            print(f"Attempting chunked read with encoding={enc} (engine='c') chunksize={chunksize} ...")
            reader = pd.read_csv(path, dtype=str, encoding=enc, engine='c', chunksize=chunksize)
            for chunk in reader:
                fixed = fix_dataframe(chunk)
                if first:
                    fixed.to_csv(out_path, index=False, encoding='utf-8-sig', mode='w')
                    first = False
                else:
                    fixed.to_csv(out_path, index=False, encoding='utf-8-sig', mode='a', header=False)
            print("Chunked write complete")
            return
        except Exception as e:
            print(f"  Chunked read failed with encoding={enc}, engine='c': {type(e).__name__}: {e}")
    try:
        print(f"Attempting chunked read with encoding='utf-8' engine='python' chunksize={chunksize} ...")
        reader = pd.read_csv(path, dtype=str, encoding='utf-8', engine='python', chunksize=chunksize)
        for chunk in reader:
            fixed = fix_dataframe(chunk)
            if first:
                fixed.to_csv(out_path, index=False, encoding='utf-8-sig', mode='w')
                first = False
            else:
                fixed.to_csv(out_path, index=False, encoding='utf-8-sig', mode='a', header=False)
        print("Chunked write complete (python engine)")
        return
    except Exception as e:
        print("All chunked attempts failed:", type(e).__name__, e)
        raise

def main():
    try:
        if not INPUT_PATH.exists():
            print("ERROR: INPUT_PATH does not exist:", INPUT_PATH)
            return

        ext = INPUT_PATH.suffix.lower()
        if ext in (".csv", ".tsv"):
            if CHUNKSIZE and CHUNKSIZE > 0:
                print("Using chunked processing. CHUNKSIZE =", CHUNKSIZE)
                OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                process_in_chunks(INPUT_PATH, OUTPUT_PATH, CHUNKSIZE)
            else:
                try:
                    df = try_read_csv_with_strategies(INPUT_PATH)
                    print("CSV loaded; applying fixes...")
                    df = fix_dataframe(df)
                    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
                    print("Done. Output saved to", OUTPUT_PATH)
                except Exception as e_full:
                    print("Full-read attempts failed:", type(e_full).__name__, e_full)
                    fallback_chunksize = 10000
                    print(f"Falling back to chunked processing with chunksize={fallback_chunksize} ...")
                    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                    process_in_chunks(INPUT_PATH, OUTPUT_PATH, fallback_chunksize)
        elif ext in (".xls", ".xlsx"):
            print("Reading Excel file...")
            df = pd.read_excel(INPUT_PATH, dtype=str)
            df = fix_dataframe(df)
            OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            if OUTPUT_PATH.suffix.lower() in ('.xlsx', '.xls'):
                df.to_excel(OUTPUT_PATH, index=False, engine='openpyxl')
            else:
                df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
            print("Done. Output saved to", OUTPUT_PATH)
        else:
            print("Unsupported file extension:", ext)
            return

    except Exception as e:
        print("Unhandled exception:", type(e).__name__, e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
