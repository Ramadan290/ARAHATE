#!/usr/bin/env python3
"""
extract_cyber_singlefile_with_not.py

Static script for a single dataset file:
 - Edit INPUT_FILE and OUTPUT_FILE below (no prompts).
 - Detects a text-like column, finds whole-word 'not cyber' (case-insensitive, allows small punctuation between)
   and extracts it as 'not cyber'; otherwise extracts 'cyber'.
 - Removes occurrences of the matched phrase(s) from the text, writes extracted label into 'extracted_label'.
 - Preserves original text in 'original_text'.
 - Supports CSV/TSV/JSON/Excel/Parquet (best-effort).
"""

import os
import re
import pandas as pd

# ---------------- USER CONFIG ----------------
INPUT_FILE = "Fixes\Decoded\D17.csv"   
OUTPUT_FILE = "Fixes\Decoded\D17_edited.csv"  
EXTRACT_LABEL_COL = "label"
ORIGINAL_TEXT_COL = "text"
# ---------------------------------------------

NOT_CYBER_RE = re.compile(r"\bnot\b[\s\-\:\,\;]*\bcyber\b", flags=re.IGNORECASE)
CYBER_RE = re.compile(r"\bcyber\b", flags=re.IGNORECASE)

COMMON_TEXT_NAMES = ['text', 'tweet', 'content', 'comment', 'post', 'sentence']

def safe_read(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.csv', '.tsv'):
        sep = '\t' if ext == '.tsv' else ','
        for enc in ('utf-8', 'utf-8-sig', 'latin1', 'cp1256'):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc, dtype=str)
            except Exception:
                continue
        raise
    elif ext == '.json':
        try:
            return pd.read_json(path, dtype=str, lines=True)
        except ValueError:
            return pd.read_json(path, dtype=str)
    elif ext in ('.xlsx', '.xls'):
        return pd.read_excel(path, dtype=str)
    elif ext == '.parquet':
        return pd.read_parquet(path)
    else:
        return pd.read_csv(path, dtype=str)

def find_text_column(df):
    cols_lower = {c.lower(): c for c in df.columns}
    for name in COMMON_TEXT_NAMES:
        if name in cols_lower:
            return cols_lower[name]
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            return c
    return df.columns[0]

def clean_after_removal(text):
    """
    Clean leftover spaces and stray punctuation after removing phrases.
    """
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s\-\:\,\;\.\!]+', '', text)
    text = re.sub(r'[\s\-\:\,\;\.\!]+$', '', text)
    return text

def process_single_file(input_path, output_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    print(f"[INFO] Reading file: {input_path}")
    df = safe_read(input_path)
    print(f"[INFO] Read {len(df)} rows, {len(df.columns)} columns")

    text_col = find_text_column(df)
    print(f"[INFO] Using text column: '{text_col}'")

    df[text_col] = df[text_col].fillna('').astype(str)

    if ORIGINAL_TEXT_COL not in df.columns:
        df[ORIGINAL_TEXT_COL] = df[text_col]

    df[EXTRACT_LABEL_COL] = ""

    mask_not_cyber = df[text_col].str.contains(NOT_CYBER_RE, na=False)
    mask_cyber = df[text_col].str.contains(CYBER_RE, na=False)

    if mask_not_cyber.any():
        count_nc = int(mask_not_cyber.sum())
        print(f"[INFO] Found 'not cyber' in {count_nc} rows. Extracting 'not cyber' and cleaning those rows.")
        df.loc[mask_not_cyber, EXTRACT_LABEL_COL] = "not cyber"
        df.loc[mask_not_cyber, text_col] = df.loc[mask_not_cyber, text_col].apply(
            lambda t: NOT_CYBER_RE.sub(" ", t)
        )
        df.loc[mask_not_cyber, text_col] = df.loc[mask_not_cyber, text_col].apply(
            lambda t: CYBER_RE.sub(" ", t)
        )
        df.loc[mask_not_cyber, text_col] = df.loc[mask_not_cyber, text_col].apply(clean_after_removal)
    else:
        print("[INFO] No 'not cyber' occurrences found.")

    mask_cy_only = mask_cyber & (~mask_not_cyber)
    if mask_cy_only.any():
        count_cy = int(mask_cy_only.sum())
        print(f"[INFO] Found 'cyber' in {count_cy} rows (excluding 'not cyber' rows). Extracting 'cyber' and cleaning those rows.")
        df.loc[mask_cy_only, EXTRACT_LABEL_COL] = "cyber"
        df.loc[mask_cy_only, text_col] = df.loc[mask_cy_only, text_col].apply(lambda t: CYBER_RE.sub(" ", t))
        df.loc[mask_cy_only, text_col] = df.loc[mask_cy_only, text_col].apply(clean_after_removal)
    else:
        print("[INFO] No standalone 'cyber' occurrences found (excluding 'not cyber').")

    total_extracted = int((df[EXTRACT_LABEL_COL] != "").sum())
    print(f"[INFO] Total rows labeled (cyber / not cyber): {total_extracted} / {len(df)}")

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    out_ext = os.path.splitext(output_path)[1].lower()
    if out_ext in ('.csv', '.tsv'):
        sep = '\t' if out_ext == '.tsv' else ','
        df.to_csv(output_path, sep=sep, index=False, encoding='utf-8-sig')
    elif out_ext == '.json':
        df.to_json(output_path, orient='records', lines=True, force_ascii=False)
    elif out_ext in ('.xlsx', '.xls'):
        df.to_excel(output_path, index=False)
    elif out_ext == '.parquet':
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"[INFO] Saved fixed file to: {output_path} (rows: {len(df)})")

if __name__ == "__main__":
    process_single_file(INPUT_FILE, OUTPUT_FILE)
