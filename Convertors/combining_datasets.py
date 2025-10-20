#!/usr/bin/env python3
"""
merge_datasets_3class_static.py

Static, non-interactive dataset merger producing 3 classes:
  - normal
  - offensive
  - profanity

Edit the constants below (INPUT_DIR, OUTPUT_PATH, LABEL_MAP) to match your environment.
Edit get_label_map() to define exact per-source label mappings.

Run:
  python merge_datasets_3class_static.py
"""

import os
import glob
import json
import unicodedata
import re
from collections import defaultdict
import pandas as pd

# -------------------------
# USER CONFIG - edit these
# -------------------------
INPUT_DIR = "All_datasets/"               
OUTPUT_PATH = "Merged_dataset.csv"      
PER_SOURCE_DIST_OUT = "merged_3class_per_source_distribution.csv"
SUMMARY_JSON_OUT = "merged_3class_summary.json"

DROP_UNMAPPED = False        
DEFAULT_MAP_TARGET = "normal"  
DROP_EXACT_DUPLICATES = False
KEEP_ALL_COLUMNS = True     
ORIGINAL_TEXT_COL = "original_text" 

CANONICAL_LABELS = {"normal", "offensive", "profanity"}

# -------------------------
# Label mapping function
# -------------------------
def get_label_map():
    """
    Return a nested dict mapping: mapping[source_name][original_label] = canonical_label

    source_name should match the filename (without extension) that the script uses as the 'source'.
    To apply a global mapping for any source, use the key 'all' or '' (empty string).

    Example:
        return {
            "D1": {"0": "normal", "1": "offensive"},
            "D2": {"neutral": "normal", "abusive": "offensive", "swear": "profanity"},
            "all": {"0": "normal", "1": "offensive", "2": "profanity"}
        }
    """
    # ---- EDIT THIS DICT ----
    mapping = {
        # "dataset_name": {
        #     "original_label_value": "normal" | "offensive" | "profanity",
        #     ...
        # },
        # global fallback mapping for any source:
        "all": {
            "Neutral": "normal",
            "Positive" : "normal",
            "Negative": "offensive",
            "ADULT": "profanity",
            "NOT_ADULT": "normal",
            "no": "normal",
            "yes": "offensive",
            "neutral": "normal",
            "negative": "offensive",
            "positive": "normal",
            "offensive": "offensive",
            "not": "normal",
            "Offensive": "offensive",
            "non-offensive": "normal",
            "Non-offensive": "normal",
            "abusive":"profanity",
            "normal" : "normal",
            "hate" : "offensive",
            "Bullying" : "offensive",
            "non-bullying" : "normal",
            "none" : "normal",
            "None" : "normal",
            "HATE" : "offensive",
            "OFFENSIVE" : "offensive",
            "NORMAL" : "normal",
            "" : "normal",
            "cyber" : "offensive",
            "not cyber" : "normal"
            # add other global mappings as you like
        },

        # Example per-file overrides:
        # "D1": {"spam": "normal", "abuse": "offensive", "swear_word": "profanity"},
        # "twitter_dataset": {"0": "normal", "1": "offensive"},
    }
    return mapping

# -------------------------
# Internal utilities
# -------------------------
def find_files(input_dir, patterns=None):
    if patterns is None:
        patterns = ['**/*.csv', '**/*.tsv', '**/*.json', '**/*.xlsx', '**/*.xls', '**/*.parquet']
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(input_dir, p), recursive=True))
    files = sorted(files)
    return files

def safe_read_table(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in {'.csv', '.tsv'}:
        sep = '\t' if ext == '.tsv' else ','
        for enc in ('utf-8', 'utf-8-sig', 'latin1', 'cp1256'):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc, dtype=str)
            except Exception:
                continue
        raise
    elif ext in {'.json'}:
        try:
            return pd.read_json(path, dtype=str, lines=True)
        except ValueError:
            return pd.read_json(path, dtype=str)
    elif ext in {'.xlsx', '.xls'}:
        return pd.read_excel(path, dtype=str)
    elif ext in {'.parquet'}:
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")

def canonicalize_columns(df, prefer_text_fields=None, prefer_label_fields=None):
    if prefer_text_fields is None:
        prefer_text_fields = ['text', 'content', 'tweet', 'sentence', 'comment', 'post']
    if prefer_label_fields is None:
        prefer_label_fields = ['label', 'target', 'class', 'annotation', 'y']

    cols = {c.lower(): c for c in df.columns}
    text_col = None
    for name in prefer_text_fields:
        if name in cols:
            text_col = cols[name]; break
    if text_col is None:
        for c in df.columns:
            if pd.api.types.is_string_dtype(df[c]):
                text_col = c; break

    label_col = None
    for name in prefer_label_fields:
        if name in cols:
            label_col = cols[name]; break

    id_col = None
    for candidate in ['id', 'uid', 'post_id', 'tweet_id', 'idx']:
        if candidate in cols:
            id_col = cols[candidate]; break

    return text_col, label_col, id_col

def arabic_normalize(text: str) -> str:
    if not isinstance(text, str):
        text = str(text) if text is not None else ""
    text = unicodedata.normalize('NFKC', text)
    text = text.replace('\u0640', '')             # tatweel
    text = re.sub(r'[إأآ]', 'ا', text)             # alef variants -> ا
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)  # remove diacritics
    text = re.sub(r'ى', 'ي', text)                # alef maqsura to ya
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def map_label_for_row(src, lbl_raw, label_map):
    lbl_raw = "" if lbl_raw is None else str(lbl_raw).strip()
    mapped = None
    if src in label_map and lbl_raw in label_map[src]:
        mapped = label_map[src][lbl_raw]
    elif 'all' in label_map and lbl_raw in label_map['all']:
        mapped = label_map['all'][lbl_raw]
    elif '' in label_map and lbl_raw in label_map['']:
        mapped = label_map[''][lbl_raw]

    if mapped is None and lbl_raw != "":
        low = lbl_raw.lower()
        if low in {'0','none','neutral','normal','clean','non'}:
            mapped = 'normal'
        elif low in {'1','offensive','offence','abusive','hate','abuse','toxic','insult'}:
            mapped = 'offensive'
        elif low in {'2','profanity','swear','swear_word','vulgar','obscene'}:
            mapped = 'profanity'
    if mapped is None:
        if DROP_UNMAPPED:
            return None
        else:
            mapped = DEFAULT_MAP_TARGET
    mapped = mapped.lower().strip()
    if mapped not in CANONICAL_LABELS:
        return DEFAULT_MAP_TARGET
    return mapped

# -------------------------
# Main merge routine
# -------------------------
def merge_all():
    label_map = get_label_map()
    files = find_files(INPUT_DIR)
    print(f"Found {len(files)} files to process under: {INPUT_DIR}")

    all_rows = []
    per_file_counts = {}
    for fpath in files:
        try:
            df = safe_read_table(fpath)
        except Exception as e:
            print(f"[WARN] Could not read {fpath}: {e}; skipping.")
            continue
        fname = os.path.splitext(os.path.basename(fpath))[0]
        text_col, label_col, id_col = canonicalize_columns(df)
        if text_col is None:
            print(f"[WARN] No text-like column in {fpath}; skipping.")
            continue

        df[text_col] = df[text_col].astype(str).fillna('')
        if label_col is not None:
            df[label_col] = df[label_col].astype(str).fillna('')
        else:
            df['__no_label__'] = ''
            label_col = '__no_label__'

        if id_col is not None:
            df[id_col] = df[id_col].astype(str)
        else:
            df['_auto_idx_'] = df.index.astype(str)
            id_col = '_auto_idx_'

        rows_from_file = []
        for idx, row in df.iterrows():
            src = fname
            raw_text = row[text_col]
            raw_label = row[label_col] if label_col in row else ""
            mapped = map_label_for_row(src, raw_label, label_map)
            if mapped is None:
                continue

            normalized_text = arabic_normalize(raw_text)
            data_row = {
                "global_id": f"{src}_{row[id_col]}",
                "source": src,
                "text": normalized_text,
                "label_orig": raw_label,
                "label": mapped
            }

            if KEEP_ALL_COLUMNS:
                for col in df.columns:
                    val = row[col]
                    data_row[f"orig__{col}"] = val

            rows_from_file.append(data_row)

        per_file_counts[fname] = len(rows_from_file)
        all_rows.extend(rows_from_file)
        print(f"Processed {fpath} -> collected {len(rows_from_file)} rows")

    if len(all_rows) == 0:
        raise RuntimeError("No rows collected. Check INPUT_DIR and LABEL_MAP.")

    merged = pd.DataFrame(all_rows)

    duplicates_count = int(merged.duplicated(subset=['text']).sum())
    if duplicates_count > 0:
        print(f"[INFO] Exact text duplicates found: {duplicates_count}. All duplicates are being kept per config.")
    else:
        print("[INFO] No exact text duplicates found.")

    bad = set(merged['label'].unique()) - CANONICAL_LABELS
    if bad:
        print(f"[WARN] labels outside canonical: {bad} -> remapping to {DEFAULT_MAP_TARGET}")
        merged['label'] = merged['label'].apply(lambda x: x if x in CANONICAL_LABELS else DEFAULT_MAP_TARGET)

    if ORIGINAL_TEXT_COL not in merged.columns:
        merged[ORIGINAL_TEXT_COL] = merged['text']

    merged.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print(f"Saved merged file: {OUTPUT_PATH} (rows: {len(merged)})")

    per_src = merged.groupby('source')['label'].value_counts().unstack(fill_value=0)
    per_src['total'] = per_src.sum(axis=1)
    per_src = per_src.sort_values('total', ascending=False)
    per_src.to_csv(PER_SOURCE_DIST_OUT, encoding='utf-8-sig')
    print(f"Saved per-source distribution: {PER_SOURCE_DIST_OUT}")

    summary = {
        "rows": int(len(merged)),
        "label_counts": merged['label'].value_counts().to_dict(),
        "files_processed_counts": per_file_counts
    }
    with open(SUMMARY_JSON_OUT, 'w', encoding='utf-8') as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(f"Saved summary JSON: {SUMMARY_JSON_OUT}")

    return merged

# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    merged_df = merge_all()
    print("\nFINAL LABEL DISTRIBUTION:")
    print(merged_df['label'].value_counts())
    print("\nTop 10 sources by row count:")
    print(merged_df['source'].value_counts().head(10))
