import pandas as pd

def tsv_to_csv(tsv_file, csv_file):
    """
    Converts a TSV (tab-separated values) file into a CSV file.
    
    Parameters:
    - tsv_file: str, path to input .tsv file
    - csv_file: str, path to output .csv file
    """
    df = pd.read_csv(tsv_file, sep="\t", encoding="utf-8")
    
    df.to_csv(csv_file, index=False, encoding="utf-8")
    print(f"✅ Converted {tsv_file} to {csv_file} with {len(df)} rows.")

tsv_to_csv("dataset.tsv", "dataset.csv")
