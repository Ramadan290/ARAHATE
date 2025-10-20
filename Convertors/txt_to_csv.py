import pandas as pd

def txt_to_csv(txt_file, output_csv, label=None):
    """
    Converts a plain text file to CSV.
    
    Parameters:
    - txt_file: path to input .txt file (one text per line)
    - output_csv: path to output .csv file
    - label: optional label to assign to all rows (string). If None, column will be empty.
    """
    texts = []
    
    with open(txt_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:  # skip empty lines
                texts.append(line)
    
    df = pd.DataFrame({"text": texts})
    
    if label is not None:
        df["label"] = label
    else:
        df["label"] = "" 
    
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Converted {len(texts)} lines to {output_csv}")

txt_to_csv("example.txt", "example.csv", label="HATE")
