import pandas as pd

# === CONFIGURATION ===
INPUT_FILE = "D18.xlsx"          # Your input Excel file
LABEL_COLUMN = "Type"        # Column name to check
OUTPUT_FILE = "Fixes\Reformatted\D18.xlsx"  # Output file name
# ======================

def fill_blank_labels():
    # Load Excel
    df = pd.read_excel(INPUT_FILE)

    # Fill blanks with "non-bullying"
    df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna("non-bullying")

    # Save new file
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Done! Saved as {OUTPUT_FILE}")


if __name__ == "__main__":
    fill_blank_labels()
