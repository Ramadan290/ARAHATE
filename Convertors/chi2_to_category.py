import pandas as pd

# Load dataset
df = pd.read_csv("Fixes\Decoded\D13.csv")

def categorize_chi2(score):
    if score <= 0:
        return "Normal"
    elif score <= 200:
        return "Offensive"
    else:
        return "Profanity"

# Apply mapping
df['Chi2_score'] = df['Chi2_score'].apply(categorize_chi2)

# Save
df.to_csv("Fixes\Reformatted\D13.csv", index=False)

print("File saved as chi2_categorized.csv")
