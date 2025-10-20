import pandas as pd

# Load your CSV file
df = pd.read_csv("Fixes\Decoded\D12.csv")

# Replace only the score column with categories
def categorize_bns(score):
    if score < -0.566:
        return "Offensive"
    elif score <= 0.566:
        return "Normal"
    else:
        return "Profanity"

# Apply conversion
df['BNS_score'] = df['BNS_score'].apply(categorize_bns)

# Save the new CSV
df.to_csv("Fixes\Reformatted\D12.csv", index=False)

print("File saved ")
