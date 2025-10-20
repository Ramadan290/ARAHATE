import pandas as pd

# Load dataset
df = pd.read_csv("Fixes\Decoded\D14.csv")

def categorize_pmi(score):
    if score <= 0:
        return "Normal"
    elif score <= 3:
        return "Offensive"
    else:
        return "Profanity"

# Apply mapping
df['pmi_score'] = df['pmi_score'].apply(categorize_pmi)

# Save result
df.to_csv("Fixes\Reformatted\D14.csv", index=False)

print("File saved ")
