import pandas as pd

# Read Excel instead of CSV
df = pd.read_excel("D32.xlsx")

def map_annotations(value):
    if value == 0:
        return "Normal"
    elif value == -1:
        return "Offensive"
    elif value == -2:
        return "Profanity"
    else:
        return "Unknown"

df['aggregatedAnnotation'] = df['aggregatedAnnotation'].apply(map_annotations)

# Save to a new file so it doesn't clash with the original
df.to_excel("Fixes\Reformatted\D32.xlsx", index=False)

print("File saved as D32_categorized.xlsx")
