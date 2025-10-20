import pandas as pd




def conllu_to_csv(conllu_file, output_csv):
    sentences = []
    current_sentence = []

    with open(conllu_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line == "":
                if current_sentence:
                    sentences.append(" ".join(current_sentence))
                    current_sentence = []
            elif line.startswith("#"):
                continue
            else:
                parts = line.split("\t")
                if len(parts) >= 2:
                    current_sentence.append(parts[1])
        if current_sentence:
            sentences.append(" ".join(current_sentence))

    df = pd.DataFrame(sentences, columns=["text"])

    df["label"] = ""  

    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Converted {len(sentences)} sentences to {output_csv}")

conllu_to_csv("example.conllu", "example.csv")
