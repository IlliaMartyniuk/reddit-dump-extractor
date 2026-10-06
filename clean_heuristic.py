import pandas as pd

INPUT_FILE = "final_dataset.csv"
OUTPUT_FILE = "cleaned_dataset.csv"

def clean_dataset():
    print(f"Loading dataset: {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    initial_rows = len(df)
    
    # 1. Protect against NaN values
    for col in ['title', 'selftext', 'body']:
        if col in df.columns:
            df[col] = df[col].fillna('')

    # 2. Remove bots and deleted accounts
    invalid_authors = ['AutoModerator', '[deleted]']
    df = df[~df['author'].isin(invalid_authors)]

    # 3. Remove moderation placeholders
    invalid_texts = ['[deleted]', '[removed]']
    df = df[~df['body'].isin(invalid_texts)]
    df = df[~df['selftext'].isin(invalid_texts)]

    # 4. Remove completely empty records (no title and no body)
    df = df[~((df['title'] == '') & (df['body'] == ''))]

    # 5. Remove duplicates by Reddit ID
    df = df.drop_duplicates(subset=['id'])

    final_rows = len(df)
    print(f"Initial rows: {initial_rows:,}")
    print(f"Garbage removed: {initial_rows - final_rows:,}")
    print(f"Clean rows remaining: {final_rows:,}")

    print(f"Saving file {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done!")

if __name__ == "__main__":
    clean_dataset()