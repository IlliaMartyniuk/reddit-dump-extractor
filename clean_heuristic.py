import pandas as pd

INPUT_FILE = "final_dataset.csv"
OUTPUT_FILE = "cleaned_dataset.csv"

def clean_dataset():
    print(f"Загрузка датасета: {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    initial_rows = len(df)
    
    # 1. Защита от NaN
    for col in ['title', 'selftext', 'body']:
        if col in df.columns:
            df[col] = df[col].fillna('')

    # 2. Удаление ботов и удаленных аккаунтов
    invalid_authors = ['AutoModerator', '[deleted]']
    df = df[~df['author'].isin(invalid_authors)]

    # 3. Очистка от системных заглушек модерации
    invalid_texts = ['[deleted]', '[removed]']
    df = df[~df['body'].isin(invalid_texts)]
    df = df[~df['selftext'].isin(invalid_texts)]

    # 4. Удаление абсолютно пустых записей (нет ни заголовка, ни текста комментария)
    df = df[~((df['title'] == '') & (df['body'] == ''))]

    # 5. Удаление дубликатов по системному ID Reddit
    df = df.drop_duplicates(subset=['id'])

    final_rows = len(df)
    print(f"Было строк: {initial_rows:,}")
    print(f"Удалено мусора: {initial_rows - final_rows:,}")
    print(f"Осталось чистых строк: {final_rows:,}")

    print(f"Сохранение файла {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Готово!")

if __name__ == "__main__":
    clean_dataset()