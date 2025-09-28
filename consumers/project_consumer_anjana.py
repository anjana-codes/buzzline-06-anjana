import json
import sqlite3
import pathlib
import time

DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/anjana_keyword.sqlite')


def init_db(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            keyword_mentioned TEXT,
            message_length INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keyword_counts (
            keyword TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')

    conn.commit()
    conn.close()


def store_message(message, db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        message.get("message"),
        message.get("author"),
        message.get("timestamp"),
        message.get("category"),
        float(message.get("sentiment", 0.0)),
        message.get("keyword_mentioned"),
        int(message.get("message_length", 0))
    ))

    keyword = message.get("keyword_mentioned")
    if keyword:
        cursor.execute('''
            INSERT INTO keyword_counts (keyword, count)
            VALUES (?, 1)
            ON CONFLICT(keyword) DO UPDATE SET count = count + 1
        ''', (keyword,))

    conn.commit()
    conn.close()


def read_new_messages(data_file, last_pos):
    with open(data_file, 'r', encoding="utf-8") as f:
        f.seek(last_pos)  # Go to where we last stopped
        lines = f.readlines()
        new_pos = f.tell()  # Remember new position for next loop
    return lines, new_pos


def main():
    init_db(DB_FILE)
    print(f"Starting consumer_anjana.py — reading from {DATA_FILE} into {DB_FILE}")

    last_pos = 0
    try:
        while True:
            lines, last_pos = read_new_messages(DATA_FILE, last_pos)

            if not lines:
                print("No new messages. Waiting...")
            for line in lines:
                if line.strip():
                    message = json.loads(line.strip())
                    store_message(message, DB_FILE)
                    print(f"Stored new message: {message.get('message')}")
            time.sleep(2)  # Delay before checking again
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")


if __name__ == "__main__":
    main()
