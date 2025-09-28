"""
producer_mortality_anjana.py

Mortality data streaming producer.
Reads mortality data from data/USRegionalMortality.csv, filters for three regions (HHS Region 01, HHS Region 02, HHS Region 03),
and streams JSONL messages to data/project_live.json. Also stores messages in SQLite database (data/mortality.sqlite).

Example JSON message:
{
    "message": "In HHS Region 01, Urban Male population reported Heart disease mortality at 188.2 (SE 1.0).",
    "author": "HHS_Region_01",
    "timestamp": "2025-09-27 23:44:00",
    "category": "heart_disease",
    "sentiment": 0.2,
    "keyword_mentioned": "High",
    "message_length": 78,
    "region": "HHS Region 01",
    "status": "Urban",
    "sex": "Male",
    "cause": "Heart disease",
    "rate": 188.2,
    "se": 1.0
}
"""

# stdlib
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
from typing import Mapping, Any
import csv
import sqlite3

# local
import utils.utils_config as config
from utils.utils_logger import logger

# local emitters
from utils.emitters import file_emitter, sqlite_emitter

def init_sqlite_db(db_path: pathlib.Path) -> None:
    """Initialize SQLite database for storing mortality messages."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mortality_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            keyword_mentioned TEXT,
            message_length INTEGER,
            region TEXT,
            status TEXT,
            sex TEXT,
            cause TEXT,
            rate REAL,
            se REAL
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info(f"SQLite database initialized at {db_path}")

def load_mortality_data(csv_path: str) -> list[dict]:
    """Load mortality data from CSV file and filter for three regions."""
    data = []
    allowed_regions = ["HHS Region 01", "HHS Region 02", "HHS Region 03"]
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Region"] in allowed_regions:
                data.append({
                    "region": row["Region"],
                    "status": row["Status"],
                    "sex": row["Sex"],
                    "cause": row["Cause"],
                    "rate": float(row["Rate"]),
                    "se": float(row["SE"])
                })
    return data

def generate_mortality_messages(csv_path: str):
    """Yield mortality data JSON messages from CSV for three regions."""
    
    # Load data from CSV
    mortality_data = load_mortality_data(csv_path)
    
    # Sentiment and keyword logic based on mortality rate
    RATE_THRESHOLDS = {
        "High": (100, float('inf')),  # High mortality rate
        "Medium": (50, 100),          # Moderate mortality rate
        "Low": (0, 50)                # Low mortality rate
    }
    
    # Message templates for varied text
    MESSAGE_TEMPLATES = [
        "In {region}, {status} {sex} population reported {cause} mortality at {rate} (SE {se}).",
        "{region} {status} {sex}s observed a {cause} death rate of {rate}, SE {se}.",
        "{cause} mortality in {region} for {status} {sex}s: {rate} with SE {se}.",
        "{region} records {rate} deaths per 100,000 from {cause} among {status} {sex}s (SE {se}).",
        "For {status} {sex}s in {region}, {cause} mortality rate is {rate} (SE {se}).",
        "{sex} residents in {status} {region} face a {cause} death rate of {rate} ± {se}.",
        "{cause} claims {rate} per 100,000 {status} {sex}s in {region}, SE {se}.",
        "{region}'s {status} {sex} population has a {cause} mortality rate of {rate} (SE {se})."
    ]
    
    message_count = 0
    base_time = datetime(2025, 9, 27, 23, 44, 0)  # Updated to 11:44 PM EDT, Sep 27, 2025
    
    while True:
        for record in mortality_data:
            region = record["region"]
            status = record["status"]
            sex = record["sex"]
            cause = record["cause"]
            rate = record["rate"]
            se = record["se"]
            
            # Determine keyword and sentiment based on rate
            for keyword, (min_rate, max_rate) in RATE_THRESHOLDS.items():
                if min_rate <= rate < max_rate:
                    efficiency = keyword
                    if keyword == "High":
                        sentiment = random.uniform(0.1, 0.3)
                    elif keyword == "Medium":
                        sentiment = random.uniform(0.4, 0.7)
                    else:  # Low
                        sentiment = random.uniform(0.8, 1.0)
                    break
            
            # Select a random message template
            message_text = random.choice(MESSAGE_TEMPLATES).format(
                region=region, status=status, sex=sex, cause=cause, rate=rate, se=se
            )
            
            # Calculate timestamp (increment by 1 minute per message)
            timestamp = base_time.replace(minute=(message_count % 60), 
                                       hour=(message_count // 60) % 24)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            
            # Format category
            category = cause.lower().replace(" ", "_")
            
            message = {
                "message": message_text,
                "author": f"{region.replace(' ', '_')}",
                "timestamp": timestamp_str,
                "category": category,
                "sentiment": round(sentiment, 2),
                "keyword_mentioned": efficiency,
                "message_length": len(message_text),
                "region": region,
                "status": status,
                "sex": sex,
                "cause": cause,
                "rate": rate,
                "se": se
            }
            
            yield message
            message_count += 1

def emit_to_file(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    """Append one message to a JSONL file."""
    return file_emitter.emit_message(message, path=path)

def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into SQLite."""
    return sqlite_emitter.emit_message(message, db_path=db_path)

def main() -> None:
    logger.info("Starting Mortality Data Producer")
    logger.info("Streaming mortality data from data/USRegionalMortality.csv for three regions")
    logger.info("Use Ctrl+C to stop.")

    # STEP 1. Read config
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = (
            config.get_sqlite_path() if hasattr(config, "get_sqlite_path")
            else pathlib.Path("data/mortality.sqlite")
        )
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Initialize SQLite database
    try:
        init_sqlite_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite database: {e}")
        sys.exit(2)

    # STEP 3. Reset file sink
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to prep live data file: {e}")
        sys.exit(3)

    # STEP 4. Emit loop
    try:
        csv_path = "data/USRegionalMortality.csv"
        if not pathlib.Path(csv_path).exists():
            logger.error(f"ERROR: CSV file {csv_path} not found")
            sys.exit(4)
        for message in generate_mortality_messages(csv_path):
            logger.info(f"{message['region']} {message['status']} {message['sex']} {message['cause']}: "
                       f"Rate {message['rate']}, SE {message['se']}, "
                       f"Keyword: {message['keyword_mentioned']}, Message: {message['message']}")

            # Emit to JSONL file
            emit_to_file(message, path=live_data_path)
            # Emit to SQLite
            emit_to_sqlite(message, db_path=sqlite_path)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Mortality Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("Mortality Producer shutting down.")

if __name__ == "__main__":
    main()