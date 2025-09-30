"""
producer_mortality_anjana.py

Mortality data streaming producer.
Reads mortality data from data/USRegionalMortality.csv (all regions included),
and streams JSONL messages to data/project_live.json. Also stores messages in SQLite database (data/mortality_analytics.sqlite).

Example JSON message:
{
    "message": "In HHS Region 01, Urban Male population reported Heart disease mortality at 188.2 (SE 1.0).",
    "author": "HHS_Region_01",
    "timestamp": "2025-09-27 23:44:00",
    "category": "heart_disease",
    "region": "HHS Region 01",
    "status": "Urban",
    "sex": "Male",
    "cause": "Heart disease",
    "rate": 188.2,
    "se": 1.0
}
"""

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

import utils.utils_config as config
from utils.utils_logger import logger
from utils.emitters import file_emitter, sqlite_emitter

def init_sqlite_db(db_path: pathlib.Path) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mortality_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
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
    data = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
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
    mortality_data = load_mortality_data(csv_path)

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
    base_time = datetime(2025, 9, 27, 23, 44, 0)
    
    while True:
        for record in mortality_data:
            region = record["region"]
            status = record["status"]
            sex = record["sex"]
            cause = record["cause"]
            rate = record["rate"]
            se = record["se"]

            message_text = random.choice(MESSAGE_TEMPLATES).format(
                region=region, status=status, sex=sex, cause=cause, rate=rate, se=se
            )

            timestamp = base_time.replace(minute=(message_count % 60), 
                                          hour=(message_count // 60) % 24)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            category = cause.lower().replace(" ", "_")

            message = {
                "message": message_text,
                "author": f"{region.replace(' ', '_')}",
                "timestamp": timestamp_str,
                "category": category,
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
    return file_emitter.emit_message(message, path=path)

def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    return sqlite_emitter.emit_message(message, db_path=db_path)

def main() -> None:
    logger.info("Starting Mortality Data Producer")
    logger.info("Streaming mortality data from data/USRegionalMortality.csv")
    logger.info("Use Ctrl+C to stop.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path() if hasattr(config, "get_sqlite_path") else pathlib.Path("data/mortality.sqlite")
    except Exception as e:
        logger.error(f"Failed to read environment variables: {e}")
        sys.exit(1)

    try:
        init_sqlite_db(sqlite_path)
    except Exception as e:
        logger.error(f"Failed to initialize SQLite database: {e}")
        sys.exit(2)

    try:
        if live_data_path.exists():
            live_data_path.unlink()
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to prep live data file: {e}")
        sys.exit(3)

    try:
        csv_path = "data/USRegionalMortality.csv"
        if not pathlib.Path(csv_path).exists():
            logger.error(f"CSV file {csv_path} not found")
            sys.exit(4)

        for message in generate_mortality_messages(csv_path):
            logger.info(f"{message['region']} {message['status']} {message['sex']} {message['cause']}: Rate {message['rate']}, SE {message['se']}")
            emit_to_file(message, path=live_data_path)
            emit_to_sqlite(message, db_path=sqlite_path)
            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Mortality Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Mortality Producer shutting down.")

if __name__ == "__main__":
    main()
