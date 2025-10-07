"""
consumer_mortality_anjana.py

Mortality Analytics Consumer with Real-time Gender and Regional Visualization
"""

import json
import sqlite3
import pathlib
import time
import os
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque, defaultdict
from typing import Optional, Dict, Any, List

# Configuration
DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/mortality_analytics.sqlite')
HIGH_RATE_THRESHOLD = float(os.getenv("HIGH_RATE_THRESHOLD", 100))  # dynamic threshold

# Real-time data storage
mortality_rates = defaultdict(lambda: deque(maxlen=20))
cause_gender_stats = defaultdict(lambda: defaultdict(list))
cause_region_stats = defaultdict(lambda: defaultdict(list))
high_mortality_count = 0
processed_messages = set()


def init_db(db_file: pathlib.Path) -> None:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mortality_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT,
            status TEXT,
            sex TEXT,
            cause TEXT,
            rate REAL,
            se REAL,
            timestamp TEXT,
            is_high_mortality BOOLEAN,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Mortality analytics database initialized at {db_file}")


def extract_mortality_data(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        return {
            "region": message.get("region"),
            "status": message.get("status"),
            "sex": message.get("sex"),
            "cause": message.get("cause"),
            "rate": float(message.get("rate", 0)),
            "se": float(message.get("se", 0)),
            "timestamp": message.get("timestamp"),
            "author": message.get("author"),
        }
    except (ValueError, TypeError):
        print("⚠️ Could not extract mortality data from message")
        return None


def process_and_store_message(message: Dict[str, Any], db_file: pathlib.Path) -> None:
    global high_mortality_count
    data = extract_mortality_data(message)
    if not data:
        return

    region, status, sex, cause, rate, se = (
        data["region"],
        data["status"],
        data["sex"],
        data["cause"],
        data["rate"],
        data["se"],
    )

    is_high_mortality = rate >= HIGH_RATE_THRESHOLD

    mortality_rates[(cause, region)].append(rate)
    mortality_rates[(cause, sex)].append(rate)
    cause_region_stats[cause][region].append(rate)
    cause_gender_stats[cause][sex].append(rate)

    if is_high_mortality:
        high_mortality_count += 1
        print(f"🚨 HIGH MORTALITY DETECTED: {region} {status} {sex} {cause}: Rate {rate}")

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO mortality_analytics 
        (region, status, sex, cause, rate, se, timestamp, is_high_mortality)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        region, status, sex, cause, rate, se, data["timestamp"], is_high_mortality
    ))
    conn.commit()
    conn.close()

    print(f"📊 {region} {status} {sex} {cause}: Rate {rate}, SE {se}, Message: {message['message']}")


def read_latest_messages(data_file: pathlib.Path) -> List[Dict[str, Any]]:
    global processed_messages
    if not data_file.exists():
        return []

    new_messages = []
    try:
        with open(data_file, 'r') as f:
            lines = f.readlines()
        for line in lines:
            if line.strip():
                message = json.loads(line.strip())
                msg_id = f"{message.get('author')}_{message.get('timestamp')}"
                if msg_id not in processed_messages:
                    new_messages.append(message)
                    processed_messages.add(msg_id)
    except Exception as e:
        print(f"Error reading messages: {e}")

    return new_messages


def setup_dynamic_plot():
    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(12, 18), constrained_layout=True)
    fig.suptitle('Real-time Mortality Analytics Dashboard', fontsize=18, fontweight='bold')
    return fig, (ax0, ax1)


def animate_dashboard(frame, fig, axes):
    ax0, ax1 = axes
    ax0.cla()
    ax1.cla()

    # Chart 1: Mortality by Cause and Gender
    ax0.set_title('Average Mortality Rate by Cause and Gender', fontweight='bold')
    ax0.set_ylabel('Average Mortality Rate')
    ax0.set_xlabel('Cause')

    causes = list(cause_gender_stats.keys())
    genders = ["Male", "Female"]

    if causes:
        bar_width = 0.3
        index = range(len(causes))
        male_rates = []
        female_rates = []

        for cause in causes:
            male_avg = sum(cause_gender_stats[cause].get("Male", [])) / max(len(cause_gender_stats[cause].get("Male", [])), 1)
            female_avg = sum(cause_gender_stats[cause].get("Female", [])) / max(len(cause_gender_stats[cause].get("Female", [])), 1)
            male_rates.append(male_avg)
            female_rates.append(female_avg)

        # Bars
        bars_male = ax0.bar([i - bar_width/2 for i in index], male_rates, width=bar_width, color='#4682B4', label='Male')
        bars_female = ax0.bar([i + bar_width/2 for i in index], female_rates, width=bar_width, color='#FF69B4', label='Female')

        # Text labels for individual bars
        for i, (m, f) in enumerate(zip(male_rates, female_rates)):
            ax0.text(i - bar_width/2, m + 1, f"{m:.1f}", ha='center', fontsize=8, fontweight='bold')
            ax0.text(i + bar_width/2, f + 1, f"{f:.1f}", ha='center', fontsize=8, fontweight='bold')

    
        ax0.set_xticks(index)
        ax0.set_xticklabels(causes, rotation=45, ha='right')
        ax0.legend()
        ax0.grid(True, alpha=0.3, axis='y')

    # Chart 2: Heart Disease Trends
    ax1.set_title('Heart Disease Mortality Rate Trends by HHS Region', fontweight='bold')
    ax1.set_ylabel('Mortality Rate')
    ax1.set_xlabel('Recent Readings')

    regions = sorted(set(region for cause in cause_region_stats for region in cause_region_stats[cause]))
    for region in regions:
        key = ("Heart disease", region)
        if key in mortality_rates and mortality_rates[key]:
            ax1.plot(range(len(mortality_rates[key])), mortality_rates[key], 'o-', label=region)

    ax1.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=9, title="HHS Region")
    ax1.grid(True, alpha=0.3)

def main():
    print("=== Mortality Analytics Consumer ===")
    print(f"Reading from: {DATA_FILE}")
    print(f"Storing analytics in: {DB_FILE}")
    print(f"Using HIGH_RATE_THRESHOLD = {HIGH_RATE_THRESHOLD}")
    print("Starting dynamic visualization... (Press Ctrl+C to stop)")

    init_db(DB_FILE)
    plt.ion()
    fig, axes = setup_dynamic_plot()

    ani = animation.FuncAnimation(fig, animate_dashboard, fargs=(fig, axes),
                                  interval=2000, cache_frame_data=False)

    try:
        while True:
            messages = read_latest_messages(DATA_FILE)
            for message in messages:
                process_and_store_message(message, DB_FILE)
            plt.pause(0.1)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    finally:
        plt.ioff()
        print(f"📈 Total high mortality events detected: {high_mortality_count}")


if __name__ == "__main__":
    main() 
