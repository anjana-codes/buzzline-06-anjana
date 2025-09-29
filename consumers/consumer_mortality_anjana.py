"""
consumer_mortality_anjana.py

Mortality Analytics Consumer with Real-time Gender and Regional Visualization

This consumer processes mortality data streams from data/project_live.json and provides:
1. Real-time mortality rate analysis by region, gender, cause, status, and sex
2. New Bar chart: Average mortality rate by cause (split by gender)
3. Bar chart: Average Heart disease mortality rates by gender
4. Line chart: Heart disease mortality rate trends across three regions
5. High mortality rate detection and alerting
"""

import json
import sqlite3
import pathlib
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque, defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, List

# Configuration
DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/mortality_analytics.sqlite')

# Real-time data storage
mortality_rates = defaultdict(lambda: deque(maxlen=20))  # Last 20 readings per cause/region or cause/sex
cause_gender_stats = defaultdict(lambda: defaultdict(list))  # Track rates by cause and sex
cause_region_stats = defaultdict(lambda: defaultdict(list))  # Track rates by cause and region
high_mortality_count = 0  # Counter for high mortality alerts
processed_messages = set()


def init_db(db_file: pathlib.Path) -> None:
    """Initialize SQLite database for mortality analytics."""
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
    """Extract mortality data from message."""
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
    """Process mortality message and store analytics."""
    global high_mortality_count

    data = extract_mortality_data(message)
    if not data:
        return

    region = data["region"]
    status = data["status"]
    sex = data["sex"]
    cause = data["cause"]
    rate = data["rate"]
    se = data["se"]

    # Determine if high mortality (rate >= 100)
    is_high_mortality = rate >= 100

    # Update real-time data for visualization
    mortality_rates[(cause, region)].append(rate)
    mortality_rates[(cause, sex)].append(rate)
    cause_region_stats[cause][region].append(rate)
    cause_gender_stats[cause][sex].append(rate)

    if is_high_mortality:
        high_mortality_count += 1
        print(f"🚨 HIGH MORTALITY DETECTED: {region} {status} {sex} {cause}: Rate {rate}")

    # Store in SQLite
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
    """Read all new messages from the live data file."""
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

    except (json.JSONDecodeError, Exception) as e:
        print(f"Error reading messages: {e}")

    return new_messages


def setup_dynamic_plot():
    """Setup matplotlib for three charts."""
    fig, (ax0, ax1, ax2) = plt.subplots(3, 1, figsize=(12, 14))
    fig.suptitle('Real-time Mortality Analytics Dashboard', fontsize=16, fontweight='bold')
    return fig, (ax0, ax1, ax2)


def animate_dashboard(frame, fig, axes):
    ax0, ax1, ax2 = axes
    ax0.clear()
    ax1.clear()
    ax2.clear()

    # Chart 0: Real Time Mortality Dash board 
    ax0.set_title('Average Mortality Rate by Cause and Split by Gender', fontweight='bold')
    ax0.set_ylabel('Average Rate')
    ax0.set_xlabel('Cause')

    causes = list(cause_gender_stats.keys())
    genders = ["Male", "Female"]

    if causes:
        bar_width = 0.35
        index = range(len(causes))
        male_rates = []
        female_rates = []

        for cause in causes:
            male_rates.append(
                sum(cause_gender_stats[cause].get("Male", [])) / max(len(cause_gender_stats[cause].get("Male", [])), 1)
            )
            female_rates.append(
                sum(cause_gender_stats[cause].get("Female", [])) / max(len(cause_gender_stats[cause].get("Female", [])), 1)
            )

        ax0.bar([i - bar_width/2 for i in index], male_rates, width=bar_width, color='#4682B4', label='Male')
        ax0.bar([i + bar_width/2 for i in index], female_rates, width=bar_width, color='#FF69B4', label='Female')

        ax0.set_xticks(index)
        ax0.set_xticklabels(causes, rotation=45, ha='right')
        ax0.legend()
        ax0.grid(True, alpha=0.3, axis='y')

    # Chart 1: Average Mortality Rates by Cause (Heart Disease)
    ax1.set_title('Average Heart Disease Mortality Rates by Gender', fontweight='bold')
    ax1.set_ylabel('Rate')
    ax1.set_xlabel('Gender')

    selected_cause = "Heart disease"
    if selected_cause in cause_gender_stats:
        genders = ["Male", "Female"]
        gender_rates = [
            sum(cause_gender_stats[selected_cause].get(gender, [])) /
            len(cause_gender_stats[selected_cause].get(gender, [1]))
            if cause_gender_stats[selected_cause].get(gender) else 0
            for gender in genders
        ]
        bars = ax1.bar(genders, gender_rates, color=['#4682B4', '#FF69B4'])
        for bar, rate in zip(bars, gender_rates):
            if rate > 0:
                ax1.text(bar.get_x() + bar.get_width()/2, rate + 1, f'{rate:.1f}', ha='center', fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')

    # Chart 2: Mortality Rate Trends by Region (cause: Heart disease)
    ax2.set_title('Heart Disease Mortality Rate Trends by Region', fontweight='bold')
    ax2.set_ylabel('Mortality Rate')
    ax2.set_xlabel('Recent Readings')

    regions = sorted(set(region for cause in cause_region_stats for region in cause_region_stats[cause]))
    for region in regions:
        key = ("Heart disease", region)
        if key in mortality_rates and mortality_rates[key]:
            ax2.plot(range(len(mortality_rates[key])), mortality_rates[key], 'o-', label=region)
    ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax2.grid(True, alpha=0.3)

    fig.text(0.02, 0.02, f'High Mortality Events (≥100): {high_mortality_count}',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="#FF4C4C", alpha=0.3),
             fontsize=12, fontweight='bold')

    plt.tight_layout()


def main():
    print("=== Mortality Analytics Consumer ===")
    print(f"Reading from: {DATA_FILE}")
    print(f"Storing analytics in: {DB_FILE}")
    print("Starting dynamic visualization...")

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

            if not messages:
                print("⏳ Waiting for new mortality data...")
            plt.pause(0.1)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    except Exception as e:
        print(f"❌ Error in consumer: {e}")
    finally:
        plt.ioff()
        print("Consumer shutting down...")
        print(f"📈 Total high mortality events detected: {high_mortality_count}")


if __name__ == "__main__":
    main()
