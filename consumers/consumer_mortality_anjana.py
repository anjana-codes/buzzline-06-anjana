"""
consumer_mortality.py

Mortality Analytics Consumer with Real-time Gender and Regional Visualization for Heart Disease

This consumer processes mortality data streams from data/project_live.json and provides:
1. Real-time mortality rate analysis by region, gender, cause, status, and sex
2. Bar chart of average Heart disease mortality rates by gender
3. Line chart of Heart disease mortality rate trends across three regions
4. High mortality rate detection and alerting

Key Insights:
- Average Heart disease mortality rates by gender (Male vs. Female)
- Regional trends for Heart disease mortality rates
- Identification of high mortality events for public health prioritization
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
            keyword_mentioned TEXT,
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
            "keyword_mentioned": message.get("keyword_mentioned")
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
    keyword = data["keyword_mentioned"]
    
    # Determine if high mortality (rate >= 100)
    is_high_mortality = rate >= 100
    
    # Update real-time data for visualization
    if cause == "Heart disease":
        mortality_rates[("Heart disease", region)].append(rate)  # For regional trends
        mortality_rates[("Heart disease", sex)].append(rate)     # For gender bar chart
    
    # Track analytics by cause, region, and sex
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
        (region, status, sex, cause, rate, se, timestamp, keyword_mentioned, is_high_mortality)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        region, status, sex, cause, rate, se, data["timestamp"], keyword, is_high_mortality
    ))
    
    conn.commit()
    conn.close()
    
    print(f"📊 {region} {status} {sex} {cause}: Rate {rate}, SE {se}, Keyword: {keyword}, "
          f"Message: {message['message']}")

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
    """Setup matplotlib for dynamic visualization."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('Real-time Mortality Analytics Dashboard (Heart Disease)', fontsize=16, fontweight='bold')
    
    return fig, (ax1, ax2)

def animate_dashboard(frame, fig, axes):
    """Animation function for real-time dashboard updates."""
    ax1, ax2 = axes
    
    # Clear all axes
    ax1.clear()
    ax2.clear()
    
    # Chart 1: Bar Chart - Average Heart Disease Mortality Rates by Gender
    ax1.set_title('Average Heart Disease Mortality Rates by Gender', fontweight='bold')
    ax1.set_ylabel('Average Rate (per 100,000)')
    ax1.set_xlabel('Gender')
    
    selected_cause = "Heart disease"
    if selected_cause in cause_gender_stats:
        genders = ["Male", "Female"]
        gender_rates = [sum(cause_gender_stats[selected_cause].get(gender, [])) / 
                        len(cause_gender_stats[selected_cause].get(gender, [1]))
                        if cause_gender_stats[selected_cause].get(gender) else 0 
                        for gender in genders]
        
        bars = ax1.bar(genders, gender_rates, color=['#4682B4', '#FF69B4'])
        ax1.set_ylim(0, max(gender_rates, default=100) * 1.2)
        
        # Add value labels
        for bar, rate in zip(bars, gender_rates):
            if rate > 0:
                ax1.text(bar.get_x() + bar.get_width()/2, rate + 1, f'{rate:.1f}', 
                         ha='center', fontweight='bold')
        
        ax1.grid(True, alpha=0.3, axis='y')
    
    # Chart 2: Line Chart - Heart Disease Mortality Rate Trends by Region
    ax2.set_title('Heart Disease Mortality Rate Trends by Region', fontweight='bold')
    ax2.set_ylabel('Mortality Rate (per 100,000)')
    ax2.set_xlabel('Time (Recent Readings)')
    
    regions = sorted(set(region for cause in cause_region_stats for region in cause_region_stats[cause]))
    for region in regions:
        key = (selected_cause, region)
        if key in mortality_rates and mortality_rates[key]:
            ax2.plot(range(len(mortality_rates[key])), mortality_rates[key], 'o-',
                    label=region, linewidth=2, markersize=4,
                    color=['#1f77b4', '#ff7f0e', '#2ca02c'][regions.index(region) % 3])
    
    ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    # Add high mortality counter
    fig.text(0.02, 0.02, f'High Mortality Events (Rate ≥ 100): {high_mortality_count}', 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="#FF4C4C", alpha=0.3),
             fontsize=12, fontweight='bold')
    
    plt.tight_layout()

def main():
    """Main consumer with real-time visualization."""
    print("=== Mortality Analytics Consumer (Heart Disease, Gender and Regional Insights) ===")
    print("Real-time processing of mortality data with gender and regional visualizations")
    print(f"Reading from: {DATA_FILE}")
    print(f"Storing analytics in: {DB_FILE}")
    print("Starting dynamic visualization...")
    
    # Initialize database
    init_db(DB_FILE)
    
    # Setup matplotlib for real-time plotting
    plt.ion()  # Interactive mode
    fig, axes = setup_dynamic_plot()
    
    # Create animation
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
        print("\n🛑 Mortality Analytics Consumer stopped by user")
    except Exception as e:
        print(f"❌ Error in consumer: {e}")
    finally:
        plt.ioff()
        print("Consumer shutting down...")
        print(f"📈 Total high mortality events detected: {high_mortality_count}")
        print(f"📊 Processed data for {len(cause_region_stats)} causes")

if __name__ == "__main__":
    main()