#!/usr/bin/env python3
import sqlite3
import json
import os

# Use the same DB path as the main ETL script
DB_PATH = os.path.join('financial_data', 'financial_data.db')

def connect_to_db():
    """Connect to SQLite database"""
    conn = sqlite3.connect(DB_PATH)
    return conn

def ensure_financial_metrics_table():
    """Ensure the financial_metrics table exists"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        period TEXT,
        metric_type TEXT NOT NULL,
        metric_values TEXT,
        UNIQUE(symbol, date, metric_type)
    )
    ''')
    
    conn.commit()
    conn.close()

def add_sample_peer_ratios():
    """Add sample ratio data for AAPL's peers"""
    # Ensure the table exists
    ensure_financial_metrics_table()
    
    # Get list of AAPL's peers
    conn = connect_to_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT peer_symbol FROM stock_peers WHERE symbol = 'AAPL'")
    peers = [row[0] for row in cursor.fetchall()]
    
    if not peers:
        print("No peers found for AAPL")
        conn.close()
        return
    
    print(f"Found {len(peers)} peers for AAPL: {', '.join(peers)}")
    
    # Sample ratio data with varied metrics for each peer
    for i, peer in enumerate(peers):
        # Create different values for each peer to make the results interesting
        base_value = 0.2 + (i * 0.05)
        
        ratio_data = {
            "grossProfitMargin": round(base_value + 0.1, 3),
            "ebitMargin": round(base_value, 3),
            "ebitdaMargin": round(base_value + 0.15, 3),
            "operatingProfitMargin": round(base_value - 0.05, 3),
            "pretaxProfitMargin": round(base_value - 0.02, 3),
            "netProfitMargin": round(base_value - 0.07, 3),
            "continuousOperationsProfitMargin": round(base_value - 0.01, 3)
        }
        
        # Sample income data with revenue
        income_data = {
            "revenue": 1000000000 + (i * 500000000),
            "totalRevenue": 1000000000 + (i * 500000000)
        }
        
        try:
            # Insert ratio data
            cursor.execute('''
            INSERT OR REPLACE INTO financial_metrics 
            (symbol, date, metric_type, metric_values)
            VALUES (?, ?, ?, ?)
            ''', (peer, '2023-12-31', 'ratio', json.dumps(ratio_data)))
            
            # Insert income data
            cursor.execute('''
            INSERT OR REPLACE INTO financial_metrics 
            (symbol, date, metric_type, metric_values)
            VALUES (?, ?, ?, ?)
            ''', (peer, '2023-12-31', 'income', json.dumps(income_data)))
            
            print(f"Added sample data for {peer}")
        except Exception as e:
            print(f"Error adding data for {peer}: {e}")
    
    conn.commit()
    conn.close()
    print("Sample peer ratio data added successfully")

if __name__ == "__main__":
    add_sample_peer_ratios() 