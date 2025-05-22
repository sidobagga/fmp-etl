#!/usr/bin/env python3
import requests
import psycopg2
import argparse
import json
import sys
from datetime import datetime
from psycopg2.extras import RealDictCursor

# API configuration
API_KEY = "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv"
BASE_URL = "https://financialmodelingprep.com/stable"

# PostgreSQL database details
PG_HOST = 'orbe360.ai'
PG_PORT = 5432
PG_USER = 'postgres'
PG_PASSWORD = 'Admin0rbE'
PG_DB = 'finmetrics'

def connect_to_db(db_name=None):
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=db_name or PG_DB
        )
        conn.autocommit = True
        print(f"Connected to PostgreSQL database: {db_name or PG_DB} on {PG_HOST}")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        sys.exit(1)

def ensure_stock_peers_table():
    """Ensure the stock_peers table exists"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'stock_peers'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("Creating stock_peers table...")
            cursor.execute("""
                CREATE TABLE stock_peers (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    peer_symbol TEXT NOT NULL,
                    company_name TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, peer_symbol)
                )
            """)
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX idx_stock_peers_symbol ON stock_peers(symbol)')
            cursor.execute('CREATE INDEX idx_stock_peers_peer_symbol ON stock_peers(peer_symbol)')
            
            print("Table created successfully")
        else:
            print("Table stock_peers already exists")
    except Exception as e:
        print(f"Error ensuring stock_peers table: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

def fetch_stock_peers(symbol):
    """Fetch stock peers for a given symbol from the API."""
    url = f"{BASE_URL}/stock-peers"
    params = {
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Save to database
        conn = connect_to_db()
        cursor = conn.cursor()
        
        try:
            # First, remove existing peers for this symbol
            cursor.execute("DELETE FROM stock_peers WHERE symbol = %s", (symbol,))
            
            inserted_count = 0
            for item in data:
                # Extract the fields we need
                peer_symbol = item.get("symbol")
                company_name = item.get("companyName")
                
                # Skip if peer_symbol is the same as the main symbol
                if peer_symbol == symbol:
                    continue
                
                try:
                    cursor.execute("""
                        INSERT INTO stock_peers 
                        (symbol, peer_symbol, company_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, peer_symbol) DO UPDATE 
                        SET company_name = EXCLUDED.company_name,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        symbol, peer_symbol, company_name
                    ))
                    
                    if cursor.rowcount > 0:
                        inserted_count += 1
                except Exception as e:
                    print(f"Error inserting record: {e}")
            
            conn.commit()
            print(f"Processed {len(data)} peers for {symbol}, inserted/updated {inserted_count} records")
            
            cursor.close()
            conn.close()
            return data
        except Exception as e:
            print(f"Error inserting stock peers: {e}")
            conn.close()
            return data
    else:
        print(f"Error fetching stock peers: {response.status_code}")
        return None

def fetch_peer_metrics(symbol):
    """Fetch financial metrics for a stock's peers"""
    conn = connect_to_db()
    
    try:
        # Get peers for the symbol
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT peer_symbol, company_name
                FROM stock_peers
                WHERE symbol = %s
            """, (symbol,))
            peers = cursor.fetchall()
            
            if not peers:
                print(f"No peers found for {symbol}")
                return None
            
            print(f"Found {len(peers)} peers for {symbol}")
            
            # Get metrics for each peer
            results = []
            for peer in peers:
                peer_symbol = peer[0]
                company_name = peer[1]
                
                print(f"Fetching metrics for {peer_symbol} ({company_name})...")
                
                # Get latest financial metrics from financial_metrics table
                cursor.execute("""
                    SELECT m.symbol, m.date, m.metric_values
                    FROM financial_metrics m
                    WHERE m.symbol = %s
                    AND m.metric_type = 'ratio'
                    ORDER BY m.date DESC
                    LIMIT 1
                """, (peer_symbol,))
                
                metric_row = cursor.fetchone()
                
                if not metric_row:
                    print(f"No metrics found for {peer_symbol}")
                    continue
                
                # Get latest income statement for revenue
                cursor.execute("""
                    SELECT m.symbol, m.date, m.metric_values
                    FROM financial_metrics m
                    WHERE m.symbol = %s
                    AND m.metric_type = 'income'
                    ORDER BY m.date DESC
                    LIMIT 1
                """, (peer_symbol,))
                
                income_row = cursor.fetchone()
                
                # Extract the metrics we need
                metrics = {
                    "symbol": peer_symbol,
                    "companyName": company_name,
                    "date": metric_row[1].strftime('%Y-%m-%d') if metric_row[1] else None
                }
                
                # Add ratio metrics if available
                if metric_row and metric_row[2]:
                    metric_values = json.loads(metric_row[2]) if isinstance(metric_row[2], str) else metric_row[2]
                    for key in ["grossProfitMargin", "ebitMargin", "ebitdaMargin", "operatingProfitMargin", 
                               "pretaxProfitMargin", "continuousOperationsProfitMargin", "netProfitMargin"]:
                        metrics[key] = metric_values.get(key)
                
                # Add revenue if available
                if income_row and income_row[2]:
                    income_values = json.loads(income_row[2]) if isinstance(income_row[2], str) else income_row[2]
                    metrics["revenue"] = income_values.get("revenue")
                
                results.append(metrics)
            
            return results
    except Exception as e:
        print(f"Error fetching peer metrics: {e}")
        return None
    finally:
        conn.close()

def display_stock_peers(symbol):
    """Display stock peers for a given symbol"""
    conn = connect_to_db()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT symbol, peer_symbol, company_name, updated_at
                FROM stock_peers
                WHERE symbol = %s
                ORDER BY peer_symbol
            """, (symbol,))
            
            results = cursor.fetchall()
            
            if not results:
                print(f"No peers found for {symbol}")
                return
            
            print(f"\nStock peers for {symbol}:")
            print("=" * 80)
            
            for row in results:
                updated_at = row['updated_at'].strftime('%Y-%m-%d') if row['updated_at'] else 'N/A'
                print(f"Peer: {row['peer_symbol']} - {row['company_name']}")
                print(f"Last updated: {updated_at}")
                print("-" * 80)
    except Exception as e:
        print(f"Error displaying stock peers: {e}")
    finally:
        conn.close()

def display_peer_metrics(symbol):
    """Display financial metrics for a stock's peers"""
    peers_metrics = fetch_peer_metrics(symbol)
    
    if not peers_metrics:
        return
    
    print(f"\nFinancial metrics for {symbol}'s peers:")
    print("=" * 100)
    
    # Print header
    print(f"{'Symbol':<8} {'Company':<30} {'Revenue':<15} {'Gross %':<10} {'EBIT %':<10} {'EBITDA %':<10} {'Net %':<10}")
    print("-" * 100)
    
    for metrics in peers_metrics:
        symbol = metrics.get('symbol', 'N/A')
        company = metrics.get('companyName', 'N/A')
        revenue = metrics.get('revenue')
        gross_margin = metrics.get('grossProfitMargin')
        ebit_margin = metrics.get('ebitMargin')
        ebitda_margin = metrics.get('ebitdaMargin')
        net_margin = metrics.get('netProfitMargin')
        
        # Format numbers
        revenue_str = f"${revenue/1000000:.1f}M" if revenue else 'N/A'
        gross_pct = f"{gross_margin*100:.1f}%" if gross_margin else 'N/A'
        ebit_pct = f"{ebit_margin*100:.1f}%" if ebit_margin else 'N/A'
        ebitda_pct = f"{ebitda_margin*100:.1f}%" if ebitda_margin else 'N/A'
        net_pct = f"{net_margin*100:.1f}%" if net_margin else 'N/A'
        
        print(f"{symbol:<8} {company[:29]:<30} {revenue_str:<15} {gross_pct:<10} {ebit_pct:<10} {ebitda_pct:<10} {net_pct:<10}")

def main():
    parser = argparse.ArgumentParser(description='Fetch and store stock peers data')
    parser.add_argument('symbols', type=str, help='Comma-separated list of ticker symbols (e.g., AAPL,MSFT,GOOGL)')
    parser.add_argument('--display-only', action='store_true', help='Only display stored peers without fetching new data')
    parser.add_argument('--metrics', action='store_true', help='Display financial metrics for peers')
    
    args = parser.parse_args()
    
    # Parse symbols
    symbols = [s.strip() for s in args.symbols.split(',')]
    
    # Ensure table exists
    ensure_stock_peers_table()
    
    # Process each symbol
    for symbol in symbols:
        if not args.display_only:
            print(f"Fetching peers for {symbol}...")
            fetch_stock_peers(symbol)
        
        display_stock_peers(symbol)
        
        if args.metrics:
            display_peer_metrics(symbol)

if __name__ == "__main__":
    main() 