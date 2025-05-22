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

def ensure_price_target_table():
    """Ensure the price_target_news table exists"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'price_target_news'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("Creating price_target_news table...")
            cursor.execute("""
                CREATE TABLE price_target_news (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    published_date TIMESTAMP,
                    news_url TEXT,
                    news_title TEXT,
                    analyst_name TEXT,
                    price_target FLOAT,
                    adj_price_target FLOAT,
                    price_when_posted FLOAT,
                    news_publisher TEXT,
                    news_base_url TEXT,
                    analyst_company TEXT
                )
            """)
            
            # Create index for better performance
            cursor.execute('CREATE INDEX idx_price_target_news_symbol ON price_target_news(symbol);')
            
            # Create unique constraint
            cursor.execute("""
                CREATE UNIQUE INDEX idx_price_target_news_unique 
                ON price_target_news(symbol, published_date, COALESCE(analyst_name, ''), price_target);
            """)
            
            print("Table created successfully")
        else:
            print("Table price_target_news already exists")
            
            # Check if unique index exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'idx_price_target_news_unique'
                )
            """)
            index_exists = cursor.fetchone()[0]
            
            if not index_exists:
                print("Adding unique constraint to prevent duplicates...")
                # Clear any duplicates first
                cursor.execute("""
                    DELETE FROM price_target_news
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id,
                                   ROW_NUMBER() OVER (PARTITION BY symbol, published_date, COALESCE(analyst_name, ''), price_target 
                                                     ORDER BY id) as row_num
                            FROM price_target_news
                        ) t
                        WHERE t.row_num > 1
                    )
                """)
                
                # Create unique constraint
                cursor.execute("""
                    CREATE UNIQUE INDEX idx_price_target_news_unique 
                    ON price_target_news(symbol, published_date, COALESCE(analyst_name, ''), price_target);
                """)
                print("Unique constraint added")
    except Exception as e:
        print(f"Error ensuring price_target_news table: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

def fetch_price_target_news(symbol):
    """Fetch price target news for a given symbol from the API."""
    url = f"{BASE_URL}/price-target-news"
    params = {
        "symbol": symbol,
        "page": 0,
        "limit": 10,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Save to database
        conn = connect_to_db()
        cursor = conn.cursor()
        
        try:
            inserted_count = 0
            for item in data:
                # Extract the fields we need
                symbol = item.get("symbol")
                published_date = item.get("publishedDate")
                news_url = item.get("newsURL")
                news_title = item.get("newsTitle")
                analyst_name = item.get("analystName", "") or ""  # Convert None to empty string
                price_target = item.get("priceTarget")
                adj_price_target = item.get("adjPriceTarget")
                price_when_posted = item.get("priceWhenPosted")
                news_publisher = item.get("newsPublisher")
                news_base_url = item.get("newsBaseURL", "")
                analyst_company = item.get("analystCompany", "") or ""  # Convert None to empty string
                
                try:
                    cursor.execute("""
                        INSERT INTO price_target_news 
                        (symbol, published_date, news_url, news_title, analyst_name, 
                         price_target, adj_price_target, price_when_posted, news_publisher, 
                         news_base_url, analyst_company)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        symbol, published_date, news_url, news_title, analyst_name,
                        price_target, adj_price_target, price_when_posted, news_publisher, 
                        news_base_url, analyst_company
                    ))
                    
                    if cursor.rowcount > 0:
                        inserted_count += 1
                except Exception as e:
                    print(f"Error inserting record: {e}")
            
            conn.commit()
            print(f"Processed {len(data)} price targets for {symbol}, inserted {inserted_count} new records")
            
            cursor.close()
            conn.close()
            return data
        except Exception as e:
            print(f"Error inserting price target news: {e}")
            conn.close()
            return data
    else:
        print(f"Error fetching price target news: {response.status_code}")
        return None

def display_price_targets(symbol):
    """Display price targets for a given symbol"""
    conn = connect_to_db()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT symbol, published_date, news_title, analyst_name, 
                       price_target, price_when_posted, analyst_company
                FROM price_target_news
                WHERE symbol = %s
                ORDER BY published_date DESC
            """, (symbol,))
            
            results = cursor.fetchall()
            
            if not results:
                print(f"No price targets found for {symbol}")
                return
            
            print(f"\nPrice targets for {symbol}:")
            print("=" * 80)
            
            for row in results:
                published_date = row['published_date'].strftime('%Y-%m-%d') if row['published_date'] else 'N/A'
                print(f"Date: {published_date}")
                print(f"Analyst: {row['analyst_name'] or 'N/A'} ({row['analyst_company'] or 'N/A'})")
                print(f"Target: ${row['price_target']:.2f} (Stock was ${row['price_when_posted']:.2f} when posted)")
                print(f"Title: {row['news_title'] or 'N/A'}")
                print("-" * 80)
    except Exception as e:
        print(f"Error displaying price targets: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Fetch and store price target data')
    parser.add_argument('symbols', type=str, help='Comma-separated list of ticker symbols (e.g., AAPL,MSFT,GOOGL)')
    parser.add_argument('--display-only', action='store_true', help='Only display stored price targets without fetching new data')
    
    args = parser.parse_args()
    
    # Parse symbols
    symbols = [s.strip() for s in args.symbols.split(',')]
    
    # Ensure table exists
    ensure_price_target_table()
    
    # Process each symbol
    for symbol in symbols:
        if not args.display_only:
            print(f"Fetching price targets for {symbol}...")
            fetch_price_target_news(symbol)
        
        display_price_targets(symbol)

if __name__ == "__main__":
    main() 