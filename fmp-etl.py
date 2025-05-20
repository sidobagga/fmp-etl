#!/usr/bin/env python3
import requests
import pandas as pd
import os
import json
import time
import sys
import psycopg2
import sqlite3
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
from pathlib import Path
import concurrent.futures
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Financial data ETL process')
parser.add_argument('--symbols', type=str, help='Comma-separated list of ticker symbols (e.g., AAPL,MSFT,GOOGL)')
parser.add_argument('--db-type', type=str, choices=['sqlite', 'postgres'], default='sqlite', help='Database type to use (default: sqlite)')
parser.add_argument('--limit', type=int, default=3, help='Number of symbols to process (default: 3)')
parser.add_argument('--db-name', type=str, help='Database name for PostgreSQL (default: finmetrics)')
args = parser.parse_args()

# API configuration
API_KEY = "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv"
BASE_URL = "https://financialmodelingprep.com/stable"

# Database configuration
USE_SQLITE = args.db_type == 'sqlite'  # Default to SQLite for local testing
SQLITE_DB_PATH = os.path.join('financial_data', 'financial_data.db')

# PostgreSQL database details (only used if USE_SQLITE is False)
PG_HOST = 'orbe360.ai'
PG_PORT = 5432
PG_USER = 'postgres'
PG_PASSWORD = 'Admin0rbE'
PG_DB = args.db_name or 'finmetrics'

# Configuration
# Default list of 25+ major companies to analyze
DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", 
    "TSLA", "NVDA", "JPM", "V", "JNJ",
    "WMT", "PG", "HD", "BAC", "INTC",
    "VZ", "DIS", "CSCO", "XOM", "NFLX",
    "ADBE", "CRM", "PYPL", "CMCSA", "PEP"
]

# Use command line arguments if provided, otherwise use default symbols (limited by args.limit)
if args.symbols:
    SYMBOLS = args.symbols.split(',')
else:
    SYMBOLS = DEFAULT_SYMBOLS[:min(args.limit, len(DEFAULT_SYMBOLS))]
    
MAX_CONCURRENT_REQUESTS = 3  # Limit concurrent API requests to avoid rate limiting

YEARS = list(range(2020, 2026))  # 2020 to 2025
QUARTERS = [1, 2, 3, 4]

# Create output directory
OUTPUT_DIR = "financial_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Create subdirectories for each data type
for subdir in ["raw", "csv", "consolidated"]:
    os.makedirs(os.path.join(OUTPUT_DIR, subdir), exist_ok=True)

# API endpoints to fetch
ENDPOINTS = {
    "earning_call_transcript": {
        "url": "/earning-call-transcript",
        "params": ["symbol", "year", "quarter"],
        "quarterly": True,
        "table_name": "earnings_transcripts",
        "metric_type": "transcript"
    },
    "cash_flow_statement": {
        "url": "/cash-flow-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period",
        "table_name": "cash_flow_statements",
        "metric_type": "cash_flow"
    },
    "balance_sheet_statement": {
        "url": "/balance-sheet-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period",
        "table_name": "balance_sheets",
        "metric_type": "balance"
    },
    "income_statement": {
        "url": "/income-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period",
        "table_name": "income_statements",
        "metric_type": "income"
    },
    "ratios": {
        "url": "/ratios",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period",
        "table_name": "financial_ratios",
        "metric_type": "ratio"
    },
    "analyst_estimates": {
        "url": "/analyst-estimates",
        "params": ["symbol", "period"],
        "additional_params": {"page": 0, "limit": 10},
        "quarterly": True,
        "table_name": "analyst_estimates",
        "metric_type": "analyst"
    },
    "news_press_releases": {
        "url": "/news/press-releases",
        "params": ["symbols"],
        "quarterly": False,
        "table_name": "company_news",
        "metric_type": "news"
    }
}

# Table mappings for consolidation
FINANCIAL_TABLES = {
    'income': 'income_statements',
    'balance': 'balance_sheets',
    'cash_flow': 'cash_flow_statements',
    'ratio': 'financial_ratios',
    'analyst': 'analyst_estimates'
}

TEXT_TABLES = {
    'transcript': 'earnings_transcripts',
    'news': 'company_news'
}

# Columns to exclude from metric values
EXCLUDED_COLUMNS = [
    'symbol', 'date', 'period', 'reportedcurrency', 'fiscalyear', 
    'data_source', 'cik', 'filingdate', 'accepteddate'
]

def connect_to_db(db_name=None):
    """Connect to database (PostgreSQL or SQLite)"""
    if USE_SQLITE:
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
            conn = sqlite3.connect(SQLITE_DB_PATH)
            print(f"Connected to SQLite database: {SQLITE_DB_PATH}")
            return conn
        except sqlite3.Error as e:
            print(f"SQLite connection error: {e}")
            sys.exit(1)
    else:
        try:
            # First try to connect to the target database
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
            except psycopg2.OperationalError as e:
                if "does not exist" in str(e) and db_name:
                    print(f"Database {db_name} does not exist, trying to create it...")
                    # Connect to default 'postgres' database to create the new database
                    conn = psycopg2.connect(
                        host=PG_HOST,
                        port=PG_PORT,
                        user=PG_USER,
                        password=PG_PASSWORD,
                        database="postgres"
                    )
                    conn.autocommit = True
                    
                    # Create the database
                    with conn.cursor() as cursor:
                        cursor.execute(f"CREATE DATABASE {db_name}")
                    conn.close()
                    
                    # Now connect to the newly created database
                    conn = psycopg2.connect(
                        host=PG_HOST,
                        port=PG_PORT,
                        user=PG_USER,
                        password=PG_PASSWORD,
                        database=db_name
                    )
                    conn.autocommit = True
                    print(f"Created and connected to PostgreSQL database: {db_name} on {PG_HOST}")
                    return conn
                else:
                    raise
        except Exception as e:
            print(f"PostgreSQL connection error: {e}")
            sys.exit(1)

def fetch_api_data(endpoint_name, endpoint_config, symbol, year=None, quarter=None, period=None):
    """Fetch data from the FMP API for a specific endpoint, symbol, year, and quarter"""
    url = f"{BASE_URL}{endpoint_config['url']}"
    
    # Build parameters
    params = {"apikey": API_KEY}
    
    if "symbol" in endpoint_config["params"]:
        params["symbol"] = symbol
    if "symbols" in endpoint_config["params"]:
        params["symbols"] = symbol
    
    if year is not None and "year" in endpoint_config["params"]:
        params["year"] = year
    
    if quarter is not None and "quarter" in endpoint_config["params"]:
        params["quarter"] = quarter
    
    # Handle period parameter (annual vs quarterly)
    if period and "period_param" in endpoint_config:
        params[endpoint_config["period_param"]] = period
    elif "period" in endpoint_config["params"]:
        params["period"] = period if period else "annual"
            
    # Add any additional parameters
    if "additional_params" in endpoint_config:
        params.update(endpoint_config["additional_params"])
    
    try:
        print(f"Fetching {endpoint_name} for {symbol}" + 
              (f" for year {year}" if year else "") + 
              (f" quarter {quarter}" if quarter else "") +
              (f" period {period}" if period else ""))
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw JSON for debugging purposes
            save_raw_json(data, endpoint_name, symbol, year, quarter, period)
            
            return data
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def save_raw_json(data, endpoint_name, symbol, year=None, quarter=None, period=None):
    """Save raw JSON data for debugging"""
    if not data:
        return
    
    # Create a filename
    filename_parts = [endpoint_name, symbol]
    if year:
        filename_parts.append(f"Y{year}")
    if quarter:
        filename_parts.append(f"Q{quarter}")
    if period:
        filename_parts.append(period)
    
    filename = "_".join(filename_parts) + ".json"
    filepath = os.path.join(OUTPUT_DIR, "raw", filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def save_to_csv(data, endpoint_name, symbol, year=None, quarter=None, period=None):
    """Save API response data to CSV"""
    if not data:
        return None
    
    # Create a filename based on the endpoint, symbol, year, and quarter
    filename_parts = [endpoint_name, symbol]
    if year:
        filename_parts.append(f"Y{year}")
    if quarter:
        filename_parts.append(f"Q{quarter}")
    if period:
        filename_parts.append(period)
    
    filename = "_".join(filename_parts) + ".csv"
    filepath = os.path.join(OUTPUT_DIR, "csv", filename)
    
    # Convert to DataFrame and save
    try:
        # Handle different data structures
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # If it's a single record, convert to a list with one item
            df = pd.DataFrame([data])
        else:
            print(f"Unrecognized data format for {endpoint_name}")
            return None
        
        if not df.empty:
            # Add metadata columns if not present
            if "symbol" not in df.columns:
                df["symbol"] = symbol
            if year is not None and "year" not in df.columns:
                df["year"] = year
            if quarter is not None and "quarter" not in df.columns:
                df["quarter"] = quarter
            if period is not None and "period" not in df.columns:
                df["period"] = period
            
            # Add data source column
            df["data_source"] = endpoint_name
            
            df.to_csv(filepath, index=False)
            print(f"Saved to {filepath}")
            return df
        else:
            print(f"No data to save for {endpoint_name}")
            return None
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return None

def create_consolidated_files():
    """Consolidate individual CSV files into endpoint-specific consolidated files"""
    consolidated_dir = os.path.join(OUTPUT_DIR, "consolidated")
    csv_dir = os.path.join(OUTPUT_DIR, "csv")
    
    # Standardized mapping from API endpoint names to database table names
    endpoint_mapping = {
        'income_statement': 'income_statements',
        'balance_sheet_statement': 'balance_sheets',
        'cash_flow_statement': 'cash_flow_statements',
        'ratios': 'financial_ratios',
        'analyst_estimates': 'analyst_estimates',
        'earning_call_transcript': 'earnings_transcripts',
        'news_press_releases': 'company_news'
    }
    
    # Group files by endpoint
    endpoints = {}
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):
            endpoint_name = file.split("_")[0]
            if endpoint_name not in endpoints:
                endpoints[endpoint_name] = []
            endpoints[endpoint_name].append(os.path.join(csv_dir, file))
    
    # Consolidate each endpoint's files
    for endpoint, files in endpoints.items():
        if not files:
            continue
            
        print(f"Consolidating {len(files)} files for {endpoint}")
        
        dfs = []
        for file in files:
            try:
                df = pd.read_csv(file)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        if dfs:
            consolidated = pd.concat(dfs, ignore_index=True)
            
            # Use standardized table names for consistent file naming
            for key, value in endpoint_mapping.items():
                if endpoint.startswith(key) or key.startswith(endpoint):
                    # Create two files:
                    # 1. A file with the API endpoint name for readability
                    api_output_path = os.path.join(consolidated_dir, f"{key}_all_data.csv")
                    # 2. A file with the table name for direct DB import
                    table_output_path = os.path.join(consolidated_dir, f"{value}_all_data.csv")
                    
                    # Save both files
                    consolidated.to_csv(api_output_path, index=False)
                    consolidated.to_csv(table_output_path, index=False)
                    print(f"Created consolidated files: {api_output_path} and {table_output_path}")
                    break
            else:
                # If no match found, just save with original name
                output_path = os.path.join(consolidated_dir, f"{endpoint}_all_data.csv")
                consolidated.to_csv(output_path, index=False)
                print(f"Created consolidated file: {output_path}")

def create_master_csv():
    """Create a single CSV file with data from all sources"""
    print("Creating master CSV file with all data...")
    
    csv_dir = os.path.join(OUTPUT_DIR, "csv")
    all_dfs = []
    
    # Read all CSV files
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):
            try:
                file_path = os.path.join(csv_dir, file)
                df = pd.read_csv(file_path)
                
                # Extract metadata from filename
                parts = file.replace(".csv", "").split("_")
                endpoint = parts[0]
                
                # Add endpoint if not already present
                if "data_source" not in df.columns:
                    df["data_source"] = endpoint
                
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
    
    if all_dfs:
        # Create master dataframe
        master_df = pd.concat(all_dfs, ignore_index=True)
        
        # Standardize column names to lowercase with underscores
        master_df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in master_df.columns]
        
        # Common identifier columns to move to the front
        id_columns = ["data_source", "symbol", "year", "quarter", "period", "date"]
        front_cols = [col for col in id_columns if col in master_df.columns]
        other_cols = [col for col in master_df.columns if col not in front_cols]
        master_df = master_df[front_cols + other_cols]
        
        # Save master CSV
        master_path = os.path.join(OUTPUT_DIR, "master_financial_data.csv")
        master_df.to_csv(master_path, index=False)
        print(f"Created master CSV file with {len(master_df)} rows at {master_path}")
        
        # Generate a sample with a subset of columns for preview
        sample_cols = front_cols + other_cols[:min(10, len(other_cols))]
        sample_df = master_df[sample_cols].head(100)
        sample_path = os.path.join(OUTPUT_DIR, "sample_financial_data.csv")
        sample_df.to_csv(sample_path, index=False)
        print(f"Created sample CSV file for preview at {sample_path}")
        
        return master_df
    else:
        print("No data to consolidate into master CSV")
        return None

def get_quarter_from_period(period):
    """Extract quarter number from period string"""
    if not period:
        return None
    
    if period.startswith('Q'):
        try:
            return int(period[1])
        except (ValueError, IndexError):
            return None
    return None

def create_database_schema():
    """Create database schema (SQLite or PostgreSQL)"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        if USE_SQLITE:
            # SQLite schema
            # Create financial_metrics table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                reportedcurrency TEXT,
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                data_source TEXT,
                metric_values TEXT,
                metric_type TEXT NOT NULL
            );
            """)
            
            # Create text_metrics table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS text_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                metric_type TEXT NOT NULL,
                title TEXT,
                content TEXT,
                metadata TEXT
            );
            """)
            
            # Create traditional tables for each data type with all possible columns
            # Income statements - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS income_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                reportedcurrency TEXT,
                fiscalyear INTEGER,
                revenue REAL,
                costofrevenue REAL,
                grossprofit REAL,
                researchanddevelopmentexpenses REAL,
                generalandadministrativeexpenses REAL,
                sellingandmarketingexpenses REAL,
                operatingexpenses REAL,
                operatingincome REAL,
                interestexpense REAL,
                ebitda REAL,
                ebitdaratio REAL,
                netincome REAL,
                eps REAL,
                epsdiluted REAL,
                weightedaverageshsout REAL,
                weightedaverageshsoutdil REAL,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER,
                cik TEXT,
                fillingdate TEXT,
                accepteddate TEXT,
                calendarYear TEXT,
                link TEXT,
                finallink TEXT,
                income_before_tax REAL,
                income_tax_expense REAL,
                gross_profit_ratio REAL,
                comprehensive_income_net_of_tax REAL,
                effective_tax_rate REAL
            );
            """)
            
            # Balance sheets - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                reportedcurrency TEXT,
                fiscalyear INTEGER,
                cashandcashequivalents REAL,
                shortterminvestments REAL,
                cashandshortterminvestments REAL,
                netreceivables REAL,
                inventory REAL,
                totalcurrentassets REAL,
                propertyplantequipmentnet REAL,
                goodwill REAL,
                intangibleassets REAL,
                totalassets REAL,
                accountspayable REAL,
                shorttermdebt REAL,
                totalcurrentliabilities REAL,
                longtermdebt REAL,
                totalliabilities REAL,
                totalstockholdersequity REAL,
                totaldebt REAL,
                netdebt REAL,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER,
                cik TEXT,
                fillingdate TEXT,
                accepteddate TEXT,
                calendarYear TEXT,
                link TEXT,
                finallink TEXT,
                other_current_assets REAL,
                other_non_current_assets REAL,
                deferred_revenue REAL,
                deferred_tax_liabilities_non_current REAL,
                preferred_stock REAL,
                common_stock REAL,
                retained_earnings REAL,
                accumulated_other_comprehensive_income_loss REAL,
                othertotalstockholdersequity REAL,
                totalinvestments REAL,
                totalliabilitiesandtotalequity REAL
            );
            """)
            
            # Cash flow statements - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS cash_flow_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                reportedcurrency TEXT,
                fiscalyear INTEGER,
                netincome REAL,
                depreciationandamortization REAL,
                stockbasedcompensation REAL,
                changeinworkingcapital REAL,
                cashfromoperations REAL,
                capitalexpenditure REAL,
                acquisitionsnet REAL,
                cashfrominvesting REAL,
                debtrepayment REAL,
                commonstockissued REAL,
                commonstockrepurchased REAL,
                dividendspaid REAL,
                cashfromfinancing REAL,
                freecashflow REAL,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER,
                cik TEXT,
                fillingdate TEXT,
                accepteddate TEXT,
                calendarYear TEXT,
                link TEXT,
                finallink TEXT,
                operating_cash_flow REAL,
                inventory_changes REAL,
                accounts_receivables_changes REAL,
                accounts_payables_changes REAL,
                net_cash_used_provided_by_operating_activities REAL,
                purchases_of_investments REAL,
                sales_maturities_of_investments REAL,
                payments_for_acquisition_of_business REAL,
                effect_of_forex_changes_on_cash REAL,
                net_change_in_cash REAL
            );
            """)
            
            # Financial ratios - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_ratios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                reportedcurrency TEXT,
                fiscalyear INTEGER,
                peratio REAL,
                pegration REAL,
                payoutratio REAL,
                currentratio REAL,
                quickratio REAL,
                cashration REAL,
                grosseprofitmargin REAL,
                operatingprofitmargin REAL,
                netprofitmargin REAL,
                roe REAL,
                roa REAL,
                debtratio REAL,
                debtequityratio REAL,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER,
                grossprofitmargin REAL,
                dividendyield REAL,
                dividendyielttm REAL,
                dividendyieldpercentagettm REAL,
                pbratioTTM REAL,
                ptbratioTTM REAL,
                evtorevenue REAL,
                enterprisevalueoverebitda REAL,
                evtocff REAL,
                earningsyield REAL,
                dividend REAL
            );
            """)
            
            # Analyst estimates - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyst_estimates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                fiscalyear INTEGER,
                estimatedate TEXT,
                estimatetype TEXT,
                estimate REAL,
                numberofanalysts INTEGER,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER,
                revenue REAL,
                revenuelow REAL,
                revenuehigh REAL,
                revenueavg REAL,
                ebitda REAL,
                ebitdalow REAL,
                ebitdahigh REAL,
                ebitdaavg REAL,
                netincome REAL,
                netincomelow REAL,
                netincomehigh REAL,
                netincomeavg REAL
            );
            """)
            
            # Earnings transcripts
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS earnings_transcripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                period TEXT,
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                title TEXT,
                content TEXT,
                data_source TEXT,
                year INTEGER,
                quarter INTEGER
            );
            """)
            
            # Company news - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                title TEXT,
                summary TEXT,
                source TEXT,
                url TEXT,
                data_source TEXT,
                publisheddate TEXT,
                category TEXT,
                image TEXT,
                publisher TEXT
            );
            """)
            
            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_metrics_symbol ON financial_metrics(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_text_metrics_symbol ON text_metrics(symbol);')
        else:
            # PostgreSQL schema
            # Create financial_metrics table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_metrics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                reportedcurrency VARCHAR(10),
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                data_source VARCHAR(50),
                metric_values JSONB,
                metric_type VARCHAR(20) NOT NULL
            );
            """)
            
            # Create text_metrics table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS text_metrics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                metric_type VARCHAR(20) NOT NULL,
                title TEXT,
                content TEXT,
                metadata JSONB
            );
            """)
            
            # Create traditional tables for each data type with all possible columns
            # Income statements - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS income_statements (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                reportedcurrency VARCHAR(10),
                fiscalyear INTEGER,
                revenue NUMERIC,
                costofrevenue NUMERIC,
                grossprofit NUMERIC,
                researchanddevelopmentexpenses NUMERIC,
                generalandadministrativeexpenses NUMERIC,
                sellingandmarketingexpenses NUMERIC,
                operatingexpenses NUMERIC,
                operatingincome NUMERIC,
                interestexpense NUMERIC,
                ebitda NUMERIC,
                ebitdaratio NUMERIC,
                netincome NUMERIC,
                eps NUMERIC,
                epsdiluted NUMERIC,
                weightedaverageshsout NUMERIC,
                weightedaverageshsoutdil NUMERIC,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER,
                cik VARCHAR(20),
                fillingdate DATE,
                accepteddate DATE,
                calendarYear VARCHAR(10),
                link TEXT,
                finallink TEXT,
                income_before_tax NUMERIC,
                income_tax_expense NUMERIC,
                gross_profit_ratio NUMERIC,
                comprehensive_income_net_of_tax NUMERIC,
                effective_tax_rate NUMERIC
            );
            """)
            
            # Balance sheets - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheets (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                reportedcurrency VARCHAR(10),
                fiscalyear INTEGER,
                cashandcashequivalents NUMERIC,
                shortterminvestments NUMERIC,
                cashandshortterminvestments NUMERIC,
                netreceivables NUMERIC,
                inventory NUMERIC,
                totalcurrentassets NUMERIC,
                propertyplantequipmentnet NUMERIC,
                goodwill NUMERIC,
                intangibleassets NUMERIC,
                totalassets NUMERIC,
                accountspayable NUMERIC,
                shorttermdebt NUMERIC,
                totalcurrentliabilities NUMERIC,
                longtermdebt NUMERIC,
                totalliabilities NUMERIC,
                totalstockholdersequity NUMERIC,
                totaldebt NUMERIC,
                netdebt NUMERIC,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER,
                cik VARCHAR(20),
                fillingdate DATE,
                accepteddate DATE,
                calendarYear VARCHAR(10),
                link TEXT,
                finallink TEXT,
                other_current_assets NUMERIC,
                other_non_current_assets NUMERIC,
                deferred_revenue NUMERIC,
                deferred_tax_liabilities_non_current NUMERIC,
                preferred_stock NUMERIC,
                common_stock NUMERIC,
                retained_earnings NUMERIC,
                accumulated_other_comprehensive_income_loss NUMERIC,
                othertotalstockholdersequity NUMERIC,
                totalinvestments NUMERIC,
                totalliabilitiesandtotalequity NUMERIC
            );
            """)
            
            # Cash flow statements - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS cash_flow_statements (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                reportedcurrency VARCHAR(10),
                fiscalyear INTEGER,
                netincome NUMERIC,
                depreciationandamortization NUMERIC,
                stockbasedcompensation NUMERIC,
                changeinworkingcapital NUMERIC,
                cashfromoperations NUMERIC,
                capitalexpenditure NUMERIC,
                acquisitionsnet NUMERIC,
                cashfrominvesting NUMERIC,
                debtrepayment NUMERIC,
                commonstockissued NUMERIC,
                commonstockrepurchased NUMERIC,
                dividendspaid NUMERIC,
                cashfromfinancing NUMERIC,
                freecashflow NUMERIC,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER,
                cik VARCHAR(20),
                fillingdate DATE,
                accepteddate DATE,
                calendarYear VARCHAR(10),
                link TEXT,
                finallink TEXT,
                operating_cash_flow NUMERIC,
                inventory_changes NUMERIC,
                accounts_receivables_changes NUMERIC,
                accounts_payables_changes NUMERIC,
                net_cash_used_provided_by_operating_activities NUMERIC,
                purchases_of_investments NUMERIC,
                sales_maturities_of_investments NUMERIC,
                payments_for_acquisition_of_business NUMERIC,
                effect_of_forex_changes_on_cash NUMERIC,
                net_change_in_cash NUMERIC
            );
            """)
            
            # Financial ratios - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_ratios (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                reportedcurrency VARCHAR(10),
                fiscalyear INTEGER,
                peratio NUMERIC,
                pegration NUMERIC,
                payoutratio NUMERIC,
                currentratio NUMERIC,
                quickratio NUMERIC,
                cashration NUMERIC,
                grosseprofitmargin NUMERIC,
                operatingprofitmargin NUMERIC,
                netprofitmargin NUMERIC,
                roe NUMERIC,
                roa NUMERIC,
                debtratio NUMERIC,
                debtequityratio NUMERIC,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER,
                grossprofitmargin NUMERIC,
                dividendyield NUMERIC,
                dividendyielttm NUMERIC,
                dividendyieldpercentagettm NUMERIC,
                pbratioTTM NUMERIC,
                ptbratioTTM NUMERIC,
                evtorevenue NUMERIC,
                enterprisevalueoverebitda NUMERIC,
                evtocff NUMERIC,
                earningsyield NUMERIC,
                dividend NUMERIC
            );
            """)
            
            # Analyst estimates - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyst_estimates (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                fiscalyear INTEGER,
                estimatedate DATE,
                estimatetype VARCHAR(20),
                estimate NUMERIC,
                numberofanalysts INTEGER,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER,
                revenue NUMERIC,
                revenuelow NUMERIC,
                revenuehigh NUMERIC,
                revenueavg NUMERIC,
                ebitda NUMERIC,
                ebitdalow NUMERIC,
                ebitdahigh NUMERIC,
                ebitdaavg NUMERIC,
                netincome NUMERIC,
                netincomelow NUMERIC,
                netincomehigh NUMERIC,
                netincomeavg NUMERIC,
                eps NUMERIC,
                epslow NUMERIC,
                epshigh NUMERIC,
                epsavg NUMERIC
            );
            """)
            
            # Earnings transcripts
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS earnings_transcripts (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                period VARCHAR(10),
                fiscalyear INTEGER,
                fiscalquarter INTEGER,
                title TEXT,
                content TEXT,
                data_source VARCHAR(50),
                year INTEGER,
                quarter INTEGER
            );
            """)
            
            # Company news - expanded schema with additional columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_news (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                title TEXT,
                summary TEXT,
                source VARCHAR(100),
                url TEXT,
                data_source VARCHAR(50),
                publisheddate DATE,
                category VARCHAR(50),
                image TEXT,
                publisher VARCHAR(100)
            );
            """)
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_metrics_symbol ON financial_metrics(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_text_metrics_symbol ON text_metrics(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_statements_symbol ON income_statements(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_sheets_symbol ON balance_sheets(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_flow_statements_symbol ON cash_flow_statements(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_ratios_symbol ON financial_ratios(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analyst_estimates_symbol ON analyst_estimates(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_earnings_transcripts_symbol ON earnings_transcripts(symbol);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_news_symbol ON company_news(symbol);')
        conn.commit()
        print(f"Database schema created successfully for {('SQLite' if USE_SQLITE else 'PostgreSQL')}")
    except Exception as e:
        print(f"Error creating database schema: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

def convert_date_format(df):
    """Convert date columns to the appropriate format for the target database"""
    date_columns = ['date', 'publisheddate', 'estimatedate', 'fillingdate', 'accepteddate']
    
    for col in date_columns:
        if col in df.columns:
            # Check if the column contains date-like strings
            # First row that's not NaN
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            
            if sample and isinstance(sample, str):
                try:
                    # Try to parse and standardize the date format
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    print(f"Converted {col} to standard date format")
                except Exception as e:
                    print(f"Error converting {col} to date: {e}")
    
    return df

def migrate_data_to_database(master_df):
    """Migrate data to database (SQLite or PostgreSQL)"""
    if master_df is None or master_df.empty:
        print("No data to migrate to database")
        return

    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        # Process consolidated CSVs for traditional tables
        consolidated_dir = os.path.join(OUTPUT_DIR, "consolidated")
        
        # Direct mapping for table names - use actual table names as file prefix
        # This simplifies the matching logic
        table_names = [
            'income_statements',
            'balance_sheets', 
            'cash_flow_statements',
            'financial_ratios',
            'analyst_estimates',
            'earnings_transcripts',
            'company_news'
        ]
        
        # Get a list of the actual files in the consolidated directory
        consolidated_files = os.listdir(consolidated_dir)
        print(f"Found consolidated files: {consolidated_files}")
        
        # Loop through each table name and check for matching files
        for table_name in table_names:
            # Look for exact table name match
            target_file = f"{table_name}_all_data.csv"
            
            if target_file in consolidated_files:
                file_path = os.path.join(consolidated_dir, target_file)
                
                print(f"Processing {table_name} data from {target_file}")
                
                try:
                    df = pd.read_csv(file_path)
                    
                    # Convert column names to lowercase and replace spaces/hyphens with underscores
                    df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
                    
                    # Drop duplicates if any
                    if 'symbol' in df.columns and 'date' in df.columns:
                        df = df.drop_duplicates(subset=['symbol', 'date'], keep='first')
                    
                    if df.empty:
                        print(f"No data in {target_file}, skipping")
                        continue

                    # Convert date columns to appropriate format
                    df = convert_date_format(df)
                    
                    # Get the actual columns in the database table
                    if USE_SQLITE:
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        table_columns = [row[1] for row in cursor.fetchall()]  # column name is at index 1
                    else:
                        cursor.execute(f"""
                            SELECT column_name FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                        """)
                        table_columns = [row[0] for row in cursor.fetchall()]
                    
                    print(f"Table columns: {table_columns}")
                    
                    # Filter dataframe to only include columns that exist in the table
                    # First, find which dataframe columns match table columns (case-insensitive)
                    df_columns = df.columns.tolist()
                    
                    # Create mapping from df columns to table columns
                    column_mapping = {}
                    for df_col in df_columns:
                        for table_col in table_columns:
                            if df_col.lower() == table_col.lower():
                                column_mapping[df_col] = table_col
                                break
                    
                    if not column_mapping:
                        print(f"No matching columns found for {table_name}, skipping")
                        continue
                    
                    # Create a new dataframe with only the columns that exist in the table
                    filtered_df = df[list(column_mapping.keys())].copy()
                    
                    # Rename columns to match the table schema
                    filtered_df.rename(columns=column_mapping, inplace=True)
                    
                    # Print the matching columns being inserted
                    print(f"Found {len(filtered_df.columns)} matching columns: {filtered_df.columns.tolist()}")
                    print(f"Found {len(filtered_df)} rows to insert into {table_name}")
                    
                    if USE_SQLITE:
                        # For SQLite, use pandas to_sql method 
                        filtered_df.to_sql(table_name, conn, if_exists='append', index=False)
                        print(f"Inserted {len(filtered_df)} rows into {table_name}")
                    else:
                        # For PostgreSQL, use prepared statements in chunks
                        # Prepare data for insertion
                        columns = filtered_df.columns.tolist()
                        quoted_columns = [f'"{col}"' for col in columns]
                        placeholders = ", ".join(["%s"] * len(columns))
                        
                        # Build INSERT statement
                        insert_sql = f'INSERT INTO {table_name} ({", ".join(quoted_columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
                        
                        # Convert any None/NaN values to PostgreSQL NULL
                        filtered_df = filtered_df.where(pd.notnull(filtered_df), None)
                        
                        # Insert data in chunks
                        data_tuples = [tuple(x) for x in filtered_df.to_numpy()]
                        chunk_size = 1000
                        for i in range(0, len(data_tuples), chunk_size):
                            chunk = data_tuples[i:i + chunk_size]
                            cursor.executemany(insert_sql, chunk)
                            conn.commit()
                            print(f"Inserted chunk {i//chunk_size + 1} ({len(chunk)} rows) into {table_name}")
                        
                        print(f"Inserted {len(filtered_df)} rows into {table_name}")
                except Exception as e:
                    print(f"Error inserting {table_name} data: {e}")
                    print(f"Error details: {str(e)}")
            else:
                print(f"No consolidated file found for {table_name}")
                
                # Try alternate file formats
                for file in consolidated_files:
                    if table_name.replace('_statements', '') in file or table_name.replace('_sheets', '') in file:
                        print(f"Found possible match: {file} for table {table_name}")
        
        # After inserting data, verify tables have content
        for table_name in table_names:
            if USE_SQLITE:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"Table {table_name} has {count} rows")
                except Exception as e:
                    print(f"Error counting rows in {table_name}: {e}")
            else:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"Table {table_name} has {count} rows")
                except Exception as e:
                    print(f"Error counting rows in {table_name}: {e}")
        
        conn.close()
        print("Data migration to database complete")
    except Exception as e:
        print(f"Error during database migration: {e}")

def migrate_to_consolidated_tables():
    """Migrate data from traditional tables to consolidated financial_metrics and text_metrics tables"""
    print("Starting migration to consolidated tables...")
    
    # Migrate financial tables
    print("\nMigrating financial tables...")
    migrate_financial_tables()
    
    # Migrate text tables
    print("\nMigrating text tables...")
    migrate_text_tables()
    
    print("\nMigration to consolidated tables completed successfully!")

def migrate_financial_tables():
    """Migrate data from financial tables to consolidated financial_metrics table"""
    conn = connect_to_db()
    
    total_rows = 0
    
    try:
        for metric_type, table_name in FINANCIAL_TABLES.items():
            # Check if source table exists
            table_exists = False
            
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                table_exists = cursor.fetchone() is not None
                cursor.close()
            else:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table_name,))
                    table_exists = cursor.fetchone()[0]
                
            if not table_exists:
                print(f"Table {table_name} does not exist in database, skipping...")
                continue
            
            # Check if the table has any data
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                
                if row_count == 0:
                    print(f"Table {table_name} exists but is empty, skipping...")
                    continue
                    
                print(f"Table {table_name} has {row_count} rows")
            else:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    
                    if row_count == 0:
                        print(f"Table {table_name} exists but is empty, skipping...")
                        continue
                        
                    print(f"Table {table_name} has {row_count} rows")
            
            # Get all data from source table
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f'SELECT * FROM "{table_name}"')
                # Convert cursor rows to dictionaries
                rows = []
                for row in cursor.fetchall():
                    columns = [desc[0] for desc in cursor.description]
                    rows.append(dict(zip(columns, row)))
                cursor.close()
            else:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f'SELECT * FROM "{table_name}"')
                    rows = cursor.fetchall()
                
            print(f"Migrating {len(rows)} rows from {table_name} to financial_metrics...")
            
            # Process each row
            for row in rows:
                # Extract base fields
                symbol = row.get('symbol')
                date = row.get('date')
                period = row.get('period')
                reportedcurrency = row.get('reportedcurrency')
                fiscalyear = row.get('fiscalyear')
                data_source = row.get('data_source')
                fiscalquarter = get_quarter_from_period(period)
                
                # Build metric values from remaining fields
                metric_values = {}
                for key, value in row.items():
                    if key not in EXCLUDED_COLUMNS and value is not None:
                        # Handle different data types
                        if isinstance(value, pd.Timestamp):
                            metric_values[key] = value.strftime('%Y-%m-%d')
                        elif pd.isna(value):
                            continue  # Skip NaN values
                        else:
                            metric_values[key] = value
                
                # Insert into financial_metrics table
                metric_values_json = json.dumps(metric_values)
                
                if USE_SQLITE:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO financial_metrics 
                        (symbol, date, period, reportedcurrency, fiscalyear, 
                         fiscalquarter, data_source, metric_values, metric_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol, date, period, reportedcurrency, fiscalyear,
                        fiscalquarter, data_source, metric_values_json, metric_type
                    ))
                    conn.commit()
                    cursor.close()
                else:
                    with conn.cursor() as insert_cursor:
                        insert_cursor.execute("""
                            INSERT INTO financial_metrics 
                            (symbol, date, period, reportedcurrency, fiscalyear, 
                             fiscalquarter, data_source, metric_values, metric_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            symbol, date, period, reportedcurrency, fiscalyear,
                            fiscalquarter, data_source, metric_values_json, metric_type
                        ))
                
                total_rows += 1
    
    except Exception as e:
        print(f"Error during financial table migration: {e}")
        sys.exit(1)
    finally:
        conn.close()
    
    print(f"Successfully migrated {total_rows} rows to financial_metrics table")

def migrate_text_tables():
    """Migrate data from text-based tables to consolidated text_metrics table"""
    conn = connect_to_db()
    
    total_rows = 0
    
    try:
        for metric_type, table_name in TEXT_TABLES.items():
            # Check if source table exists
            table_exists = False
            
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                table_exists = cursor.fetchone() is not None
                cursor.close()
            else:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table_name,))
                    table_exists = cursor.fetchone()[0]
                
            if not table_exists:
                print(f"Table {table_name} does not exist in database, skipping...")
                continue
            
            # Get schema to determine columns
            column_names = []
            
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                column_names = [row[1] for row in cursor.fetchall()]  # column name is at index 1
                cursor.close()
            else:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = %s
                    """, (table_name,))
                    column_names = [col[0] for col in cursor.fetchall()]
            
            # Map columns based on table type
            if metric_type == 'transcript':
                title_col = 'title' if 'title' in column_names else None
                content_col = 'content' if 'content' in column_names else 'transcript'
                date_col = 'date'
            elif metric_type == 'news':
                title_col = 'headline' if 'headline' in column_names else 'title'
                content_col = 'summary' if 'summary' in column_names else 'content'
                date_col = 'published_date' if 'published_date' in column_names else 'date'
            else:
                title_col = 'title'
                content_col = 'content'
                date_col = 'date'
            
            # Get all data from source table
            if USE_SQLITE:
                cursor = conn.cursor()
                cursor.execute(f'SELECT * FROM "{table_name}"')
                # Convert cursor rows to dictionaries
                rows = []
                for row in cursor.fetchall():
                    columns = [desc[0] for desc in cursor.description]
                    rows.append(dict(zip(columns, row)))
                cursor.close()
            else:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f'SELECT * FROM "{table_name}"')
                    rows = cursor.fetchall()
            
            print(f"Migrating {len(rows)} rows from {table_name} to text_metrics...")
            
            # Process each row
            for row in rows:
                # Extract base fields
                symbol = row.get('symbol')
                date = row.get(date_col)
                period = row.get('period')
                fiscalyear = row.get('fiscalyear')
                
                # Use current date as default for news items with missing dates
                if date is None or pd.isna(date):
                    date = datetime.utcnow().strftime('%Y-%m-%d')
                    print(f"Setting missing date for {symbol} {metric_type} to {date}")
                
                # Derive fiscal year from date if not present
                if fiscalyear is None and date is not None:
                    if isinstance(date, str):
                        try:
                            date_obj = datetime.strptime(date, '%Y-%m-%d')
                            fiscalyear = date_obj.year
                        except ValueError:
                            fiscalyear = None
                    elif isinstance(date, pd.Timestamp):
                        fiscalyear = date.year
                    else:
                        fiscalyear = date.year
                
                fiscalquarter = get_quarter_from_period(period)
                title = row.get(title_col) if title_col else None
                content = row.get(content_col) if content_col else None
                
                # Build metadata from remaining fields
                metadata = {}
                for key, value in row.items():
                    if key not in [title_col, content_col, 'symbol', date_col, 'period', 'fiscalyear'] and value is not None:
                        # Handle different data types
                        if isinstance(value, pd.Timestamp):
                            metadata[key] = value.strftime('%Y-%m-%d')
                        elif pd.isna(value):
                            continue  # Skip NaN values
                        else:
                            metadata[key] = value
                
                # Insert into text_metrics table
                metadata_json = json.dumps(metadata)
                
                # Default fiscal year if missing
                if fiscalyear is None:
                    fiscalyear = datetime.utcnow().year
                
                if USE_SQLITE:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO text_metrics 
                        (symbol, date, period, fiscalyear, fiscalquarter, 
                         metric_type, title, content, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol, date, period, fiscalyear, fiscalquarter,
                        metric_type, title, content, metadata_json
                    ))
                    conn.commit()
                    cursor.close()
                else:
                    with conn.cursor() as insert_cursor:
                        insert_cursor.execute("""
                            INSERT INTO text_metrics 
                            (symbol, date, period, fiscalyear, fiscalquarter, 
                             metric_type, title, content, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                            ON CONFLICT DO NOTHING
                        """, (
                            symbol, date, period, fiscalyear, fiscalquarter,
                            metric_type, title, content, metadata_json
                        ))
                
                total_rows += 1
    
    except Exception as e:
        print(f"Error during text table migration: {e}")
        sys.exit(1)
    finally:
        conn.close()
    
    print(f"Successfully migrated {total_rows} rows to text_metrics table")

def process_symbol_data(symbol):
    """Process data for a single symbol"""
    print(f"Processing data for {symbol}")
    
    try:
        # Process earnings call transcripts - fetch last few quarters
        transcript_endpoint = "earning_call_transcript"
        transcript_config = ENDPOINTS[transcript_endpoint]
        for year in YEARS[-2:]:  # Last 2 years
            for quarter in QUARTERS:
                data = fetch_api_data(transcript_endpoint, transcript_config, symbol, year, quarter)
                save_to_csv(data, transcript_endpoint, symbol, year, quarter)
                time.sleep(1)
        
        # Process all other endpoints
        for endpoint_name, endpoint_config in ENDPOINTS.items():
            if endpoint_name != "earning_call_transcript":
                # Handle period-based endpoints
                if "period_param" in endpoint_config:
                    # Annual data
                    data = fetch_api_data(endpoint_name, endpoint_config, symbol, period="annual")
                    save_to_csv(data, endpoint_name, symbol, period="annual")
                    time.sleep(1)
                    
                    # Quarterly data
                    data = fetch_api_data(endpoint_name, endpoint_config, symbol, period="quarter")
                    save_to_csv(data, endpoint_name, symbol, period="quarter")
                    time.sleep(1)
                else:
                    # Regular endpoints
                    data = fetch_api_data(endpoint_name, endpoint_config, symbol)
                    save_to_csv(data, endpoint_name, symbol)
                    time.sleep(1)
                    
        return f"Completed processing for {symbol}"
    except Exception as e:
        return f"Error processing {symbol}: {str(e)}"

def main():
    """Main function to run the entire ETL process"""
    print(f"Starting financial data ETL process for {len(SYMBOLS)} symbols using {('SQLite' if USE_SQLITE else 'PostgreSQL')}")
    
    # Step 1: Collect financial data from FMP API with concurrent requests
    print("\n--- STEP 1: Collecting data from FMP API ---")
    
    # Use a thread pool to process symbols concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        # Submit symbol processing tasks
        future_to_symbol = {executor.submit(process_symbol_data, symbol): symbol for symbol in SYMBOLS}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"Symbol {symbol} generated an exception: {e}")
    
    # Step 2: Consolidate CSV files
    print("\n--- STEP 2: Consolidating CSV files ---")
    create_consolidated_files()
    
    # Step 3: Create master CSV with all data
    print("\n--- STEP 3: Creating master CSV ---")
    master_df = create_master_csv()
    
    # Step 4: Create database schema
    print("\n--- STEP 4: Creating database schema ---")
    create_database_schema()
    
    # Step 5: Migrate data to database
    print("\n--- STEP 5: Migrating data to database ---")
    migrate_data_to_database(master_df)
    
    # Step 6: Migrate data to consolidated tables
    print("\n--- STEP 6: Migrating data to consolidated tables ---")
    migrate_to_consolidated_tables()
    
    db_type = "SQLite" if USE_SQLITE else "PostgreSQL"
    db_location = SQLITE_DB_PATH if USE_SQLITE else f"{PG_DB} on {PG_HOST}"
    
    print("\nFinancial data ETL process completed successfully!")
    print(f"Data for {len(SYMBOLS)} symbols has been processed and stored in {db_type} database at {db_location}")

if __name__ == "__main__":
    main() 