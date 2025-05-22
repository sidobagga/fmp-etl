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
import numpy as np
import traceback

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Financial data ETL process')
parser.add_argument('--symbols', type=str, help='Comma-separated list of ticker symbols (e.g., AAPL,MSFT,GOOGL)')
parser.add_argument('--db-type', type=str, choices=['sqlite', 'postgres'], default='sqlite', help='Database type to use (default: sqlite)')
parser.add_argument('--limit', type=int, default=3, help='Number of symbols to process (default: 3)')
parser.add_argument('--db-name', type=str, help='Database name for PostgreSQL (default: finmetrics)')
parser.add_argument('--db-host', type=str, default='orbe360.ai', help='Database host (default: orbe360.ai)')
parser.add_argument('--db-port', type=int, default=5432, help='Database port (default: 5432)')
parser.add_argument('--db-user', type=str, default='postgres', help='Database user (default: postgres)')
parser.add_argument('--db-pass', type=str, default='Admin0rbE', help='Database password')
parser.add_argument('--api-key', type=str, default='fjRDKKnsRnVNMfFepDM6ox31u9RlPklv', help='FMP API key')
parser.add_argument('--mode', type=str, choices=['api', 'migrate', 'consolidate', 'calculate', 'price_targets', 'peers', 'annual', 'ratio', 'all'], default='all', help='ETL mode to run (default: all)')
parser.add_argument('--price-targets-only', action='store_true', help='Only fetch price target data')
parser.add_argument('--peers-only', action='store_true', help='Only fetch stock peers data')
parser.add_argument('--peers-with-data', action='store_true', help='Fetch stock peers and their financial data')
parser.add_argument('--migrate-only', action='store_true', help='Only migrate CSV data to database without fetching')
args = parser.parse_args()

# API configuration
API_KEY = args.api_key
BASE_URL = "https://financialmodelingprep.com/api/v3"

# Database configuration
USE_SQLITE = args.db_type == 'sqlite'  # Default to SQLite for local testing
SQLITE_DB_PATH = os.path.join('financial_data', 'financial_data.db')

# PostgreSQL database details (only used if USE_SQLITE is False)
PG_HOST = args.db_host
PG_PORT = args.db_port
PG_USER = args.db_user
PG_PASSWORD = args.db_pass
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
    "enterprise_values": {
        "url": "/enterprise-values",
        "params": ["symbol"],
        "quarterly": False,
        "table_name": "enterprise_values",
        "metric_type": "enterprise_value"
    },
    "dcf": {
        "url": "/custom-discounted-cash-flow",
        "params": ["symbol"],
        "quarterly": False,
        "table_name": "discounted_cash_flow",
        "metric_type": "dcf"
    },
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
    },
    "price_target_news": {
        "url": "/price-target-news",
        "params": ["symbol"],
        "additional_params": {"page": 0, "limit": 5},
        "quarterly": False,
        "table_name": "price_targets",
        "metric_type": "price_target"
    }
}

# Table mappings for consolidation
FINANCIAL_TABLES = {
    'income': 'income_statements',
    'balance': 'balance_sheets',
    'cash_flow': 'cash_flow_statements',
    'ratio': 'financial_ratios',
    'analyst': 'analyst_estimates',
    'enterprise_value': 'enterprise_values',
    'dcf': 'discounted_cash_flow'
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
    
    if year is not None:
        if endpoint_name in ["income_statement", "balance_sheet_statement", "cash_flow_statement"]:
            # For financial statements, use the year parameter directly
            params["year"] = year
        elif "year" in endpoint_config["params"]:
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
            
            # Special handling for price targets
            if endpoint_name == "price_target_news":
                # Calculate bullish flag (1 if price target >= price when posted, 0 otherwise)
                if "priceTarget" in df.columns and "priceWhenPosted" in df.columns:
                    df["bullish"] = df.apply(lambda row: 1 if row["priceTarget"] >= row["priceWhenPosted"] else 0, axis=1)
            
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
            
            # Enterprise values
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS enterprise_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT,
                stockPrice REAL,
                numberOfShares REAL,
                marketCapitalization REAL,
                minusCashAndCashEquivalents REAL,
                addTotalDebt REAL,
                enterpriseValue REAL,
                data_source TEXT
            );
            """)
            
            # Discounted cash flow
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS discounted_cash_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                year TEXT,
                revenue REAL,
                revenuePercentage REAL,
                capitalExpenditure REAL,
                capitalExpenditurePercentage REAL,
                price REAL,
                beta REAL,
                dilutedSharesOutstanding REAL,
                costofDebt REAL,
                taxRate REAL,
                afterTaxCostOfDebt REAL,
                riskFreeRate REAL,
                marketRiskPremium REAL,
                costOfEquity REAL,
                totalDebt REAL,
                totalEquity REAL,
                totalCapital REAL,
                debtWeighting REAL,
                equityWeighting REAL,
                wacc REAL,
                operatingCashFlow REAL,
                pvLfcf REAL,
                sumPvLfcf REAL,
                longTermGrowthRate REAL,
                freeCashFlow REAL,
                terminalValue REAL,
                presentTerminalValue REAL,
                enterpriseValue REAL,
                netDebt REAL,
                equityValue REAL,
                equityValuePerShare REAL,
                freeCashFlowT1 REAL,
                operatingCashFlowPercentage REAL,
                date TEXT,
                data_source TEXT
            );
            """)
            
            # Price targets
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                publishedDate TEXT,
                newsURL TEXT,
                newsTitle TEXT,
                analystName TEXT,
                analystCompany TEXT,
                priceTarget REAL,
                priceWhenPosted REAL,
                bullish INTEGER,
                newsPublisher TEXT,
                data_source TEXT
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
            
            # Enterprise values
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS enterprise_values (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE,
                stockPrice NUMERIC,
                numberOfShares NUMERIC,
                marketCapitalization NUMERIC,
                minusCashAndCashEquivalents NUMERIC,
                addTotalDebt NUMERIC,
                enterpriseValue NUMERIC,
                data_source VARCHAR(50)
            );
            """)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_enterprise_values_symbol ON enterprise_values(symbol);')
            
            # Discounted cash flow
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS discounted_cash_flow (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                year VARCHAR(10),
                revenue NUMERIC,
                revenuePercentage NUMERIC,
                capitalExpenditure NUMERIC,
                capitalExpenditurePercentage NUMERIC,
                price NUMERIC,
                beta NUMERIC,
                dilutedSharesOutstanding NUMERIC,
                costofDebt NUMERIC,
                taxRate NUMERIC,
                afterTaxCostOfDebt NUMERIC,
                riskFreeRate NUMERIC,
                marketRiskPremium NUMERIC,
                costOfEquity NUMERIC,
                totalDebt NUMERIC,
                totalEquity NUMERIC, 
                totalCapital NUMERIC,
                debtWeighting NUMERIC,
                equityWeighting NUMERIC,
                wacc NUMERIC,
                operatingCashFlow NUMERIC,
                pvLfcf NUMERIC,
                sumPvLfcf NUMERIC,
                longTermGrowthRate NUMERIC,
                freeCashFlow NUMERIC,
                terminalValue NUMERIC,
                presentTerminalValue NUMERIC,
                enterpriseValue NUMERIC,
                netDebt NUMERIC,
                equityValue NUMERIC,
                equityValuePerShare NUMERIC,
                freeCashFlowT1 NUMERIC,
                operatingCashFlowPercentage NUMERIC,
                date DATE,
                data_source VARCHAR(50)
            );
            """)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_discounted_cash_flow_symbol ON discounted_cash_flow(symbol);')
            
            # Price targets
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_targets (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                publishedDate DATE,
                newsURL TEXT,
                newsTitle TEXT,
                analystName VARCHAR(100),
                analystCompany VARCHAR(100),
                priceTarget NUMERIC,
                priceWhenPosted NUMERIC,
                bullish INTEGER,
                newsPublisher VARCHAR(100),
                data_source VARCHAR(50)
            );
            """)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_targets_symbol ON price_targets(symbol);')

            # Create analyst estimates table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyst_estimates (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                date DATE,
                revenue_low BIGINT,
                revenue_high BIGINT,
                revenue_avg BIGINT,
                ebitda_low BIGINT,
                ebitda_high BIGINT,
                ebitda_avg BIGINT,
                ebit_low BIGINT,
                ebit_high BIGINT,
                ebit_avg BIGINT,
                net_income_low BIGINT,
                net_income_high BIGINT,
                net_income_avg BIGINT,
                sga_expense_low BIGINT,
                sga_expense_high BIGINT,
                sga_expense_avg BIGINT,
                eps_avg FLOAT,
                eps_high FLOAT,
                eps_low FLOAT,
                num_analysts_revenue INT,
                num_analysts_eps INT
            );
            """)

            # Create price target news table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_target_news (
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
                analyst_company TEXT
            );
            """)
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

def migrate_data_to_database(data_dict, db_type='sqlite', db_path='./insightdb.db', db_host='localhost', db_port=5432, db_name='insight', db_user='postgres', db_password='password'):
    """
    Migrate data to the database
    """
    # Connect to the appropriate database
    if db_type.lower() == 'sqlite':
        # SQLite connection
        conn = sqlite3.connect(db_path)
        print(f"Connected to SQLite database: {db_path}")
    elif db_type.lower() == 'postgres':
        # PostgreSQL connection
        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            print(f"Connected to PostgreSQL database: {db_name} on {db_host}")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    else:
        print(f"Unsupported database type: {db_type}")
        return False
    
    # Create cursor
        cursor = conn.cursor()

    # For each table in the data dictionary
    for table_name, df in data_dict.items():
        if df is None or df.empty:
            print(f"No data to insert for table: {table_name}")
            continue
        
        try:
            # Create table if it doesn't exist
            create_table(conn, cursor, table_name, df, db_type)
            
            # Remove duplicates - keep only the latest data for each symbol and date
                    if 'symbol' in df.columns and 'date' in df.columns:
                # Group by symbol and date and keep the last record
                df = df.sort_values('date').groupby(['symbol', 'date']).last().reset_index()
            
            # Add additional system fields to track data
            df['data_source'] = 'fmp_api'  # Mark the source of the data
            
            # Filter out any NaN values and convert DataFrame to JSON
            filtered_df = df.replace({np.nan: None})
            
            # Handle data insertion based on database type
            try:
                if db_type.lower() == 'sqlite':
                    # For SQLite, we need to handle conflict differently
                    columns = filtered_df.columns.tolist()
                    placeholders = ", ".join(["?"] * len(columns))
                    
                    # Build INSERT OR REPLACE statement
                    insert_query = f'INSERT OR REPLACE INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
                    
                    # Insert data
                    data_tuples = [tuple(x) for x in filtered_df.to_numpy()]
                    cursor.executemany(insert_query, data_tuples)
                            conn.commit()
                    
                    print(f"Inserted {len(filtered_df)} rows into {table_name}")
                    else:
                        # For PostgreSQL, use ON CONFLICT DO NOTHING
                        if 'symbol' in filtered_df.columns and 'date' in filtered_df.columns:
                        # Check if the table has a unique constraint on (symbol, date)
                        cursor.execute(f"""
                            SELECT COUNT(*)
                            FROM pg_constraint pc
                            JOIN pg_class c ON pc.conrelid = c.oid
                            WHERE c.relname = '{table_name}'
                              AND pc.contype = 'u'
                        """)
                        has_unique_constraint = cursor.fetchone()[0] > 0
                        
                            # Prepare data for insertion
                            columns = filtered_df.columns.tolist()
                            quoted_columns = [f'"{col}"' for col in columns]
                            placeholders = ", ".join(["%s"] * len(columns))
                            
                        # Build INSERT statement with appropriate conflict handling
                        if has_unique_constraint and table_name == 'financial_ratios':
                            # For financial_ratios with unique constraint, use explicit column names
                            insert_sql = f"""
                                INSERT INTO {table_name} ({", ".join(quoted_columns)}) 
                                VALUES ({placeholders}) 
                                ON CONFLICT (symbol, date) DO NOTHING
                            """
                            print(f"Using ON CONFLICT (symbol, date) for {table_name}")
                        else:
                            # For tables without unique constraint, use simple INSERT
                                insert_sql = f"""
                                    INSERT INTO {table_name} ({", ".join(quoted_columns)}) 
                                    VALUES ({placeholders}) 
                                """
                            print(f"Using simple INSERT for {table_name} (no unique constraint)")
                                
                                # Convert any None/NaN values to PostgreSQL NULL
                                filtered_df = filtered_df.where(pd.notnull(filtered_df), None)
                                
                                # Insert data in chunks
                                data_tuples = [tuple(x) for x in filtered_df.to_numpy()]
                                chunk_size = 1000
                                inserted_count = 0
                                
                                for i in range(0, len(data_tuples), chunk_size):
                                    chunk = data_tuples[i:i + chunk_size]
                            try:
                                    cursor.executemany(insert_sql, chunk)
                                    inserted_count += cursor.rowcount
                                    conn.commit()
                            except Exception as e:
                                print(f"Error inserting chunk: {e}")
                                conn.rollback()
                                    
                        print(f"Inserted {inserted_count} rows into {table_name}")
                            else:
                        # For tables without symbol/date columns, use simple INSERT
                                columns = filtered_df.columns.tolist()
                                quoted_columns = [f'"{col}"' for col in columns]
                                placeholders = ", ".join(["%s"] * len(columns))
                                
                                # Build simple INSERT statement
                                insert_sql = f'INSERT INTO {table_name} ({", ".join(quoted_columns)}) VALUES ({placeholders})'
                                
                                # Convert any None/NaN values to PostgreSQL NULL
                                filtered_df = filtered_df.where(pd.notnull(filtered_df), None)
                                
                                # Insert data in chunks
                                data_tuples = [tuple(x) for x in filtered_df.to_numpy()]
                                chunk_size = 1000
                                for i in range(0, len(data_tuples), chunk_size):
                                    chunk = data_tuples[i:i + chunk_size]
                                    cursor.executemany(insert_sql, chunk)
                                    conn.commit()
                                    
                        print(f"Inserted {len(filtered_df)} rows into {table_name}")
                except Exception as e:
                    print(f"Error inserting {table_name} data: {e}")
                traceback.print_exc()
                except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            traceback.print_exc()
            
    # Close the connection
        conn.close()
    return True

def migrate_to_consolidated_tables(symbols=None):
    """Migrate data from traditional tables to consolidated financial_metrics and text_metrics tables"""
    print("Starting migration to consolidated tables...")
    
    # Migrate financial tables
    print("\nMigrating financial tables...")
    migrate_financial_tables(symbols)
    
    # Migrate text tables
    print("\nMigrating text tables...")
    migrate_text_tables(symbols)
    
    print("\nMigration to consolidated tables completed successfully!")

def calculate_additional_metrics(metrics):
    """Calculate additional financial metrics"""
    symbol = metrics.get('symbol')
    date = metrics.get('date')
    
    if not symbol or not date:
        return metrics
    
    # Helper function to find a value from multiple possible keys
    def find_value(possible_keys):
        for key in possible_keys:
            if key in metrics and metrics[key] is not None:
                return metrics[key]
        return None
    
    # Helper function to fetch a value from another table
    def fetch_from_table(table_name, field_names, symbol, date):
        conn = None
        try:
            conn = connect_to_db()
            cursor = conn.cursor()
            
            # Build field list
            field_list = ", ".join(field_names)
            
            # Use different parameter placeholders based on database type
            if USE_SQLITE:
                placeholder = "?"
                like_operator = "LIKE"
                date_cast = ""
            else:
                placeholder = "%s"  # For PostgreSQL
                like_operator = "LIKE"  # Regular LIKE is fine
                date_cast = "::text"  # Cast date to text for pattern matching
            
            # Try exact date match first
            query = f"SELECT {field_list} FROM {table_name} WHERE symbol = {placeholder} AND date = {placeholder} LIMIT 1"
            cursor.execute(query, (symbol, date))
            row = cursor.fetchone()
            
            if row:
                return dict(zip(field_names, row))
                
            # If not found, try approximate date match (same year/month)
            if len(date) >= 7:  # Has at least year-month
                year_month = date[:7]  # Extract YYYY-MM
                query = f"SELECT {field_list} FROM {table_name} WHERE symbol = {placeholder} AND date{date_cast} {like_operator} {placeholder} LIMIT 1"
                cursor.execute(query, (symbol, f"{year_month}%"))
                row = cursor.fetchone()
                
                if row:
                    return dict(zip(field_names, row))
                    
            # Try same year
            if len(date) >= 4:  # Has at least year
                year = date[:4]  # Extract YYYY
                query = f"SELECT {field_list} FROM {table_name} WHERE symbol = {placeholder} AND date{date_cast} {like_operator} {placeholder} LIMIT 1"
                cursor.execute(query, (symbol, f"{year}%"))
                row = cursor.fetchone()
                
                if row:
                    return dict(zip(field_names, row))
                
            return None
        except Exception as e:
            print(f"Error fetching from {table_name}: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    # Get values for the calculations
    # For enterprise value, check both the enterprise_values table and metrics
    ev_data = fetch_from_table('enterprise_values', ['enterpriseValue', 'numberOfShares', 'addTotalDebt'], symbol, date)
    dcf_data = fetch_from_table('discounted_cash_flow', ['dilutedSharesOutstanding'], symbol, date)
    
    # Extract enterprise value
    ev_value = None
    if ev_data and ev_data['enterpriseValue'] is not None:
        ev_value = ev_data['enterpriseValue']
        print(f"Found enterpriseValue from API: {ev_value}")
    else:
        # Calculate enterprise value from ratios if not available directly
        ebitda_value = find_value(['ebitda'])
        enterprise_value_multiple = find_value(['enterprisevaluemultiple', 'enterprisevaluetoebitda'])
        
        if ebitda_value is not None and enterprise_value_multiple is not None:
            ev_value = ebitda_value * enterprise_value_multiple
            print(f"Calculated enterpriseValue from multiple: {ev_value} = {ebitda_value} * {enterprise_value_multiple}")
    
    # Get total debt value
    total_debt_value = None
    if ev_data and ev_data['addTotalDebt'] is not None:
        total_debt_value = ev_data['addTotalDebt']
        print(f"Found totalDebt from API: {total_debt_value}")
    else:
        total_debt_value = find_value(['totaldebt'])
        if total_debt_value is not None:
            print(f"Found totalDebt from metrics: {total_debt_value}")
    
    # Get number of shares
    number_of_shares = None
    if ev_data and ev_data['numberOfShares'] is not None:
        number_of_shares = ev_data['numberOfShares']
        print(f"Found numberOfShares from API: {number_of_shares}")
    
    # Get diluted shares outstanding
    diluted_shares_outstanding = None
    if dcf_data and dcf_data['dilutedSharesOutstanding'] is not None:
        diluted_shares_outstanding = dcf_data['dilutedSharesOutstanding']
        print(f"Found dilutedSharesOutstanding from API: {diluted_shares_outstanding}")
    
    # Get EBITDA, EBIT, and revenue values
    ebitda_value = find_value(['ebitda'])
    ebit_value = find_value(['operatingincome', 'ebit'])
    sales_value = find_value(['revenue', 'totalrevenue', 'sales'])
    interest_expense_value = find_value(['interestexpense'])
    capex_value = find_value(['capitalexpenditure'])
    totalequity_value = find_value(['totalstockholdersequity'])
    earnings_value = find_value(['netincome'])
    price_value = find_value(['stockprice', 'price'])
    fcf_value = find_value(['freecashflow'])
    
    # Calculate new metrics
    if sales_value is not None:
        metrics['sales'] = sales_value
    
    # Add API-sourced metrics
    if number_of_shares is not None:
        metrics['numberOfShares'] = number_of_shares
    
    if diluted_shares_outstanding is not None:
        metrics['dilutedSharesOutstanding'] = diluted_shares_outstanding
    
    # Calculate EV/Sales ratio
    if ev_value is not None and sales_value is not None and sales_value != 0:
        metrics['ev_sales_ratio'] = ev_value / sales_value
        print(f"Calculated ev_sales_ratio: {metrics['ev_sales_ratio']} = {ev_value} / {sales_value}")
    
    # Calculate EV/EBIT ratio
    if ev_value is not None and ebit_value is not None and ebit_value != 0:
        metrics['ev_ebit_ratio'] = ev_value / ebit_value
        print(f"Calculated ev_ebit_ratio: {metrics['ev_ebit_ratio']} = {ev_value} / {ebit_value}")
    
    # Calculate interest coverage ratios
    if ebitda_value is not None and interest_expense_value is not None and interest_expense_value != 0:
        metrics['ebitda_interestexpense_ratio'] = ebitda_value / interest_expense_value
        print(f"Calculated ebitda_interestexpense_ratio: {metrics['ebitda_interestexpense_ratio']} = {ebitda_value} / {interest_expense_value}")
    
    if ebit_value is not None and interest_expense_value is not None and interest_expense_value != 0:
        metrics['ebit_interestexpense_ratio'] = ebit_value / interest_expense_value
        print(f"Calculated ebit_interestexpense_ratio: {metrics['ebit_interestexpense_ratio']} = {ebit_value} / {interest_expense_value}")
    
    # Calculate EBITDA less capex to interest expense ratio
    if ebitda_value is not None and capex_value is not None and interest_expense_value is not None and interest_expense_value != 0:
        ebitda_less_capex = ebitda_value - abs(capex_value)  # capex is negative, so abs() to ensure subtraction
        metrics['ebitdalesscapex_interestexpense_ratio'] = ebitda_less_capex / interest_expense_value
        print(f"Calculated ebitdalesscapex_interestexpense_ratio: {metrics['ebitdalesscapex_interestexpense_ratio']} = ({ebitda_value} - {abs(capex_value)}) / {interest_expense_value}")
    
    # Calculate total debt to enterprise value ratio
    if total_debt_value is not None and ev_value is not None and ev_value != 0:
        metrics['totaldebt_ev_ratio'] = total_debt_value / ev_value
        print(f"Calculated totaldebt_ev_ratio: {metrics['totaldebt_ev_ratio']} = {total_debt_value} / {ev_value}")
    
    # Calculate debt to equity ratio
    if total_debt_value is not None and totalequity_value is not None and totalequity_value != 0:
        metrics['debt_to_equity_ratio'] = total_debt_value / totalequity_value
        print(f"Calculated debt_to_equity_ratio: {metrics['debt_to_equity_ratio']} = {total_debt_value} / {totalequity_value}")
    
    # Calculate PE ratio 
    if price_value is not None and earnings_value is not None and earnings_value != 0:
        metrics['pe_ratio'] = price_value / earnings_value
        print(f"Calculated pe_ratio: {metrics['pe_ratio']} = {price_value} / {earnings_value}")
    
    # Add free cash flow
    if fcf_value is not None:
        metrics['free_cash_flow'] = fcf_value
        
        # Calculate FCF yield if market cap is available
        if ev_data and ev_data['marketCapitalization'] is not None and ev_data['marketCapitalization'] != 0:
            market_cap = ev_data['marketCapitalization']
            metrics['fcf_yield'] = fcf_value / market_cap
            print(f"Calculated fcf_yield: {metrics['fcf_yield']} = {fcf_value} / {market_cap}")
    
    return metrics

def migrate_financial_tables(symbols=None):
    """Migrate data from financial tables to consolidated financial_metrics table"""
    conn = connect_to_db()
    
    total_rows = 0
    skipped_rows = 0
    processed_symbols = set()  # Track the symbols we're processing
    
    # Ensure constraints are in place
    ensure_financial_ratios_constraint()
    ensure_financial_metrics_constraint()
    
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
                            WHERE table_name = '{table_name}'
                        )
                    """)
                    table_exists = cursor.fetchone()[0]
                
            if not table_exists:
                print(f"Source table {table_name} does not exist, skipping")
                continue
            
            print(f"Migrating data from {table_name} to financial_metrics")
            
            # Process rows from source table
            if USE_SQLITE:
                # SQLite version
                cursor = conn.cursor()
                # Add symbol filtering if specified
                if symbols:
                    placeholders = ', '.join(['?'] * len(symbols))
                    cursor.execute(f"SELECT * FROM {table_name} WHERE symbol IN ({placeholders})", symbols)
                else:
                    cursor.execute(f"SELECT * FROM {table_name}")
                columns = [description[0] for description in cursor.description]
                
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))
                    symbol = row_dict.get('symbol')
                    date = row_dict.get('date')
                    
                    if symbol:
                        processed_symbols.add(symbol)
                    
                    if not symbol or not date:
                        print(f"Skipping row with missing symbol or date: {row_dict}")
                        skipped_rows += 1
                    continue
                    
                    # Standardize date format if it's a string
                    if isinstance(date, str):
                        try:
                            date_obj = pd.to_datetime(date)
                            date = date_obj.strftime('%Y-%m-%d')
                        except:
                            # Keep original if parsing fails
                            pass
                    
                    # Extract base fields
                    period = row_dict.get('period')
                    reportedcurrency = row_dict.get('reportedcurrency')
                    fiscalyear = row_dict.get('fiscalyear')
                    data_source = row_dict.get('data_source', 'fmp')
                    
                    # Check for duplicate before inserting
                    check_cursor = conn.cursor()
                    check_cursor.execute("""
                        SELECT COUNT(*) FROM financial_metrics 
                        WHERE symbol = ? AND date = ? AND metric_type = ?
                    """, (symbol, date, metric_type))
                    already_exists = check_cursor.fetchone()[0] > 0
                    check_cursor.close()
                    
                    if already_exists:
                        print(f"Data already exists for {symbol}, {date}, {metric_type} - skipping")
                        skipped_rows += 1
                        continue
                        
                    # Build metric values
                    metric_values = {}
                    for key, value in row_dict.items():
                        if key not in ['id', 'symbol', 'date', 'period', 'reportedcurrency', 
                                       'fiscalyear', 'data_source', '_id'] and value is not None:
                            metric_values[key] = value
                    
                    # Convert to JSON string
                    metric_values_json = json.dumps(metric_values)
                    
                    # Insert into consolidated table
                    insert_cursor = conn.cursor()
                    insert_cursor.execute("""
                        INSERT INTO financial_metrics 
                        (symbol, date, period, reportedcurrency, fiscalyear, 
                         data_source, metric_values, metric_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol, date, period, reportedcurrency, fiscalyear,
                        data_source, metric_values_json, metric_type
                    ))
                    insert_cursor.close()
                    
                    total_rows += 1
                
                cursor.close()
                conn.commit()
                
            else:
                # PostgreSQL version using RealDictCursor
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Add symbol filtering if specified
                    if symbols:
                        placeholders = ', '.join(['%s'] * len(symbols))
                        cursor.execute(f"SELECT * FROM {table_name} WHERE symbol IN ({placeholders})", symbols)
                    else:
                        cursor.execute(f"SELECT * FROM {table_name}")
                    
                    for row in cursor:
                symbol = row.get('symbol')
                date = row.get('date')
                        
                        if symbol:
                            processed_symbols.add(symbol)
                        
                        if not symbol or not date:
                            print(f"Skipping row with missing symbol or date from {table_name}")
                            skipped_rows += 1
                            continue
                        
                        # Standardize date format if it's a string
                        if isinstance(date, str):
                            try:
                                date_obj = pd.to_datetime(date)
                                date = date_obj.strftime('%Y-%m-%d')
                            except:
                                # Keep original if parsing fails
                                pass
                        
                        # Extract base fields
                period = row.get('period')
                reportedcurrency = row.get('reportedcurrency')
                fiscalyear = row.get('fiscalyear')
                        data_source = row.get('data_source', 'fmp')
                        
                        # Check for duplicate before inserting
                    with conn.cursor() as check_cursor:
                        check_cursor.execute("""
                            SELECT COUNT(*) FROM financial_metrics 
                            WHERE symbol = %s AND date = %s AND metric_type = %s
                        """, (symbol, date, metric_type))
                            already_exists = check_cursor.fetchone()[0] > 0
                
                        if already_exists:
                    print(f"Data already exists for {symbol}, {date}, {metric_type} - skipping")
                    skipped_rows += 1
                    continue
                
                        # Build metric values
                metric_values = {}
                for key, value in row.items():
                            if key not in ['id', 'symbol', 'date', 'period', 'reportedcurrency', 
                                           'fiscalyear', 'data_source', '_id'] and value is not None:
                            metric_values[key] = value
                
                        # Convert to JSON
                metric_values_json = json.dumps(metric_values)
                
                        # Insert into consolidated table
                    with conn.cursor() as insert_cursor:
                        insert_cursor.execute("""
                            INSERT INTO financial_metrics 
                            (symbol, date, period, reportedcurrency, fiscalyear, 
                                 data_source, metric_values, metric_type)
                                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                                ON CONFLICT (symbol, date, metric_type) DO NOTHING
                        """, (
                            symbol, date, period, reportedcurrency, fiscalyear,
                                data_source, metric_values_json, metric_type
                        ))
                
                total_rows += 1
    
        print(f"Migration complete: {total_rows} rows inserted, {skipped_rows} rows skipped")
        print(f"Processed symbols: {', '.join(sorted(processed_symbols))}")
        
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        conn.close()
    
def migrate_text_tables(symbols=None):
    """Migrate data from text-based tables to consolidated text_metrics table"""
    conn = connect_to_db()
    
    total_rows = 0
    skipped_rows = 0
    
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
                # Add symbol filtering if specified
                if symbols:
                    placeholders = ', '.join(['?'] * len(symbols))
                    cursor.execute(f'SELECT * FROM "{table_name}" WHERE symbol IN ({placeholders})', symbols)
                else:
                cursor.execute(f'SELECT * FROM "{table_name}"')
                # Convert cursor rows to dictionaries
                rows = []
                for row in cursor.fetchall():
                    columns = [desc[0] for desc in cursor.description]
                    rows.append(dict(zip(columns, row)))
                cursor.close()
            else:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Add symbol filtering if specified
                    if symbols:
                        placeholders = ', '.join(['%s'] * len(symbols))
                        cursor.execute(f'SELECT * FROM "{table_name}" WHERE symbol IN ({placeholders})', symbols)
                    else:
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
                
                # Check if data already exists for this symbol, date, and metric_type
                if USE_SQLITE:
                    check_cursor = conn.cursor()
                    check_cursor.execute("""
                        SELECT COUNT(*) FROM text_metrics 
                        WHERE symbol = ? AND date = ? AND metric_type = ?
                    """, (symbol, date, metric_type))
                    exists = check_cursor.fetchone()[0] > 0
                    check_cursor.close()
                else:
                    with conn.cursor() as check_cursor:
                        check_cursor.execute("""
                            SELECT COUNT(*) FROM text_metrics 
                            WHERE symbol = %s AND date = %s AND metric_type = %s
                        """, (symbol, date, metric_type))
                        exists = check_cursor.fetchone()[0] > 0
                
                if exists:
                    print(f"Text data already exists for {symbol}, {date}, {metric_type} - skipping")
                    skipped_rows += 1
                    continue
                
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
    
    print(f"Successfully migrated {total_rows} rows to text_metrics table (skipped {skipped_rows} existing rows)")

def fetch_analyst_estimates(symbol):
    """Fetch analyst estimates for a given symbol from the API."""
    url = "https://financialmodelingprep.com/stable/analyst-estimates"
    params = {
        "symbol": symbol,
        "period": "annual",
        "page": 0,
        "limit": 10,
        "apikey": "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching analyst estimates: {response.status_code}")
        return None

def ensure_price_targets_table():
    """Ensure price_targets table exists in both SQLite and PostgreSQL"""
    if USE_SQLITE:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='price_targets'")
            if not cursor.fetchone():
                print("Creating price_targets table in SQLite...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS price_targets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        publishedDate TEXT,
                        newsURL TEXT,
                        newsTitle TEXT,
                        analystName TEXT,
                        priceTarget REAL,
                        adjPriceTarget REAL,
                        priceWhenPosted REAL,
                        newsPublisher TEXT,
                        newsBaseURL TEXT,
                        analystCompany TEXT,
                        data_source TEXT
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_targets_symbol ON price_targets(symbol)")
                print("price_targets table created in SQLite")
            else:
                print("price_targets table already exists in SQLite")
            
            conn.commit()
        except Exception as e:
            print(f"Error ensuring price_targets table in SQLite: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
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
                print("Creating price_target_news table in PostgreSQL...")
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
                cursor.execute('CREATE INDEX idx_price_target_news_symbol ON price_target_news(symbol)')
                # Create unique constraint
                cursor.execute("""
                    CREATE UNIQUE INDEX idx_price_target_news_unique 
                    ON price_target_news(symbol, published_date, COALESCE(analyst_name, ''), price_target)
                """)
                print("price_target_news table created in PostgreSQL")
            else:
                print("price_target_news table already exists in PostgreSQL")
            
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
                    ON price_target_news(symbol, published_date, COALESCE(analyst_name, ''), price_target)
                """)
                print("Unique constraint added")
                
            conn.commit()
        except Exception as e:
            print(f"Error ensuring price_target_news table in PostgreSQL: {e}")
        finally:
            cursor.close()
            conn.close()

def fetch_price_target_news(symbol):
    """Fetch price target news for a given symbol from the API."""
    # Ensure price targets table exists
    ensure_price_targets_table()
    
    url = "https://financialmodelingprep.com/stable/price-target-news"
    params = {
        "symbol": symbol,
        "page": 0,
        "limit": 10,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        
        # Save raw data for debugging
        save_raw_json(data, "price_target_news", symbol)
        
        # Insert into database directly
        try:
            conn = connect_to_db()
            cursor = conn.cursor()
            
            inserted_count = 0
            skipped_count = 0
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
                    # Check if we're using SQLite or PostgreSQL
                    if USE_SQLITE:
                        # Check if record already exists
                        cursor.execute("""
                            SELECT COUNT(*) FROM price_targets 
                            WHERE symbol = ? AND publishedDate = ? AND 
                                  analystName = ? AND priceTarget = ?
                        """, (symbol, published_date, analyst_name, price_target))
                        
                        if cursor.fetchone()[0] > 0:
                            print(f"Price target already exists for {symbol}, {published_date}, {analyst_name} - skipping")
                            skipped_count += 1
                            continue
                        
                        cursor.execute("""
                            INSERT OR IGNORE INTO price_targets 
                            (symbol, publishedDate, newsURL, newsTitle, analystName, 
                             priceTarget, priceWhenPosted, analystCompany, newsPublisher, data_source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            symbol, published_date, news_url, news_title, analyst_name,
                            price_target, price_when_posted, analyst_company, news_publisher, "price_target_news"
                        ))
                    else:
                        # Check if record already exists
                        cursor.execute("""
                            SELECT COUNT(*) FROM price_target_news 
                            WHERE symbol = %s AND published_date = %s AND 
                                  COALESCE(analyst_name, '') = %s AND price_target = %s
                        """, (symbol, published_date, analyst_name, price_target))
                        
                        if cursor.fetchone()[0] > 0:
                            print(f"Price target already exists for {symbol}, {published_date}, {analyst_name} - skipping")
                            skipped_count += 1
                            continue
                        
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
            cursor.close()
            conn.close()
            print(f"Processed {len(data)} price targets for {symbol}, inserted {inserted_count} new records, skipped {skipped_count} existing records")
            return data
        except Exception as e:
            print(f"Error inserting price target news: {e}")
            return data
    else:
        print(f"Error fetching price target news: {response.status_code}")
        return None

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
        
        # Process quarterly financial statements for all years since 2020
        quarterly_endpoints = ["income_statement", "balance_sheet_statement", "cash_flow_statement"]
        for endpoint_name in quarterly_endpoints:
            endpoint_config = ENDPOINTS[endpoint_name]
            for year in YEARS:  # All years from 2020-2025
                # Fetch quarterly data with year parameter
                # Adding 'limit=400' to ensure we get all quarters for each year
                if "additional_params" not in endpoint_config:
                    endpoint_config["additional_params"] = {}
                endpoint_config["additional_params"]["limit"] = 400
                
                # Fetch with both period=quarter and specific year
                data = fetch_api_data(endpoint_name, endpoint_config, symbol, period="quarter", year=year)
                save_to_csv(data, endpoint_name, symbol, year=year, period="quarter")
                time.sleep(1)
        
        # Process all other endpoints
        for endpoint_name, endpoint_config in ENDPOINTS.items():
            if endpoint_name != "earning_call_transcript" and endpoint_name not in quarterly_endpoints:
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

def fetch_price_targets_only(symbols):
    """Fetch only price target data for the given symbols"""
    print(f"Fetching price target data for {len(symbols)} symbols")
    
    for symbol in symbols:
        try:
            print(f"Processing price target data for {symbol}")
            data = fetch_price_target_news(symbol)
            if data:
                print(f"Found {len(data)} price targets for {symbol}")
            else:
                print(f"No price target data found for {symbol}")
        except Exception as e:
            print(f"Error processing price target data for {symbol}: {e}")

def ensure_stock_peers_table():
    """Ensure stock_peers table exists in both SQLite and PostgreSQL"""
    if USE_SQLITE:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_peers'")
            if not cursor.fetchone():
                print("Creating stock_peers table in SQLite...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stock_peers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        peer_symbol TEXT NOT NULL,
                        company_name TEXT,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, peer_symbol)
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_peers_symbol ON stock_peers(symbol)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_peers_peer_symbol ON stock_peers(peer_symbol)")
                print("stock_peers table created in SQLite")
            else:
                print("stock_peers table already exists in SQLite")
            
            conn.commit()
        except Exception as e:
            print(f"Error ensuring stock_peers table in SQLite: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
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
                print("Creating stock_peers table in PostgreSQL...")
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
                print("stock_peers table created in PostgreSQL")
            else:
                print("stock_peers table already exists in PostgreSQL")
                
            conn.commit()
        except Exception as e:
            print(f"Error ensuring stock_peers table in PostgreSQL: {e}")
        finally:
            cursor.close()
            conn.close()

def fetch_stock_peers(symbol):
    """Fetch stock peers for a given symbol from the API."""
    # Ensure stock peers table exists
    ensure_stock_peers_table()
    
    url = f"{BASE_URL}/stock-peers"
    params = {
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Save raw data for debugging
        save_raw_json(data, "stock_peers", symbol)
        
        # Insert into database directly
        conn = connect_to_db()
        cursor = conn.cursor()
        
        try:
            # Instead of removing existing peers, we'll update them if they exist
            # or insert new ones if they don't
            
            # Get existing peers
            existing_peers = set()
            if USE_SQLITE:
                cursor.execute("SELECT peer_symbol FROM stock_peers WHERE symbol = ?", (symbol,))
                existing_peers = {row[0] for row in cursor.fetchall()}
            else:
                cursor.execute("SELECT peer_symbol FROM stock_peers WHERE symbol = %s", (symbol,))
                existing_peers = {row[0] for row in cursor.fetchall()}
            
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            
            for item in data:
                # Extract the fields we need
                peer_symbol = item.get("symbol")
                company_name = item.get("companyName")
                
                # Skip if peer_symbol is the same as the main symbol
                if peer_symbol == symbol:
                    continue
                
                try:
                    # Check if we're using SQLite or PostgreSQL
                    if USE_SQLITE:
                        if peer_symbol in existing_peers:
                            # Update existing record
                            cursor.execute("""
                                UPDATE stock_peers 
                                SET company_name = ?, updated_at = CURRENT_TIMESTAMP
                                WHERE symbol = ? AND peer_symbol = ?
                            """, (company_name, symbol, peer_symbol))
                            updated_count += 1
                        else:
                            # Insert new record
                            cursor.execute("""
                                INSERT OR REPLACE INTO stock_peers 
                                (symbol, peer_symbol, company_name)
                                VALUES (?, ?, ?)
                            """, (symbol, peer_symbol, company_name))
                            inserted_count += 1
                    else:
                        if peer_symbol in existing_peers:
                            # Update existing record
                            cursor.execute("""
                                UPDATE stock_peers 
                                SET company_name = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE symbol = %s AND peer_symbol = %s
                            """, (company_name, symbol, peer_symbol))
                            updated_count += 1
                        else:
                            # Insert new record
                            cursor.execute("""
                                INSERT INTO stock_peers 
                                (symbol, peer_symbol, company_name)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (symbol, peer_symbol) DO UPDATE 
                                SET company_name = EXCLUDED.company_name,
                                    updated_at = CURRENT_TIMESTAMP
                            """, (symbol, peer_symbol, company_name))
                            inserted_count += 1
                except Exception as e:
                    print(f"Error inserting/updating peer record: {e}")
                    skipped_count += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Processed {len(data)} peers for {symbol}: inserted {inserted_count} new records, updated {updated_count} existing records, skipped {skipped_count} records")
            return data
        except Exception as e:
            print(f"Error inserting stock peers: {e}")
            conn.close()
            return data
    else:
        print(f"Error fetching stock peers: {response.status_code}")
        return None

def fetch_peers_only(symbols):
    """Fetch only stock peers data for the given symbols"""
    print(f"Fetching stock peers data for {len(symbols)} symbols")
    
    for symbol in symbols:
        try:
            print(f"Processing stock peers data for {symbol}")
            data = fetch_stock_peers(symbol)
            if data:
                print(f"Found {len(data)} peers for {symbol}")
                
                # If --peers-with-data flag is set, also fetch financial data for peers
                if args.peers_with_data:
                    fetch_peers_data(symbol)
            else:
                print(f"No stock peers data found for {symbol}")
        except Exception as e:
            print(f"Error processing stock peers data for {symbol}: {e}")

def fetch_peers_data(symbol):
    """Fetch stock peers and their financial data"""
    print(f"Fetching stock peers and their financial data for {symbol}")
    
    # Step 1: Fetch and store peers
    peers_data = fetch_stock_peers(symbol)
    
    if not peers_data or len(peers_data) == 0:
        print(f"No peers found for {symbol}")
        return
    
    # Step 2: Fetch financial data for each peer
    for peer in peers_data:
        peer_symbol = peer.get("symbol")
        if peer_symbol == symbol:
            continue  # Skip the original symbol
            
        print(f"Fetching financial data for peer: {peer_symbol}")
        try:
            # Process key financial endpoints for the peer
            for endpoint_name in ["income_statement", "balance_sheet_statement", "cash_flow_statement", "ratios"]:
                endpoint_config = ENDPOINTS[endpoint_name]
                # Fetch annual data
                annual_data = fetch_api_data(endpoint_name, endpoint_config, peer_symbol, period="annual")
                save_to_csv(annual_data, endpoint_name, peer_symbol, period="annual")
                # Fetch quarterly data
                quarterly_data = fetch_api_data(endpoint_name, endpoint_config, peer_symbol, period="quarter")
                save_to_csv(quarterly_data, endpoint_name, peer_symbol, period="quarter")
                time.sleep(1)  # Avoid rate limiting
            
            print(f"Successfully fetched financial data for {peer_symbol}")
        except Exception as e:
            print(f"Error fetching financial data for peer {peer_symbol}: {e}")

def ensure_financial_ratios_constraint():
    """Ensure the financial_ratios table has the necessary unique constraint on (symbol, date)"""
    print("Checking financial_ratios table for unique constraint...")
    
    if USE_SQLITE:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='financial_ratios'")
            if not cursor.fetchone():
                print("financial_ratios table does not exist in SQLite, skipping constraint check")
        return
    
            # Check if the constraint exists
            cursor.execute("PRAGMA index_list('financial_ratios')")
            has_unique_constraint = False
            for row in cursor.fetchall():
                if row[2] == 1:  # is_unique column
                    cursor.execute(f"PRAGMA index_info('{row[1]}')")
                    columns = [info[2] for info in cursor.fetchall()]
                    if set(columns) == set(['symbol', 'date']):
                        has_unique_constraint = True
                        break
            
            if not has_unique_constraint:
                print("Adding unique constraint to financial_ratios table in SQLite")
                # First check for duplicates
                cursor.execute("""
                    SELECT symbol, date, COUNT(*)
                    FROM financial_ratios
                    GROUP BY symbol, date
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"Found {len(duplicates)} symbol-date combinations with duplicates")
                    # Keep only one row for each symbol-date combination
                    for symbol, date, count in duplicates:
                        cursor.execute("""
                            DELETE FROM financial_ratios
                            WHERE id IN (
                                SELECT id FROM financial_ratios
                                WHERE symbol = ? AND date = ?
                                ORDER BY id
                                LIMIT -1 OFFSET 1
                            )
                        """, (symbol, date))
                    conn.commit()
                    print(f"Removed duplicates from financial_ratios table")
                
                # Add unique constraint
                cursor.execute("CREATE UNIQUE INDEX idx_financial_ratios_symbol_date ON financial_ratios(symbol, date)")
                conn.commit()
                print("Added unique constraint to financial_ratios table in SQLite")
            else:
                print("financial_ratios table already has unique constraint in SQLite")
        except Exception as e:
            print(f"Error ensuring SQLite constraint: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'financial_ratios'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print("financial_ratios table does not exist in PostgreSQL, skipping constraint check")
        return
    
            # Check if unique constraint exists
            cursor.execute("""
                SELECT COUNT(*)
                FROM pg_constraint pc
                JOIN pg_class c ON pc.conrelid = c.oid
                WHERE c.relname = 'financial_ratios'
                  AND pc.contype = 'u'
            """)
            has_unique_constraint = cursor.fetchone()[0] > 0
            
            if not has_unique_constraint:
                print("Adding unique constraint to financial_ratios table in PostgreSQL")
                # First check for duplicates
                cursor.execute("""
                    SELECT symbol, date, COUNT(*)
                    FROM financial_ratios
                    GROUP BY symbol, date
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"Found {len(duplicates)} symbol-date combinations with duplicates")
                    # Keep only one row for each symbol-date combination
                    for symbol, date, count in duplicates:
                        cursor.execute("""
                            WITH duplicates AS (
                                SELECT id,
                                      ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY id) as row_num
                                FROM financial_ratios
                                WHERE symbol = %s AND date = %s
                            )
                            DELETE FROM financial_ratios
                            WHERE id IN (SELECT id FROM duplicates WHERE row_num > 1)
                        """, (symbol, date))
                    conn.commit()
                    print(f"Removed duplicates from financial_ratios table")
                
                # Add unique constraint
                cursor.execute("""
                    ALTER TABLE financial_ratios
                    ADD CONSTRAINT financial_ratios_symbol_date_unique UNIQUE (symbol, date)
                """)
                conn.commit()
                print("Added unique constraint to financial_ratios table in PostgreSQL")
            else:
                print("financial_ratios table already has unique constraint in PostgreSQL")
                
            # Make sure we also have a mapping from calendarYear to fiscalyear
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'financial_ratios'
            """)
            column_names = [row[0].lower() for row in cursor.fetchall()]
            print(f"financial_ratios has {len(column_names)} columns")
            
            # Make sure we're aware of the mapping needed for the API response
            print("Note: API may return 'calendarYear' field which should be mapped to 'fiscalyear' if exists")
            
        except Exception as e:
            print(f"Error ensuring PostgreSQL constraint: {e}")
        finally:
            cursor.close()
            conn.close()

def process_financial_ratios(data, overwrite=False):
    """Process financial ratios data and insert into database"""
    if not data:
        print("No financial ratios data provided.")
        return
        
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        # Preprocess financial ratios data
        processed_data = []
        symbols_processed = set()
        
        for item in data:
            symbol = item.get('symbol')
            if symbol:
                symbols_processed.add(symbol)
                
            # Process date field
            if 'date' in item and item['date']:
                # Try to standardize date format for consistent comparisons
                try:
                    date_obj = pd.to_datetime(item['date'])
                    item['date'] = date_obj.strftime('%Y-%m-%d')
                except:
                    # If conversion fails, keep original
                    pass
                
            # Validate key fields
            if not symbol or not item.get('date'):
                print(f"Skipping financial ratio with missing symbol or date: {json.dumps(item)[:100]}...")
                continue
                
            processed_data.append(item)
        
        print(f"Processing financial ratios for symbols: {', '.join(symbols_processed)}")
        
        # Process the pre-processed data
        insert_count = 0
        skipped_count = 0
        
        # First check if table has the required unique constraint
        has_unique_constraint = False
        if not USE_SQLITE:
            cur.execute("""
                SELECT conname AS constraint_name, 
                       pg_catalog.pg_get_constraintdef(pc.oid) AS constraint_def
                FROM pg_constraint pc
                JOIN pg_class c ON pc.conrelid = c.oid
                WHERE c.relname = 'financial_ratios'
                  AND pc.contype = 'u'
            """)
            constraints = cur.fetchall()
            for constraint in constraints:
                if 'UNIQUE (symbol, date)' in constraint[1]:
                    has_unique_constraint = True
                    print("Found unique constraint on financial_ratios(symbol, date)")
                    break
            
            if not has_unique_constraint:
                print("WARNING: No unique constraint found on financial_ratios(symbol, date). ON CONFLICT clause will be disabled.")

            # Get the actual column names from the database for case mapping
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'financial_ratios'
            """)
            db_columns = [row[0].lower() for row in cur.fetchall()]
            print(f"Database has {len(db_columns)} columns for financial_ratios table")
        
        for item in processed_data:
            symbol = item.get('symbol')
            date = item.get('date')
            
            # Check if data already exists in financial_ratios or financial_metrics
            exists = False
            
            if USE_SQLITE:
                # Check in financial_ratios
                cur.execute("""
                    SELECT COUNT(*) FROM financial_ratios 
                    WHERE symbol = ? AND date = ?
                """, (symbol, date))
                exists = cur.fetchone()[0] > 0
                
                # Also check in financial_metrics with metric_type = 'ratio'
                if not exists:
                    cur.execute("""
                        SELECT COUNT(*) FROM financial_metrics 
                        WHERE symbol = ? AND date = ? AND metric_type = 'ratio'
                    """, (symbol, date))
                    exists = cur.fetchone()[0] > 0
            else:
                # Check in financial_ratios
                cur.execute("""
                    SELECT COUNT(*) FROM financial_ratios 
                    WHERE symbol = %s AND date = %s
                """, (symbol, date))
                exists = cur.fetchone()[0] > 0
                
                # Also check in financial_metrics with metric_type = 'ratio'
                if not exists:
                    cur.execute("""
                        SELECT COUNT(*) FROM financial_metrics 
                        WHERE symbol = %s AND date = %s AND metric_type = 'ratio'
                    """, (symbol, date))
                    exists = cur.fetchone()[0] > 0
            
            if exists and not overwrite:
                skipped_count += 1
                print(f"Data already exists for {symbol}, {date} - skipping.")
                continue
            
            # Prepare data with proper case handling for PostgreSQL
            if not USE_SQLITE:
                # Convert API column names to match database column names (case-sensitive)
                processed_item = {}
                for key, value in item.items():
                    # Convert API keys to lowercase to match DB column names
                    db_key = key.lower()
                    
                    # Map calendarYear to fiscalyear if needed
                    if db_key == 'calendaryear' and 'fiscalyear' in db_columns:
                        db_key = 'fiscalyear'
                        
                    # Only include fields that exist in the database
                    if db_key in db_columns and key not in ["_id", "id"]:
                        processed_item[db_key] = value
                
                # Column names for insertion
                columns = list(processed_item.keys())
                placeholders = ['%s'] * len(columns)
                values = [processed_item[col] for col in columns]
            else:
                # For SQLite (less case sensitive)
                columns = []
                placeholders = []
                values = []
                
                for key, value in item.items():
                    if key not in ["_id", "id"]:  # Exclude MongoDB objectId and any id field
                        # For SQLite, just handle calendarYear mapping if needed
                        if key.lower() == 'calendaryear':
                            key = 'fiscalyear'
                        columns.append(key)
                        values.append(value)
                        placeholders.append('?')
            
            # Generate INSERT or UPSERT statement based on database type
            if USE_SQLITE:
                query = f"""
                    INSERT INTO financial_ratios ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
            else:
                query = f"""
                    INSERT INTO financial_ratios ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                
                # Add ON CONFLICT clause only if we have the unique constraint
                if has_unique_constraint:
                    # Build update columns excluding symbol and date
                    update_columns = []
                    for col in columns:
                        if col not in ['symbol', 'date']:
                            update_columns.append(f"{col} = EXCLUDED.{col}")
                    
                    if update_columns:
                        query += f"""
                        ON CONFLICT (symbol, date) DO UPDATE SET 
                        {", ".join(update_columns)}
                        """
                    else:
                        # If no columns to update, use DO NOTHING
                        query += " ON CONFLICT (symbol, date) DO NOTHING"
                
            # Execute INSERT or UPSERT query
            try:
                cur.execute(query, tuple(values))
                insert_count += 1
            except Exception as e:
                print(f"Error inserting financial ratio for {symbol}, {date}: {e}")
                print(f"Query: {query}")
                print(f"Column count: {len(columns)}, Value count: {len(values)}")
        
        # Commit all changes to database
        conn.commit()
        
        print(f"Successfully processed {insert_count} financial ratios (skipped {skipped_count} existing records)")
        
    except Exception as e:
        print(f"Error processing financial ratios: {e}")
    finally:
        cur.close()
        conn.close()

def ensure_financial_metrics_constraint():
    """Ensure financial_metrics table has unique constraint on (symbol, date, metric_type)"""
    print("Checking financial_metrics table for unique constraint...")
    
    if USE_SQLITE:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='financial_metrics'")
            if not cursor.fetchone():
                print("financial_metrics table does not exist in SQLite, skipping constraint check")
                return
            
            # Check if the constraint exists
            cursor.execute("PRAGMA index_list('financial_metrics')")
            has_unique_constraint = False
            for row in cursor.fetchall():
                if row[2] == 1:  # is_unique column
                    cursor.execute(f"PRAGMA index_info('{row[1]}')")
                    columns = [info[2] for info in cursor.fetchall()]
                    if set(columns) == set(['symbol', 'date', 'metric_type']):
                        has_unique_constraint = True
                        break
            
            if not has_unique_constraint:
                print("Adding unique constraint to financial_metrics table in SQLite")
                # First check for duplicates
                cursor.execute("""
                    SELECT symbol, date, metric_type, COUNT(*)
                    FROM financial_metrics
                    GROUP BY symbol, date, metric_type
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"Found {len(duplicates)} symbol-date-metric_type combinations with duplicates")
                    # Keep only one row for each symbol-date-metric_type combination
                    for symbol, date, metric_type, count in duplicates:
                        cursor.execute("""
                            DELETE FROM financial_metrics
                            WHERE id IN (
                                SELECT id FROM financial_metrics
                                WHERE symbol = ? AND date = ? AND metric_type = ?
                                ORDER BY id
                                LIMIT -1 OFFSET 1
                            )
                        """, (symbol, date, metric_type))
                    conn.commit()
                    print(f"Removed duplicates from financial_metrics table")
                
                # Add unique constraint
                cursor.execute("CREATE UNIQUE INDEX idx_financial_metrics_symbol_date_metric_type ON financial_metrics(symbol, date, metric_type)")
                conn.commit()
                print("Added unique constraint to financial_metrics table in SQLite")
            else:
                print("financial_metrics table already has unique constraint in SQLite")
        except Exception as e:
            print(f"Error ensuring SQLite constraint on financial_metrics: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        conn = connect_to_db()
        cursor = conn.cursor()
        try:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'financial_metrics'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print("financial_metrics table does not exist in PostgreSQL, skipping constraint check")
                return
            
            # Check if unique constraint exists
            cursor.execute("""
                SELECT conname AS constraint_name, pg_catalog.pg_get_constraintdef(pc.oid) AS constraint_def
                FROM pg_constraint pc
                JOIN pg_class c ON pc.conrelid = c.oid
                WHERE c.relname = 'financial_metrics'
                  AND pc.contype = 'u'
            """)
            
            constraints = cursor.fetchall()
            has_constraint = False
            
            for constraint in constraints:
                if 'UNIQUE (symbol, date, metric_type)' in constraint[1]:
                    has_constraint = True
                    print(f"Found unique constraint on financial_metrics: {constraint[0]}")
                    break
            
            if not has_constraint:
                print("Adding unique constraint to financial_metrics table in PostgreSQL")
                # First check for duplicates
                cursor.execute("""
                    SELECT symbol, date, metric_type, COUNT(*)
                    FROM financial_metrics
                    GROUP BY symbol, date, metric_type
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"Found {len(duplicates)} symbol-date-metric_type combinations with duplicates")
                    # Keep only one row for each symbol-date-metric_type combination
                    for symbol, date, metric_type, count in duplicates:
                        cursor.execute("""
                            WITH duplicates AS (
                                SELECT id,
                                      ROW_NUMBER() OVER (PARTITION BY symbol, date, metric_type ORDER BY id) as row_num
                                FROM financial_metrics
                                WHERE symbol = %s AND date = %s AND metric_type = %s
                            )
                            DELETE FROM financial_metrics
                            WHERE id IN (SELECT id FROM duplicates WHERE row_num > 1)
                        """, (symbol, date, metric_type))
                    conn.commit()
                    print(f"Removed duplicates from financial_metrics table")
                
                # Add unique constraint
                cursor.execute("""
                    ALTER TABLE financial_metrics
                    ADD CONSTRAINT financial_metrics_symbol_date_metric_type_unique UNIQUE (symbol, date, metric_type)
                """)
                conn.commit()
                print("Added unique constraint to financial_metrics table in PostgreSQL")
            else:
                print("financial_metrics table already has unique constraint in PostgreSQL")
        except Exception as e:
            print(f"Error ensuring PostgreSQL constraint on financial_metrics: {e}")
        finally:
            cursor.close()
            conn.close()

def main():
    """Main function to run the entire ETL process"""
    print("Starting Financial Data ETL Process...")
    
    # Handle legacy argument mapping
    if args.price_targets_only:
        mode = 'price_targets'
    elif args.peers_only:
        mode = 'peers'
    elif args.migrate_only:
        mode = 'migrate'
    else:
        mode = args.mode
    
    # Create database schema if needed
    print("Creating database schema...")
    create_database_schema()
    
    # Ensure necessary constraints are in place
    print("Ensuring database constraints...")
    ensure_financial_ratios_constraint()
    ensure_financial_metrics_constraint()
    
    # Handle symbol selection
    if args.symbols:
        symbols_to_process = args.symbols.split(',')
    else:
        symbols_to_process = SYMBOLS
    
    print(f"Processing symbols: {', '.join(symbols_to_process)}")
    
    # Process based on mode
    if mode == 'price_targets':
        print("Mode: Price targets only")
        fetch_price_targets_only(symbols_to_process)
    elif mode == 'peers':
        print("Mode: Stock peers only")
        fetch_peers_only(symbols_to_process)
    elif mode == 'ratio':
        print("Mode: Financial ratios only")
        for symbol in symbols_to_process:
            print(f"Fetching financial ratios for {symbol}")
            ratios_data = fetch_financial_ratios(symbol)
            if ratios_data:
                process_financial_ratios(ratios_data)
    elif mode == 'api':
        print("Mode: API data fetch only")
        for symbol in symbols_to_process:
            process_symbol_data(symbol)
    elif mode == 'migrate':
        print("Mode: Migration only")
        create_consolidated_files()
        migrate_to_consolidated_tables(symbols_to_process)
    elif mode == 'consolidate':
        print("Mode: Consolidation only")
        migrate_to_consolidated_tables(symbols_to_process)
    elif mode == 'all':
        print("Mode: Full ETL process")
        # Step 1: Fetch API data
        for symbol in symbols_to_process:
            print(f"Processing symbol: {symbol}")
            process_symbol_data(symbol)
        
        # Step 2: Process financial ratios separately
        for symbol in symbols_to_process:
            print(f"Processing financial ratios for {symbol}")
            ratios_data = fetch_financial_ratios(symbol)
            if ratios_data:
                process_financial_ratios(ratios_data)
        
        # Step 3: Create consolidated files and migrate
        print("Creating consolidated files...")
        create_consolidated_files()
        
        print("Migrating to consolidated tables...")
        migrate_to_consolidated_tables(symbols_to_process)
    else:
        print(f"Unknown mode: {mode}")
        
    print("ETL process completed!")

def fetch_financial_ratios(symbol):
    """Fetch financial ratios data for a given symbol"""
    url = f"{BASE_URL}/ratios/{symbol}?apikey={API_KEY}&limit=120"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            # Process the financial ratios data
            processed_data = []
            
            for item in data:
                # Add symbol field if missing
                if 'symbol' not in item:
                    item['symbol'] = symbol
                    
                # Fix date format if needed
                date = item.get('date')
                if date:
                    try:
                        date_obj = pd.to_datetime(date)
                        item['date'] = date_obj.strftime('%Y-%m-%d')
                    except:
                        # Keep original if conversion fails
                        pass
                
                # Add data_source field
                item['data_source'] = 'fmp'
                
                processed_data.append(item)
            
            print(f"Found {len(processed_data)} financial ratios records for {symbol}")
            return processed_data
        else:
            print(f"Failed to fetch financial ratios data for {symbol}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching financial ratios for {symbol}: {e}")
        return None

if __name__ == "__main__":
    main() 