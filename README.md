# Financial Modeling Prep ETL

A Python-based ETL (Extract, Transform, Load) pipeline for financial data from the Financial Modeling Prep API.

## Overview

This project provides tools to extract financial data from Financial Modeling Prep API (income statements, balance sheets, cash flow statements, analyst estimates, and more), transform it into structured formats, and load it into both SQLite and PostgreSQL databases.

## Features

- **Data Extraction**: Fetches financial data from multiple FMP API endpoints
- **Data Transformation**: Converts API responses to CSV format for easy analysis
- **Data Loading**: Populates both traditional and consolidated database tables
- **Multiple Company Support**: Process data for multiple ticker symbols (AAPL, MSFT, GOOGL, etc.)
- **Database Compatibility**: Works with both SQLite (local) and PostgreSQL (remote)
- **Database Validation**: Includes utilities to verify data integrity and perform sanity checks
- **Stock Peers**: Fetch and store peer companies for any stock symbol
- **Price Targets**: Retrieve and store analyst price targets and recommendations
- **API Integration**: Built-in API for accessing financial metrics data

## Components

- **fmp-etl.py**: Main ETL script that handles the entire pipeline
- **fetch_stock_peers.py**: Standalone script for fetching peer companies
- **fetch_price_targets.py**: Standalone script for fetching analyst price targets
- **add_peer_ratio_data.py**: Helper script to add financial ratio data for peers
- **test_local_api.py**: Script for testing the financial metrics API endpoints

## Usage

```bash
# Run ETL with SQLite (default)
python fmp-etl.py --symbols AAPL,MSFT,GOOGL

# Run ETL with PostgreSQL
python fmp-etl.py --symbols AAPL,MSFT,GOOGL --db-type postgres --db-name finmetrics

# Fetch only stock peers data
python fmp-etl.py --symbols AAPL --peers-only

# Fetch stock peers data with financial metrics
python fmp-etl.py --symbols AAPL --peers-with-data

# Fetch only price target data
python fmp-etl.py --symbols AAPL --price-targets-only

# Run standalone peer companies script
python fetch_stock_peers.py AAPL

# Run standalone price targets script
python fetch_price_targets.py AAPL
```

## Requirements

- Python 3.7+
- pandas
- requests
- psycopg2 (for PostgreSQL support)
- sqlite3 (included in Python standard library)
- fastapi (for API functionality)
- uvicorn (for serving the API)

## Database Schema

The ETL process creates several tables:
- Traditional tables for each data type (income_statements, balance_sheets, etc.)
- Consolidated tables (financial_metrics, text_metrics) for more efficient querying
- Stock peers table (stock_peers) for storing peer company relationships
- Price targets table (price_targets) for storing analyst recommendations

## API Endpoints

The included API provides several endpoints for accessing financial data:
- `/peer-metrics/{symbol}`: Get financial metrics for a company's peers
- `/price-target-news/{symbol}`: Get analyst price targets for a company
- `/analyst-estimates/{symbol}`: Get consensus earnings estimates for a company

## License

This project is licensed under the MIT License - see the LICENSE file for details. 