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

## Components

- **financial_etl.py**: Main ETL script that handles the entire pipeline
- **db_checks.py**: Utilities for validating database content and data quality

## Usage

```bash
# Run ETL with SQLite (default)
python financial_etl.py --symbols AAPL,MSFT,GOOGL

# Run ETL with PostgreSQL
python financial_etl.py --symbols AAPL,MSFT,GOOGL --db-type postgres --db-name finmetrics

# Perform database checks
python db_checks.py
```

## Requirements

- Python 3.7+
- pandas
- requests
- psycopg2 (for PostgreSQL support)
- sqlite3 (included in Python standard library)

## Database Schema

The ETL process creates several tables:
- Traditional tables for each data type (income_statements, balance_sheets, etc.)
- Consolidated tables (financial_metrics, text_metrics) for more efficient querying

## License

This project is licensed under the MIT License - see the LICENSE file for details. 