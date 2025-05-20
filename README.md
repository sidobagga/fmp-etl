# FMP ETL Pipeline

A data extraction, transformation, and loading (ETL) pipeline for Financial Modeling Prep (FMP) data.

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/sidobagga/fmp-etl.git
   cd fmp-etl
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your API keys and configuration:
   ```
   FMP_API_KEY=your_fmp_api_key_here
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=fmp_database
   DB_USER=postgres
   DB_PASSWORD=your_db_password_here
   LOG_LEVEL=INFO
   ```

## Usage

Run the ETL process:
```
python fmp-etl.py
```

## Notes

- The `.env` file is excluded from version control in the `.gitignore` file for security.
- Make sure to update the `.env` file with your actual credentials. 