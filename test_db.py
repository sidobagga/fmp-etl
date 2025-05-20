#!/usr/bin/env python3
import os
import sys
import traceback
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

# Create a test metadata object
test_metadata = MetaData()

# Define a simple test table
test_table = Table(
    "test_table", test_metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
)

def init_test_db(db_path):
    print(f"Initializing test database at: {db_path}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Absolute path: {os.path.abspath(db_path)}")
    
    uri = f"sqlite:///{db_path}"
    print(f"SQLAlchemy URI: {uri}")
    
    try:
        engine = create_engine(uri)
        print(f"Engine created: {engine}")
        
        test_metadata.create_all(engine)
        print("Tables created successfully")
        
        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print(f"Traceback:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "test.db"
    init_test_db(db_path) 