#!/usr/bin/env python3
import subprocess
import time
import requests
import json
import sys
import os

# API details
API_PATH = "/Users/sidbagga/insight-apis-1/financialMetrics-api.py"
API_PORT = 8050
API_HOST = "127.0.0.1"

def run_api_server():
    """Start the API server as a subprocess"""
    print("Starting the Financial Metrics API server...")
    try:
        # Start uvicorn server
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "financialMetrics-api:app", "--host", API_HOST, "--port", str(API_PORT)],
            cwd=os.path.dirname(API_PATH),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Give it time to start
        time.sleep(3)
        
        # Check if the server started successfully
        if process.poll() is not None:
            print("Failed to start API server")
            stdout, stderr = process.communicate()
            print(f"STDOUT: {stdout.decode('utf-8')}")
            print(f"STDERR: {stderr.decode('utf-8')}")
            return None
        
        print(f"API server running at http://{API_HOST}:{API_PORT}")
        return process
    except Exception as e:
        print(f"Error starting API server: {e}")
        return None

def test_api_endpoints(process):
    """Test various API endpoints"""
    if not process:
        return
    
    base_url = f"http://{API_HOST}:{API_PORT}"
    
    try:
        # Test 1: Root endpoint
        print("\n=== Test 1: Root Endpoint ===")
        response = requests.get(f"{base_url}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Test 2: Stock Peers Data for AAPL
        print("\n=== Test 2: Stock Peers Data for AAPL ===")
        response = requests.get(f"{base_url}/peer-metrics/AAPL")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} peer companies")
            if data:
                for i, peer in enumerate(data[:5]):  # Show first 5 peers
                    print(f"\nPeer {i+1}:")
                    print(f"Symbol: {peer.get('symbol')}")
                    print(f"Company Name: {peer.get('companyName')}")
                    print(f"Revenue: ${peer.get('revenue', 'N/A')}")
                    print(f"Gross Profit Margin: {peer.get('grossProfitMargin', 'N/A')}")
                    print(f"EBITDA Margin: {peer.get('ebitdaMargin', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 3: Analyst Estimates for AAPL
        print("\n=== Test 3: Analyst Estimates for AAPL ===")
        response = requests.get(f"{base_url}/analyst-estimates/AAPL")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} analyst estimate records")
            if data:
                for i, estimate in enumerate(data[:3]):  # Show first 3 estimates
                    print(f"\nEstimate {i+1}:")
                    print(f"Date: {estimate.get('date')}")
                    print(f"Period: {estimate.get('period')}")
                    print(f"Revenue Avg: ${estimate.get('revenueavg', 'N/A')}")
                    print(f"EPS Avg: ${estimate.get('epsavg', 'N/A')}")
                    print(f"Number of Analysts (Revenue): {estimate.get('numanalystsrevenue', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 4: Price Target News for AAPL
        print("\n=== Test 4: Price Target News for AAPL ===")
        response = requests.get(f"{base_url}/price-target-news/AAPL")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} price target news records")
            if data:
                for i, news in enumerate(data[:3]):  # Show first 3 news
                    print(f"\nPrice Target {i+1}:")
                    print(f"Published Date: {news.get('published_date')}")
                    print(f"Analyst: {news.get('analyst_name')} ({news.get('analyst_company', 'N/A')})")
                    print(f"Price Target: ${news.get('price_target')}")
                    print(f"Price When Posted: ${news.get('price_when_posted')}")
                    print(f"News Title: {news.get('news_title')}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 5: Check available companies
        print("\n=== Test 5: Available Companies ===")
        response = requests.get(f"{base_url}/companies")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            companies = response.json()
            print(f"Found {len(companies)} companies")
            if len(companies) > 10:
                print(f"First 10 companies: {companies[:10]}")
            else:
                print(f"Companies: {companies}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 6: Get available metrics for income type
        print("\n=== Test 6: Available Income Metrics ===")
        response = requests.get(f"{base_url}/metrics/income")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            metrics = response.json()
            print(f"Found {len(metrics)} metrics")
            if len(metrics) > 10:
                print(f"First 10 metrics: {metrics[:10]}")
            else:
                print(f"Metrics: {metrics}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 7: Get MSFT quarterly income data for 2020 Q2
        print("\n=== Test 7: MSFT Q2 2020 Income Data ===")
        params = {
            "metric_type": "income",
            "year": 2020,
            "quarter": "Q2"
        }
        response = requests.get(f"{base_url}/financial/MSFT", params=params)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} records")
            if data:
                # Display key financial metrics
                record = data[0]
                print(f"Symbol: {record.get('symbol')}")
                print(f"Date: {record.get('date')}")
                print(f"Period: {record.get('period')}")
                
                # Get metrics from the values
                metric_values = record.get('metric_values', {})
                print("\nKey Financial Metrics:")
                print(f"Revenue: ${metric_values.get('revenue', 'N/A')}")
                print(f"Net Income: ${metric_values.get('netincome', 'N/A')}")
                print(f"EBITDA: ${metric_values.get('ebitda', 'N/A')}")
                print(f"EPS: ${metric_values.get('eps', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 8: Get all MSFT income data for 2020
        print("\n=== Test 8: All MSFT 2020 Income Data ===")
        params = {
            "metric_type": "income",
            "year": 2020
        }
        response = requests.get(f"{base_url}/financial/MSFT", params=params)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} records")
            for i, record in enumerate(data):
                print(f"\nRecord {i+1}:")
                print(f"Date: {record.get('date')}")
                print(f"Period: {record.get('period')}")
                metric_values = record.get('metric_values', {})
                print(f"Revenue: ${metric_values.get('revenue', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
        
        # Test 9: Get MSFT time series data with explicit date range
        print("\n=== Test 9: MSFT Time Series 2020-2021 ===")
        params = {
            "metrics": "revenue,netincome,ebitda",
            "start_year": 2019,  # Try from 2019 instead
            "end_year": 2021,
            "period": "quarter"  # Try "quarter" instead of "Q"
        }
        response = requests.get(f"{base_url}/time-series/MSFT", params=params)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} time series points")
            if data:
                for i, point in enumerate(data[:4]):  # Show first 4 points
                    print(f"\nPoint {i+1}:")
                    print(f"Date: {point.get('date')}")
                    print(f"Period: {point.get('period')}")
                    metrics = point.get('metrics', {})
                    print(f"Revenue: ${metrics.get('revenue', 'N/A')}")
                    print(f"Net Income: ${metrics.get('netincome', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
            
        # Test 10: Get RPD quarterly data (the one we updated)
        print("\n=== Test 10: RPD Quarterly Data 2020-2022 ===")
        params = {
            "metric_type": "income",
            "year": 2020
        }
        response = requests.get(f"{base_url}/financial/RPD", params=params)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data)} records")
            for i, record in enumerate(data[:3]):  # Show first 3 records
                print(f"\nRecord {i+1}:")
                print(f"Date: {record.get('date')}")
                print(f"Period: {record.get('period')}")
                metric_values = record.get('metric_values', {})
                print(f"Revenue: ${metric_values.get('revenue', 'N/A')}")
                print(f"Net Income: ${metric_values.get('netincome', 'N/A')}")
        else:
            print(f"Error response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("Failed to connect to API server")
    except Exception as e:
        print(f"Error testing API: {e}")

def main():
    # Start the API server
    process = run_api_server()
    
    if process:
        try:
            # Test the API endpoints
            test_api_endpoints(process)
        finally:
            # Stop the API server
            print("\nStopping API server...")
            process.terminate()
            process.wait()
            print("API server stopped")

if __name__ == "__main__":
    main() 