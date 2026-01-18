#!/usr/bin/env python3
"""Import Grafana dashboard via API."""

import json
import os
import sys
import httpx

def import_dashboard(dashboard_file: str, grafana_url: str = None, api_key: str = None, username: str = None, password: str = None):
    """Import dashboard to Grafana via API."""
    
    # Default Grafana URL (can be overridden with env vars)
    grafana_url = grafana_url or os.getenv('GRAFANA_URL', 'http://localhost:3000')
    api_key = api_key or os.getenv('GRAFANA_API_KEY', '')
    username = username or os.getenv('GRAFANA_USERNAME', 'admin')
    password = password or os.getenv('GRAFANA_PASSWORD', '')
    
    # Determine authentication method
    auth_header = None
    if api_key:
        auth_header = f"Bearer {api_key}"
    elif username and password:
        import base64
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        auth_header = f"Basic {credentials}"
    else:
        print("⚠️  Authentication required!")
        print("   Options:")
        print("   1. Set GRAFANA_USERNAME and GRAFANA_PASSWORD env vars")
        print("   2. Set GRAFANA_API_KEY env var")
        print("   3. Pass --username and --password arguments")
        return False
    
    # Load dashboard JSON
    try:
        with open(dashboard_file, 'r') as f:
            dashboard_data = json.load(f)
    except Exception as e:
        print(f"❌ Error reading dashboard file: {e}")
        return False
    
    # Prepare API request
    url = f"{grafana_url.rstrip('/')}/api/dashboards/db"
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json"
    }
    
    # Grafana API expects dashboard in 'dashboard' field
    payload = {
        "dashboard": dashboard_data.get("dashboard", dashboard_data),
        "overwrite": dashboard_data.get("overwrite", True)
    }
    
    print(f"📤 Importing dashboard to Grafana...")
    print(f"   URL: {grafana_url}")
    print(f"   File: {dashboard_file}")
    print(f"   Dashboard: {payload['dashboard'].get('title', 'Unknown')}\n")
    
    try:
        response = httpx.post(url, json=payload, headers=headers, timeout=30.0)
        response.raise_for_status()
        
        result = response.json()
        dashboard_url = result.get('url', '')
        
        print(f"✅ Dashboard imported successfully!")
        print(f"   Title: {result.get('title', 'Unknown')}")
        print(f"   UID: {result.get('uid', 'Unknown')}")
        if dashboard_url:
            full_url = f"{grafana_url.rstrip('/')}{dashboard_url}"
            print(f"   URL: {full_url}")
        
        return True
        
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP Error: {e.response.status_code}")
        print(f"   Response: {e.response.text[:200]}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Grafana dashboard via API')
    parser.add_argument('dashboard', nargs='?', default='grafana_hft_microstructure.json',
                       help='Dashboard JSON file to import')
    parser.add_argument('--url', default=None,
                       help='Grafana URL (default: http://localhost:3000 or GRAFANA_URL env)')
    parser.add_argument('--api-key', default=None,
                       help='Grafana API key (default: GRAFANA_API_KEY env)')
    parser.add_argument('--username', default=None,
                       help='Grafana username (default: admin or GRAFANA_USERNAME env)')
    parser.add_argument('--password', default=None,
                       help='Grafana password (default: GRAFANA_PASSWORD env)')
    
    args = parser.parse_args()
    
    success = import_dashboard(args.dashboard, args.url, args.api_key, args.username, args.password)
    sys.exit(0 if success else 1)
