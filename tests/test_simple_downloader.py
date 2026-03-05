#!/usr/bin/env python3
"""
Simple test downloader using requests instead of aiohttp
to avoid SSL certificate issues
"""
import os
import requests
import zipfile
from pathlib import Path
from datetime import datetime, timedelta

def download_with_requests(url, destination_path):
    """Download file using requests with SSL verification disabled"""
    try:
        print(f"Downloading: {url}")
        
        # Download with SSL verification disabled (for testing)
        response = requests.get(url, verify=False, timeout=30)
        
        if response.status_code == 200:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            with open(destination_path, 'wb') as f:
                f.write(response.content)
            print(f"✅ Downloaded: {destination_path}")
            return True
        elif response.status_code == 404:
            print(f"❌ File not available (404): {url}")
            return False
        else:
            print(f"❌ HTTP Error {response.status_code}: {url}")
            return False
            
    except Exception as e:
        print(f"❌ Download failed: {url} - Error: {e}")
        return False

def extract_zip(zip_path, extract_dir):
    """Extract ZIP file and return CSV path"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Find the CSV file
        csv_files = list(extract_dir.glob("*.csv"))
        if csv_files:
            print(f"✅ Extracted: {csv_files[0]}")
            return csv_files[0]
        else:
            print(f"❌ No CSV found in {zip_path}")
            return None
            
    except Exception as e:
        print(f"❌ Extraction failed: {zip_path} - Error: {e}")
        return None

def test_binance_download():
    """Test downloading recent BTCUSDT klines data"""
    
    # Disable SSL warnings for this test
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    base_url = "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m"
    symbol = "BTCUSDT"
    
    # Try last 3 days
    success_count = 0
    for i in range(3):
        target_date = datetime.now() - timedelta(days=i+1)
        date_str = target_date.strftime("%Y-%m-%d")
        
        filename = f"{symbol}-1m-{date_str}.zip"
        url = f"{base_url}/{filename}"
        
        downloads_dir = Path("downloads") / symbol / "klines" / "1m"
        data_dir = Path("data") / symbol / "klines" / "1m"
        
        zip_path = downloads_dir / filename
        
        print(f"\n📅 Testing date: {date_str}")
        
        # Download
        if download_with_requests(url, zip_path):
            # Extract
            csv_path = extract_zip(zip_path, data_dir)
            if csv_path and csv_path.exists():
                # Check file size
                size = csv_path.stat().st_size
                print(f"📊 CSV size: {size:,} bytes")
                
                # Preview first few lines
                print("📋 First 3 lines:")
                with open(csv_path, 'r') as f:
                    for i, line in enumerate(f):
                        if i < 3:
                            print(f"   {line.strip()}")
                        else:
                            break
                
                success_count += 1
                
                # Clean up ZIP after successful extraction (optional)
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                    print("🧹 Cleaned up ZIP file")
            else:
                print("❌ CSV extraction failed")
        else:
            print(f"❌ Download failed for {date_str}")
    
    print(f"\n🎯 Summary: {success_count}/3 days successfully downloaded")
    return success_count > 0

if __name__ == "__main__":
    print("🚀 Testing Binance data download with requests...")
    print("⚠️  SSL verification disabled for testing purposes")
    
    if test_binance_download():
        print("\n✅ Test PASSED: Script works with requests!")
        print("💡 Next step: Integrate this approach into main script")
    else:
        print("\n❌ Test FAILED: Check network or data availability")