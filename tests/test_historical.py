import requests  
print('Testing Binance historical data access...')  
  
# Test data.binance.vision  
url = 'https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/'  
response = requests.get(url)  
print('Data portal status:', response.status_code)  
if response.status_code == 200:  
    print('SUCCESS: Historical data portal accessible')  
    content = response.text  
    if '2024' in content or '2025' in content:  
        print('Recent data files available')  
else:  
    print('Portal not accessible or blocked')  
