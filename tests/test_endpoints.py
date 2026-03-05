import requests  
  
print('Testing Binance endpoints...')  
  
# Test trades endpoint  
response = requests.get('https://api.binance.com/api/v3/trades', params={'symbol': 'BTCUSDT', 'limit': 3})  
print('Trades endpoint status:', response.status_code)  
if response.status_code == 200:  
    data = response.json()  
    print('Trades data length:', len(data))  
    if data:  
        print('Sample trade keys:', list(data[0].keys()))  
else:  
    print('Trades error:', response.text[:100])  
