import yfinance as yf
import time
import numpy as np
import pandas as pd
#Function to get NQ numbers
def get_stock_percent(ticker):
    NQ = yf.Ticker("NQ=F")    
    data = NQ.history(period="2wk", interval="1d")
    data['Percent Change'] = data['Close'].pct_change() * 100
    return data[['Close', 'Percent Change']]
results = get_stock_percent("NQ=F")
print(results)

#Creating a message to send to the user
