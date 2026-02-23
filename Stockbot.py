import yfinance as yf
import time
import numpy as np
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#Function to get NQ numbers
#No borrar data[close] porque es necesario para calcular el Percent Change
def get_stock_percent(ticker):
    NQ = yf.Ticker("NQ=F")    
    data = NQ.history(period="2wk", interval="1d")
    data['Percent Change'] = (data['Close'].pct_change() * 100).round(2)
    return data[['Close', 'Percent Change']]

#Function to get the weekly percent change
def weekly_percent(data):
    return data['Percent Change'].sum().round(2)
print(f"According to the next percentage: {weekly_percent(get_stock_percent('NQ=F'))}%")

#