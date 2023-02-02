from polygon import RESTClient
import datetime
import pandas as pd
import time
import numpy as np
import yfinance as yf
from GBS import *

API_KEY =  'eCwRVeHlOHx_RCn8_aoNSKFfb6rkUw4C'
client = RESTClient(api_key=API_KEY)
ticker = 'INTC'


class Ticker():
    def __init__(self, ticker, client) -> None:
        self.ticker = ticker 
        self.client = client 
     
    def get_underlying_close(self, date):
        try:
            date = date.strftime('%Y-%m-%d')
        except AttributeError as err:
            print(err)
        try:  
            return self.client.get_daily_open_close_agg(ticker=ticker, date=date)[0].close
        except Exception as err:
            print(err)

    def get_expirations(self):
        self.expirations = pd.DataFrame(columns=['exp'])
        self.expirations['exp'] = yf.Ticker(self.ticker).options
        self.expirations['exp'] = pd.to_datetime(self.expirations['exp'])
        return
    


#client.list_options_contracts(underlying_ticker=ticker, contract_type='call', strike_price=)

'asdf'.strftime()
intc = Ticker(ticker, client)
intc.get_expirations()
intc.expirations

today = datetime.datetime.today().strftime('%Y-%M-%d')
response = client.list_options_contracts(
underlying_ticker=ticker, 
expiration_date='2023-01-27',
)

dfchain = pd.DataFrame(response)

op_ticker = 'O:INTC230127C00032000'
oc = client.get_daily_open_close_agg(op_ticker, '2023-01-25')
oc
data = client.get_aggs(
                ticker=op_ticker, 
                multiplier=1, 
                timespan='day', 
                from_='2022-12-01',
                to='2023-01-26'
                )
df = pd.DataFrame(data)


