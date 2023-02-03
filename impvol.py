from polygon import RESTClient

import datetime
import pandas as pd
import time
import numpy as np
import yfinance as yf
#from GBS import *

API_KEY =  'eCwRVeHlOHx_RCn8_aoNSKFfb6rkUw4C'
client = RESTClient(api_key=API_KEY)
ticker = 'INTC'


class Ticker():
    def __init__(self, ticker, client) -> None:
        self.ticker = ticker 
        self.client = client 
        self.chain = {}
        self.database = pd.DataFrame(columns=['close_date', 'underlying_ticker', 'underlying_close', 'contract_type', 'expiration_date', 'strike_price', 'ticker',  'price', 'IV%']) 

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

    def get_options_chain(self, exp_date):
        self.chain[exp_date] = pd.DataFrame(self.client.list_options_contracts(underlying_ticker=ticker, expiration_date=exp_date))
        return
    
    def get_options_prices(self, exp, eval_date):
        underlying_close = self.client.get_daily_open_close_agg(ticker=self.ticker, date=eval_date).close
        dfIV = self.chain[exp.strftime('%Y-%m-%d')][['contract_type', 'expiration_date', 'strike_price', 'ticker', 'underlying_ticker']].copy(deep=True)
        dfIV['price'] = np.nan
        for index, row in dfIV.iterrows():
            try:
                dfIV.loc[index, 'price'] = self.client.get_daily_open_close_agg(ticker=row['ticker'], date=eval_date).close  
            except Exception as err: 
                print(dfIV.loc['index', 'ticker'], f"no price found on {eval_date}")
        dfIV['close_date'] = eval_date
        dfIV['underlying_close'] = underlying_close
        self.database = pd.concat((self.database, dfIV))
        return
    
    #TODO: implement IV calc as test
    def calc_iv(self):
        

t = client.get_daily_open_close_agg(ticker='INTC', date='2023-02-01')
t.close
t

intc = Ticker(ticker, client)
intc.get_expirations()
intc.expirations.loc[1, 'exp'].strftime('%Y-%m-%d')
intc.get_options_chain(intc.expirations.loc[1, 'exp'].strftime('%Y-%m-%d'))
intc.chain
intc.expirations.loc[1, 'exp'].strftime('%Y-%m-%d')
intc.get_options_prices(intc.expirations.loc[1, 'exp'], '2023-02-02')
intc.database[~intc.database['price'].isnull()]



today = datetime.datetime.today().strftime('%Y-%M-%d')
response = client.list_options_contracts(
underlying_ticker=ticker, 
expiration_date='2023-01-27',
)

dfchain = pd.DataFrame(response)

op_ticker = 'O:INTC230203C00023000'
oc = client.get_daily_open_close_agg(op_ticker, '2023-01-25')
oc
data = client.get_aggs(
                ticker=op_ticker, 
                multiplier=1, 
                timespan='day', 
                from_='2023-01-20',
                to='2023-02-01'
                )
df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df
