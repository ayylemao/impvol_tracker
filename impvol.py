from polygon import RESTClient
from google.cloud import bigquery
import datetime
import pandas as pd
import time
import numpy as np
import yfinance as yf
from GBS import *

API_KEY =  'eCwRVeHlOHx_RCn8_aoNSKFfb6rkUw4C'
client = RESTClient(api_key=API_KEY)
cursor = bigquery.Client()


def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx

def lfnc(test_date, weekday_idx=4): return test_date + \
    datetime.timedelta(days=(weekday_idx - test_date.weekday() + 7) % 7)

class Ticker():
    def __init__(self, ticker, client, cursor) -> None:
        self.ticker = ticker 
        self.client = client 
        self.cursor = cursor
        self.chain = {}
        self.database = pd.DataFrame(columns=['close_date', 'underlying_ticker', 'underlying_close', 'contract_type', 'expiration_date', 'strike_price', 'ticker',  'price', 'IV']) 

    def get_underlying_close(self, date):
        try:
            date = date.strftime('%Y-%m-%d')
        except AttributeError as err:
            print(err)
        try:  
            return self.client.get_daily_open_close_agg(ticker=self.ticker, date=date)[0].close
        except Exception as err:
            print(err)

    def get_expiration(self, eval_date):
        eval_date = pd.to_datetime(eval_date) 
        return lfnc(eval_date).strftime('%Y-%m-%d')

    def get_options_chain(self, exp_date):
        if pd.to_datetime(exp_date) >= datetime.datetime.today():
            expired = False
        else:
            expired = True
        self.chain[exp_date] = pd.DataFrame(self.client.list_options_contracts(underlying_ticker=self.ticker, expiration_date=exp_date, expired=expired))
        return
    
    def get_options_prices(self, exp, eval_date):
        underlying_close = self.client.get_daily_open_close_agg(ticker=self.ticker, date=eval_date).close
        dfIV = self.chain[exp][['contract_type', 'expiration_date', 'strike_price', 'ticker', 'underlying_ticker']].copy(deep=True)
        calls = dfIV[dfIV['contract_type']=='call']
        puts = dfIV[dfIV['contract_type']=='put']
        call_idx = find_nearest(calls['strike_price'], underlying_close)
        put_idx = find_nearest(puts['strike_price'], underlying_close) 
         
        dfIV['price'] = np.nan
        for index, row in calls.iloc[call_idx-2:call_idx+3].iterrows():
            try:
                dfIV.loc[index, 'price'] = self.client.get_daily_open_close_agg(ticker=row['ticker'], date=eval_date).close  
            except Exception as err: 
                print(dfIV.loc[index, 'ticker'], f"no price found on {eval_date}")
        for index, row in puts.iloc[put_idx-2:put_idx+3].iterrows():
            try:
                dfIV.loc[index, 'price'] = self.client.get_daily_open_close_agg(ticker=row['ticker'], date=eval_date).close  
            except Exception as err: 
                print(dfIV.loc[index, 'ticker'], f"no price found on {eval_date}")
        dfIV['close_date'] = eval_date
        dfIV['underlying_close'] = underlying_close
        self.database = pd.concat((self.database, dfIV[~dfIV['price'].isnull()]))
        return

    def get_div_yield(self):
        tick = yf.Ticker(self.ticker)
        tick = pd.DataFrame(tick.dividends)
        tick.reset_index(inplace=True)
        tick['y'] = tick.Date.apply(lambda x: x.strftime('%Y'))
        tick = tick.groupby(['y']).agg({'Dividends':np.nansum})
        div = tick.loc[str((int(datetime.date.today().strftime('%Y'))-1)), 'Dividends']
        query = f'''IF NOT EXISTS ( SELECT 1 FROM impvoltracker.option_data.div_data WHERE underlying_ticker = '{self.ticker}') THEN
        BEGIN
        INSERT INTO impvoltracker.option_data.div_data (underlying_ticker, div_yield_cash) VALUES ('{self.ticker}', {div:.3f});
        END;
        END IF'''
        self.cursor.query(query)
        self.dividend_yield = div
        return

    def calc_impl_vol(self, exp_date, eval_date):
        tbill = yf.Ticker('^IRX')
        r = tbill.history(start=eval_date, end=pd.to_datetime(eval_date)+datetime.timedelta(days=1)).iloc[0, 3]/100 
        T = (pd.to_datetime(exp_date) - pd.to_datetime(eval_date)).days / 365
        for index, row in self.database[~self.database['price'].isnull()].iterrows():
            try:
                impvol = amer_implied_vol(
                                            option_type=row['contract_type'][0],
                                            fs=row['underlying_close'],
                                            x=row['strike_price'],
                                            t=T,
                                            r=r,
                                            q=np.log(1+self.dividend_yield/row['underlying_close']),
                                            cp=row['price']
                                            )
                self.database.loc[index, 'IV'] = impvol
            except Exception as err:
                print(err)

eval_date = '2023-01-03'
intc = Ticker('AAPL', client, cursor)
intc.get_div_yield()

dfIV = pd.DataFrame(columns=['IV'], index=pd.date_range(start='2023-01-01', end='2023-01-31'))

for date in dfIV.index:
    eval_date = date.strftime('%Y-%m-%d')
    try:
        exp_date = intc.get_expiration(eval_date)
        intc.get_options_chain(exp_date=exp_date)
        intc.get_options_prices(exp=exp_date, eval_date=eval_date)
        intc.calc_impl_vol(exp_date, eval_date)
        dfIV.loc[date, 'IV'] = intc.database.IV.mean()
    except Exception as err:
        print(date)
        print(err)

for i in range(0, 1000):
    intc.get_options_prices(exp=exp_date, eval_date=eval_date)
    print(i)