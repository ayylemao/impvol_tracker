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

def get_daily_close(ticker, eval_date):
    obj = yf.Ticker(ticker)
    res = obj.history(start=eval_date, end=pd.to_datetime(eval_date)+datetime.timedelta(days=1)).iloc[0, 3]
    return res 

def lfnc(test_date, weekday_idx=4): 
    if test_date.weekday() == 4:
        return test_date + datetime.timedelta(days=7)
    else: 
        return test_date + datetime.timedelta(days=(weekday_idx - test_date.weekday()) % 7)


class Ticker():
    def __init__(self, ticker, client, cursor) -> None:
        self.ticker = ticker 
        self.client = client 
        self.cursor = cursor
        self.dividend_yield = 0
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
        try:
            underlying_close = get_daily_close(self.ticker, eval_date) 
        except IndexError:
            return
        dfIV = self.chain[exp][['contract_type', 'expiration_date', 'strike_price', 'ticker', 'underlying_ticker']].copy(deep=True)
        calls = dfIV[dfIV['contract_type']=='call']
        puts = dfIV[dfIV['contract_type']=='put']
        call_idx = find_nearest(calls['strike_price'], underlying_close)
        put_idx = find_nearest(puts['strike_price'], underlying_close) 
         
        dfIV['price'] = np.nan
        for index, row in calls.iloc[call_idx-4:call_idx+5].iterrows():
            try:
                dfIV.loc[index, 'price'] = self.client.get_daily_open_close_agg(ticker=row['ticker'], date=eval_date).close  
            except Exception as err: 
                print(dfIV.loc[index, 'ticker'], f"no price found on {eval_date}")
        for index, row in puts.iloc[put_idx-4:put_idx+5].iterrows():
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
        try:
            div = tick.loc[str((int(datetime.date.today().strftime('%Y'))-1)), 'Dividends']
        except KeyError:
            div = 0.00
            self.dividend_yield = div
        query = f'''IF NOT EXISTS ( SELECT 1 FROM impvoltracker.option_data.div_data WHERE underlying_ticker = '{self.ticker}') THEN
        BEGIN
        INSERT INTO impvoltracker.option_data.div_data (underlying_ticker, div_yield_cash) VALUES ('{self.ticker}', {div:.3f});
        END;
        END IF'''
        #self.cursor.query(query)
        self.dividend_yield = div
        return
    
    def calc_impl_vol(self, exp_date, eval_date):
        tbill = yf.Ticker('^IRX')
        r = tbill.history(start=eval_date, end=pd.to_datetime(eval_date)+datetime.timedelta(days=1)).iloc[0, 3]/100 
        T = (pd.to_datetime(exp_date) - pd.to_datetime(eval_date)).days
        for index, row in self.database[~self.database['price'].isnull()].iterrows():
            try:
                #print('debug:')
                #print('Option Type:', row['contract_type'][0])
                #print('underlying_close:', row['underlying_close'])
                #print('strike price:', row['strike_price'])
                #print('DTE:', T, T/365)
                #print('r:', r)
                #print('q:', np.log(1+self.dividend_yield/row['underlying_close']))
                #print('cp:', row['price'])
                impvol = amer_implied_vol(
                                            option_type=row['contract_type'][0],
                                            fs=row['underlying_close'],
                                            x=row['strike_price'],
                                            t=T/365,
                                            r=r,
                                            q=np.log(1+self.dividend_yield/row['underlying_close']),
                                            cp=row['price']
                                            )
                self.database.loc[index, 'IV'] = impvol
            except Exception as err:
                impvol = np.nan
                self.database.loc[index, 'IV'] = impvol
                print(err)


ticker = 'SPY'
intc = Ticker(ticker, client, cursor)
intc.get_div_yield()
start = '2023-02-01'
end = '2023-02-05'
dfIV = pd.DataFrame(columns=['IV'], index=pd.date_range(start=start, end=end))

dfIV

for date in dfIV.index:
    if not (date.weekday() == 5) and not (date.weekday() == 6):
        eval_date = date.strftime('%Y-%m-%d')
        try:
            exp_date = intc.get_expiration(eval_date)
            intc.get_options_chain(exp_date=exp_date)
            intc.get_options_prices(exp=exp_date, eval_date=eval_date)
            intc.calc_impl_vol(exp_date, eval_date)
            dfIV.loc[date, 'IV'] = intc.database[intc.database['close_date']==date.strftime('%Y-%m-%d')].IV.mean()
            print(intc.database[intc.database['close_date']==date.strftime('%Y-%m-%d')])

        except Exception as err:
            print(date)
            print(err)
dfIV



dfIV  
intc.database[intc.database.IV.isnull()]

tick = yf.Ticker(ticker)
df = tick.history(start=start, end=end)
df = df.tz_localize(None)
dfmerged = pd.merge(left=dfIV, right=df['Close'], left_index=True, right_index=True)

dfmerged.reset_index(inplace=True)

dfmerged['exp_date'] = dfmerged['index'].apply(lambda x: lfnc(x))
dfmerged['DTE'] = dfmerged.apply(lambda x: (x['exp_date'] - x['index']).days, axis=1)
dfmerged['DTE'] = dfmerged['DTE'].apply(lambda x : 7 if (x == 0) else x)
dfmerged['actual_move%'] = (dfmerged.Close - dfmerged.Close.shift(1))/dfmerged.Close.shift(1)
dfmerged['actual_move'] = (dfmerged.Close - dfmerged.Close.shift(1))
dfmerged['implied_move'] = dfmerged.Close*(dfmerged.IV.shift(1) * np.sqrt(1/365))
dfmerged['implied_move%'] = dfmerged.IV.shift(1) * np.sqrt(1/365) 
dfmerged['direction'] = dfmerged.actual_move.apply(lambda x: True if (x>=0) else False)
dfmerged

import matplotlib.pyplot as plt

fig, ax = plt.subplots(1,1)

ax.bar(dfmerged['index'], dfmerged.actual_move, color=dfmerged.direction.map({True: 'g', False: 'r'}))
ax.errorbar(dfmerged['index'][1:], np.zeros(dfmerged.shape[0]-1), yerr=dfmerged.implied_move.shift(1)[1:], capsize=2, c='black')
ax.tick_params(axis='x', rotation=25)
fig.tight_layout()
fig.savefig(f'{ticker}.png')
dfIV
0.323669*np.sqrt(1/365)

df = intc.database
df[df['close_date']=='2023-01-26']
from GBS import *
amer_implied_vol(option_type='c', fs=30.09, x=29, t=0.8/365, r=0.04543, q=0.047, cp=0.9)
df

tbill = yf.Ticker('^IRX')
r = tbill.history(start='2023-01-26')
r