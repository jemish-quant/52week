import numpy as np
import pandas as pd
import datetime as dt
#import yfinance as yf
#import matplotlib.pyplot as plt
#import talib as ta
import time
import requests
import gzip
import json
import streamlit as st

################ Load F&O List ############################
url = "https://script.google.com/macros/s/AKfycbxBe04lgpJCIFYMa76mP6mvYJdztTWprMKYwrtz8SJ5iZNMhvZ5jq5pXF2pev0jvIqoYw/exec"   # Your Web App URL

response = requests.get(url)
data = response.json()
df1=pd.DataFrame(data['data'])

#st.dataframe(df1)
##############################################################
###################################
# URL of the file
url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

# Step 1: Download the file
response = requests.get(url)
compressed_file_path = "complete.json.gz"

# Write the content to a file
with open(compressed_file_path, 'wb') as f:
    f.write(response.content)

# Step 2: Extract and load the JSON data
with gzip.open(compressed_file_path, 'rt') as f:
    data1 = json.load(f)

######################### All Functions ######################

def RSI(prices, period: int = 14):
    """
    Calculates the Relative Strength Index (RSI) for a given pandas Series of prices.

    Args:
        prices: A pandas Series containing historical closing prices.
        period: The number of periods for the RSI calculation (default: 14).

    Returns:
        A pandas Series containing the RSI values.
    """

    # 1. Calculate the price difference (delta)
    delta = prices.diff()
    # Remove NaN from the first difference
    delta = delta.dropna()

    # 2. Separate gains (up) and losses (down)
    up = delta.copy()
    down = delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    # Make losses positive for calculation
    down = down.abs()

    # 3. Calculate the Exponential Moving Average (EMA) for gains and losses
    # The first 'period' values use a simple average, then a smoothed average (EMA) for subsequent values.
    # pandas' .ewm function with com=(period-1) performs the correct smoothed average calculation for Wilder's RSI.
    avg_gain = up.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = down.ewm(com=period - 1, min_periods=period).mean()

    # 4. Calculate the Relative Strength (RS)
    rs = avg_gain / avg_loss

    # 5. Calculate the RSI
    rsi = 100 - (100 / (1 + rs))

    return rsi


def upstox_df(Instrument_data,df1):
    matched_df = df1.merge(Instrument_data, on='ISIN', how='left')
    matched_df['priority']=matched_df['segment'].map({'NSE_EQ':1,'BSE_EQ':2}).fillna(3)
    preferred_indices = matched_df.groupby('ISIN')['priority'].idxmin()
    df101=matched_df.loc[preferred_indices]
    df101=df101.sort_index()
    matched_df=df101.copy()
    return matched_df


def stock_data(instrument_key,interval,interval_option,end_date,start_date,intra_day=True,intra_day_interval='days',intra_day_unit='1'):
  url = 'https://api.upstox.com/v3/historical-candle/{0}/{1}/{2}/{3}/{4}'.format(instrument_key,interval,interval_option,end_date,start_date)
  headers = {
        'Accept': 'application/json'
        }
  response = requests.get(url, headers=headers)
  if response.status_code == 200:
    data=response.json()
    candle_data=data['data']['candles']
    columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
    df=pd.DataFrame(candle_data, columns=columns)
    df=df[::-1]
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce',utc=False)
    df.set_index('Date', inplace=True)
  else:
      print(f"Error: {response.status_code} - {response.text}")

  if intra_day==True:
    url1 = 'https://api.upstox.com/v3/historical-candle/intraday/{0}/days/1'.format(instrument_key,intra_day_interval,intra_day_unit)
    payload={}
    headers = {
              'Accept': 'application/json'
            }

    response1 = requests.request("GET", url1, headers=headers, data=payload)
    if response1.status_code ==200:
          # Do something with the response data (e.g., print it)
          ######## DataFrame for Intraday Data #########
          data_intra=response1.json()
          data_intraday=data_intra['data']['candles']
          columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
          df_intraday=pd.DataFrame(data_intraday, columns=columns)
          df_intraday=df_intraday[::-1]
          df_intraday['Date'] = pd.to_datetime(df_intraday['Date'])
          df_new_intra=pd.DataFrame([dict(df_intraday.iloc[-1])])
          df_new_intra.set_index('Date', inplace=True)
          df=pd.concat([df, df_new_intra])
    else:
      print(f"Error: {response.status_code} - {response.text}")
  return df

import math
def rounding_up(x):
    if x=='Manual Check':
        z='Manual Check'
    if x>=0 and x<1:
        z=x
    elif x<100:
        z=math.ceil(x)
    elif x>=100 and x<1000:
        y=math.ceil(x/10)
        z=y*10
    elif x>=1000 and x<10000:
        y=math.ceil(x/100)
        z=y*100
    elif x>=10000 and x<100000:
        y=math.ceil(x/100)
        z=y*1000
    else:
        y=math.ceil(x/10000)
        z=y*10000
    return z
def rounding_down(x):
    if x=='Manual Check':
        z='Manual Check'
    if x>=0 and x<1:
        z=x
    elif x<100:
        z=math.floor(x)
    elif x>=100 and x<1000:
        y=math.floor(x/10)
        z=y*10
    elif x>=1000 and x<10000:
        y=math.floor(x/100)
        z=y*100
    elif x>=10000 and x<100000:
        y=math.floor(x/1000)
        z=y*1000
    else:
        y=math.floor(x/10000)
        z=y*10000
    return z

#################### matched with upstox data #####################
Instrument_data=pd.DataFrame(data1)
Instrument_data=Instrument_data.rename(columns={'isin':'ISIN'})
matched_df=upstox_df(Instrument_data,df1)
#print(matched_df)
#st.dataframe(matched_df)

################ Time ############################################
interval='weeks'  #1minute,30minute,week,day,month
start_date=(dt.date.today()-dt.timedelta(365*25)).strftime("%Y-%m-%d")
end_date=(dt.date.today()).strftime("%Y-%m-%d")



long1=[]
short1=[]
long_ISIN=[]
long_symbol=[]
long_high=[]
long_low_sl=[]
long_qty=[]
short_ISIN=[]
short_symbol=[]
short_low=[]
short_high_sl=[]
short_qty=[]
sym=[]
lot_size=[]
for i,ticker in enumerate(matched_df['Code']):
    time.sleep(0.5)
    print(str(i+1)+" : Downloading.....",ticker)
    try:
      df=stock_data(matched_df['instrument_key'].iloc[i],interval,'1',end_date,start_date)
      #print(df)
      #print(df)
      if df['High'].iloc[-53:-1].max()<=df['High'].iloc[-1] :
        print("*"*10+'\n'+'Long :'+matched_df['Code'].iloc[i]+'\n'+'*'*10)
        long1.append(matched_df['Code'].iloc[i])
        if df['Close'].iloc[-1]>df['Open'].iloc[-1] and df['High'].iloc[:-53].max()>df['High'].iloc[-1]:
          long_ISIN.append(matched_df['ISIN'].iloc[i])
          long_symbol.append(matched_df['Code'].iloc[i])
          high_new=round(df['High'].iloc[-1],2)
          low_2wk=round(df['Low'].iloc[-3:-1].min(),2)
          long_high.append(high_new)
          long_low_sl.append(low_2wk)
          long_qty.append(int(300000/(high_new*1.02-low_2wk)))
      if df['Low'].iloc[-53:-1].min()>=df['Low'].iloc[-1]:
        print("*"*10+'\n'+'Short :'+matched_df['Code'].iloc[i]+'\n'+'*'*10)
        short1.append(matched_df['Code'].iloc[i])
        if df['Close'].iloc[-1]<df['Open'].iloc[-1]:
          short_ISIN.append(matched_df['ISIN'].iloc[i])
          short_symbol.append(matched_df['Code'].iloc[i])
          short_low1=round(df['Low'].iloc[-1],2)
          short_high_sl1=round(df['High'].iloc[-3:-1].max(),2)
          short_low.append(short_low1)
          short_high_sl.append(short_high_sl1)
          short_qty.append(int(300000/abs(short_high_sl1*1.02-short_low1)))
    except:
      print('Data Not found')
####################################

#print('Long :',long1)
#print('Short :',short1)
#######################################
df22=pd.DataFrame(list(zip(long_ISIN,long_symbol)),columns=['ISIN','Symbol'])
matched_df1=upstox_df(Instrument_data,df22)
interval1='days'  #1minute,30minute,week,day,month
start_date1=(dt.date.today()-dt.timedelta(365*2)).strftime("%Y-%m-%d")
end_date1=(dt.date.today()).strftime("%Y-%m-%d")
df_index=stock_data('NSE_INDEX|Nifty 50',interval1,'1',end_date1,start_date1)

df22['Range %']=0.0
df22['RSI']=0.0
df22['Days']=0
df22['Closing']=0
df22['Rng_Score <4%']=0
df22['RS']=0
df22['Volume_Score 2x']=0
df22['RSI_Score >70']=0
df22['Total']=0
for i,ticker in enumerate(matched_df1['Symbol']):
    time.sleep(0.5)
    print(str(i+1)+" : Downloading.....",ticker)
    try:
      df=stock_data(matched_df1['instrument_key'].iloc[i],interval1,'1',end_date1,start_date1)
      df['RS']=df['Close']/df_index['Close']
      #print(df)
      previous_high=df['High'].iloc[-252:-1].max()
      today_close=df['Close'].iloc[-1]
      round_high=rounding_up(df['High'].iloc[-1])
      previous_high_date=df['High'].iloc[-252:-1].idxmax()
      today_date=df.index[-1]
      df22.loc[i,'Days']=(today_date-previous_high_date).days
      if previous_high<=today_close :
        df22.loc[i,'Closing']=1

      df22.loc[i,'Range %']=round((round_high/previous_high-1)*100,2)
      if df22['Range %'].iloc[i]<4:
        df22.loc[i,'Rng_Score <4%']=1
      if df['RS'].iloc[-252:-1].max()<=df['RS'].iloc[-1]:
        df22.loc[i,'RS']=1
      if df['Volume'].iloc[-10:-1].mean()*2<=df['Volume'].iloc[-1]:
        df22.loc[i,'Volume_Score 2x']=1

      df22.loc[i,'RSI']=round(RSI(df['Close']).iloc[-1],2)
      if df22['RSI'].iloc[i]>70:
        df22.loc[i,'RSI_Score >70']=1
      df22.loc[i,'Total']=df22.loc[i,'Closing']+df22.loc[i,'Rng_Score <4%']+df22.loc[i,'RS']+df22.loc[i,'Volume_Score 2x']+df22.loc[i,'RSI_Score >70']


    except:
      print('Data Not found')

df22.sort_values('Total',ascending=False,inplace=True)
#print(df22)

###################################
df2=pd.DataFrame(list(zip(long_symbol,long_high,long_low_sl,long_qty)),columns=['Ticker','High','StopLoss','Qty'])
df2['Lots']=0.0
df2['Lot Size']=0.0
for i,ticker in enumerate(df2['Ticker']):
  df11=Instrument_data[(Instrument_data['underlying_symbol']==ticker) & (Instrument_data['instrument_type']=='FUT')]
  if df11.empty:
    lots=0
    df2.loc[df2['Ticker']==ticker,'Lots']=0
  else:
    lots=df11['lot_size'].iloc[0]
    df2.loc[df2['Ticker']==ticker,'Lots']=df2['Qty'].iloc[i]/lots
  #lots=df11['lot_size'].iloc[0]
  #print(df2['Qty'].iloc[i])
  #print(lots)
  df2.loc[df2['Ticker']==ticker,'Lots']=df2['Qty'].iloc[i]/lots
  df2.loc[df2['Ticker']==ticker,'Lot Size']=int(lots)

#print("*"*10+' Long Position Prospect '+"*"*10)
#print(df2)
#st.dataframe(df2)

df33=pd.DataFrame(list(zip(short_ISIN,short_symbol)),columns=['ISIN','Symbol'])
matched_df2=upstox_df(Instrument_data,df33)
df33['Range %']=0.0
df33['RSI']=0.0
df33['Days']=0
df33['Closing']=0
df33['Rng_Score >-4%']=0
df33['RS']=0
df33['Volume_Score 2x']=0
df33['RSI_Score <30']=0
df33['Total']=0
for i,ticker in enumerate(matched_df2['Symbol']):
    time.sleep(0.5)
    print(str(i+1)+" : Downloading.....",ticker)
    try:
      df=stock_data(matched_df1['instrument_key'].iloc[i],interval1,'1',end_date1,start_date1)
      df['RS']=df['Close']/df_index['Close']
      #print(df)
      previous_low=df['Low'].iloc[-252:-1].min()
      today_close=df['Close'].iloc[-1]
      round_low=rounding_down(df['Low'].iloc[-1])
      previous_high_date=df['Low'].iloc[-252:-1].idxmin()
      today_date=df.index[-1]
      df33.loc[i,'Days']=(today_date-previous_high_date).days
      if previous_low>=today_close :
        df33.loc[i,'Closing']=1
      df33.loc[i,'Range %']=round((round_low/previous_low-1)*100,2)
      if df33['Range %'].iloc[i]>-4:
        df33.loc[i,'Rng_Score >-4%']=1
      if df['RS'].iloc[-252:-1].min()>=df['RS'].iloc[-1]:
        df33.loc[i,'RS']=1
      if df['Volume'].iloc[-10:-1].mean()*2<=df['Volume'].iloc[-1]:
        df33.loc[i,'Volume_Score 2x']=1

      df33.loc[i,'RSI']=round(RSI(df['Close']).iloc[-1],2)
      if df33['RSI'].iloc[i]<30:
        df33.loc[i,'RSI_Score <30']=1
      df33.loc[i,'Total']=df33.loc[i,'Closing']+df33.loc[i,'Rng_Score >-4%']+df33.loc[i,'RS']+df33.loc[i,'Volume_Score 2x']+df33.loc[i,'RSI_Score <30']

    except:
      print('Data Not found')
df33.sort_values('Total',ascending=False,inplace=True)
#print(" Short position scoring...")
#print(df33)

df3=pd.DataFrame(list(zip(short_symbol,short_low,short_high_sl,short_qty)),columns=['Ticker','Low','StopLoss','Qty'])
df3['Lots']=0.0
df3['Lot Size']=0.0
for i,ticker in enumerate(df3['Ticker']):
  df11=Instrument_data[(Instrument_data['underlying_symbol']==ticker) & (Instrument_data['instrument_type']=='FUT')]
  if df11.empty:
    lots=0
    df3.loc[df3['Ticker']==ticker,'Lots']=0
  else:
    lots=df11['lot_size'].iloc[0]
    df3.loc[df3['Ticker']==ticker,'Lots']=df3['Qty'].iloc[i]/lots
  #print(df3['Qty'].iloc[i])
  #print(lots)
  df3.loc[df3['Ticker']==ticker,'Lot Size']=int(lots)
  #print(ticker)
#print("*"*10+' Short Position Prospect '+"*"*10)
#print(df3)

st.set_page_config(page_title="52 Week High & Low",layout="wide")
st.title("52 Week High & Low Screener")
st.write("Long Position Scoring")
st.dataframe(df22)
st.write("Long Position Prospect list")
st.dataframe(df2)
st.write("Short Position Scoring")
st.dataframe(df33)
st.write("Short Position Prospect list")
st.dataframe(df3)

