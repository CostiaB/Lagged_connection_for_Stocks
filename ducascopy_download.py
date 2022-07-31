#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import requests
from lzma import LZMADecompressor, LZMAError, FORMAT_AUTO
import struct
import pandas as pd
from tqdm import tqdm



def read_day(symbol:str, day:str):
    '''
    Parameters
    ----------
    symbol : str
        Name of symbol
    day : str
        Date to download

    Returns
    -------
    decompresseddata : bytes
        decompressed bytes from bi5 file data

    '''
    decomp = LZMADecompressor(FORMAT_AUTO, None, None)
    
    url_prefix='https://datafeed.dukascopy.com/datafeed'
    file_name = 'BID_candles_min_1.bi5'
    url = f'{url_prefix}/{symbol}/{day.year:04d}/{day.month-1:02d}/{day.day:02d}/{file_name}'
    
    res = requests.get(url)
    if res.status_code == 404:
        print('download failed...')
        return -1
    else:
        rawdata = res.content
     

    if len(rawdata) :
        try:
            decompresseddata = decomp.decompress(rawdata)
        except LZMAError:
            print('decompress failed. continuing..')
    return decompresseddata


def bi5_to_df(raw_data:bytes, fmt:str):
    '''
    Parameters
    ----------
    raw_data : bytes
        DESCRIPTION.
    fmt : str
        DESCRIPTION.

    Returns
    -------
    df : pd.dataframe
        Dataframe with 'time', 'open', 'high','low', 'close', 'volume'
        columns

    '''
    chunk_size = struct.calcsize(fmt)
    chunk_count = len(raw_data)//chunk_size
    data_list = []
    for step in range(0, chunk_count):
        left = chunk_size*step 
        right = (chunk_size)*step + chunk_size
        data_list.append(struct.unpack(fmt, raw_data[left: right]))
    df = pd.DataFrame(data_list ,
                      columns=['time', 'open', 'high','low', 'close', 'volume'])
    return df

def normalize_df(df:pd.DataFrame, day:datetime):
    '''
    Parameters
    ----------
    df : pd.DataFrame
        df consist of 6 columns
        1st time is seconds from the start of current day
        2nd-5th Open, High, Low, Close prices
        6th is Volume 
    day : datetime
        Day in the datetime format

    Returns
    -------
    df : pd.dataframe
        Changes time column to day-time format
        normalize values of prices in other columns

    '''
    df.iloc[:,0] = df.iloc[:,0].apply(lambda x: day+timedelta(seconds=x))
    df.iloc[:, 1:5] = df.iloc[:, 1:5]/1000 
    return df

def download_period(symbol:str,
                  start_day:str,
                  end_day:str,
                  fmt:str):
    '''
    
    Parameters
    ----------
    symbol : str
        Name of symbol.
    start_day : str
        first day to download string format (YYYY-MM-DD)
    end_day : str
        last day to download string format (YYYY-MM-DD)
    fmt : str
        format of bytes data

    Returns
    -------
    dataset : pd.dataframe
        Dataframe with 'time', 'open', 'high','low', 'close', 'volume'
        columns for asked symbol and range of dates

    '''
    start_day = datetime.strptime(start_day, '%Y-%m-%d')
    end_day = datetime.strptime(end_day, '%Y-%m-%d')
    period = (end_day - start_day).days
    dataset = pd.DataFrame(
                     columns=['time', 'open', 'high','low', 'close', 'volume'])
    for d in tqdm(range(0, period)):
        current_day = start_day + timedelta(days=d)
        raw_day = read_day(symbol, current_day)
        if raw_day != -1:
            df = bi5_to_df(raw_day, fmt)
            df = normalize_df(df, current_day)
            dataset = pd.concat([dataset, df])
    dataset = dataset.reset_index(drop = True)
    
    return dataset

def download_data(symbols:list,
                  symbols_names:list,
                  start_day:str,
                  end_day:str,
                  fmt:str):
    '''
    

    Parameters
    ----------
    symbols : list[str]
        List of symbol names
    symbols_names : list[str]
        List of full symbols names
    start_day : str
        first day to download string format (YYYY-MM-DD).
    end_day : str
        last day to download string format (YYYY-MM-DD)
    fmt : str
        format of bytes data

    Returns
    -------
    data : pd.dataframe
        Dataframe with 'time', 'open', 'high','low', 'close', 'volume' and
        'Ticker Full Name' columns for asked symbol and range of dates

    '''

    
    data = pd.DataFrame(
                     columns=['time', 'open', 'high','low', 'close', 'volume'])
    for s, n in zip(symbols, symbols_names):
        print(f'Downloading {n} data')
        tmp = download_period(s, start_day, end_day, fmt)
        tmp.loc[:, 'Ticker Full Name'] = n
        data = pd.concat([data, tmp])
        data = data.reset_index(drop = True)
    return data
