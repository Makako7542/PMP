from datetime import datetime

import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis
from typing import List
import warnings
import pandas_datareader.data as web
from yfinance.exceptions import YFPricesMissingError

GERMAN_3M_TICKER = 'IR3TIB01DEM156N'

def get_stats(election_date: str, ticker: str, period_length: int, rf_name: str,
                   period_type: str = 'pre'):
    election_date = pd.to_datetime(election_date)
    if period_type == 'pre':
        start_date = (election_date - pd.DateOffset(months=period_length)).strftime('%Y-%m-%d')
        end_date = (election_date - pd.DateOffset(days=1)).strftime('%Y-%m-%d')
    elif period_type == 'post':
        start_date = election_date.strftime('%Y-%m-%d')
        end_date = (election_date + pd.DateOffset(months=period_length)).strftime('%Y-%m-%d')
    else:
        start_date = (election_date - pd.DateOffset(months=period_length)).strftime('%Y-%m-%d')
        end_date = (election_date + pd.DateOffset(months=period_length)).strftime('%Y-%m-%d')

    try:

        stock_data = yf.download(ticker, start=start_date, end=end_date).dropna()
        stock_data = stock_data['Close']

        if rf_name == 'german_3m':
            start_date_extended = (election_date - pd.DateOffset(months=period_length + 2)).strftime('%Y-%m-%d')
            end_date_extended = (election_date + pd.DateOffset(period_length + 2)).strftime('%Y-%m-%d')
            monthly_rf_data = web.DataReader(GERMAN_3M_TICKER, 'fred',
                                     start_date_extended, end_date_extended)
            monthly_rf_data = monthly_rf_data / 100
            rf_daily_rate = (1 + monthly_rf_data) ** (1 / 12) - 1
            rf_data = rf_daily_rate.resample('D').ffill()

        else:
            rf_data = yf.download(rf_name, start=start_date, end=end_date).dropna()
            rf_data = rf_data['Close']

        stock_returns = stock_data.pct_change().dropna()
        rf_data = rf_data.squeeze()
        rf_data = rf_data.reindex(stock_returns.index).fillna(method='ffill')
        excess_returns = stock_returns - rf_data

        avg_returns = stock_returns.mean()
        geo_avg_returns = (np.prod(1 + stock_returns)) ** (1 / len(stock_returns)) - 1
        avg_excess_returns = excess_returns.mean()
        geo_avg_excess_returns = (np.prod(1 + excess_returns)) ** (1 / len(excess_returns)) - 1
        std_excess_returns = excess_returns.std()
        sharpe_ratio = avg_excess_returns / std_excess_returns
        min_excess_returns = excess_returns.min()
        max_excess_returns = excess_returns.max()
        skew_excess_returns = skew(excess_returns)
        kurtosis_excess_returns = kurtosis(excess_returns)


        stats = {
            'avg_returns': avg_returns,
            'geo_avg_returns': geo_avg_returns,
            'avg_excess_returns': avg_excess_returns,
            'geo_avg_excess_returns': geo_avg_excess_returns,
            'std_excess_returns': std_excess_returns,
            'sharpe_ratio': sharpe_ratio,
            'min_excess_returns': min_excess_returns,
            'max_excess_returns': max_excess_returns,
            'skew_excess_returns': skew_excess_returns,
            'kurtosis_excess_returns': kurtosis_excess_returns,

        }

    except ZeroDivisionError as e:
        warnings.warn(str(e))
        stats = {key: 'No data' for key in [
            'avg_returns', 'geo_avg_returns', 'avg_excess_returns', 'geo_avg_excess_returns',
            'std_excess_returns', 'sharpe_ratio', 'min_excess_returns', 'max_excess_returns',
            'skew_excess_returns', 'kurtosis_excess_returns'
        ]}

    stats_df = pd.DataFrame([stats])
    stats_df['Index/stock name'] = ticker.lstrip('^')
    if period_type == 'pre':
        stats_df['Period type'] = 'Pre-election'
    elif period_type == 'post':
        stats_df['Period type'] = 'Post-election'
    else:
        stats_df['Period type'] = 'During election'

    stats_df['Period length'] = period_length
    print(f'Calculation finished for ticker {ticker} for election date {election_date} with period type {period_type}')

    return stats_df

def calculate_performance(election_dates: List[str], ticker_list: List[str], period_length: int = 3,
                       rf_name: str = '^german_3m'):

    results_df = pd.DataFrame()

    for ticker in ticker_list:
        for election_date in election_dates:
            for period_type in ['pre', 'post', 'during']:
                stats_df = get_stats(election_date, ticker, period_length, rf_name, period_type)
                if results_df.empty:
                    results_df = stats_df
                else:
                    results_df = pd.concat([results_df, stats_df], ignore_index=True)

    results_df.to_excel(f'Election_performance_metrics.xlsx', index=False)
    return results_df

# stats_df = get_stats('2016-11-08', '^STOXX50E', period_length=3, rf_name='german_3m',
#                      period_type='pre')

ticker_list = ['^STOXX50E', '^STOXX', 'DAX', '^FCHI', 'FTSEMIB.MI', 'IBEX', '^AEX', 'MC.PA', 'TTE.PA', 'SIE.DE',
               'SAP.DE', 'ALV.DE', 'SAN.PA', 'AIR.PA', 'ASML.AS', 'BNP.PA', 'INGA.AS', 'ENEL.MI', 'ISP.MI',
               'KER.PA', 'HEIA.AS', 'DTE.DE']

election_dates = [
    '2020-11-03',
    '2016-11-08',
    '2012-11-06',
    '2008-11-04',
    '2004-11-02',
    '2000-11-07'
]

results_df = calculate_performance(election_dates, ticker_list, period_length=3, rf_name='german_3m')


