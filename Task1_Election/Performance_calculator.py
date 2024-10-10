from datetime import datetime

import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis
from typing import List
import warnings
import pandas_datareader.data as web

GERMAN_3M_TICKER = 'IR3TIB01DEM156N'
AVERAGE_DAYS_IN_A_MONTH = 30.44

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
            end_date_extended = (election_date + pd.DateOffset(months=period_length + 2)).strftime('%Y-%m-%d')
            monthly_rf_data = web.DataReader(GERMAN_3M_TICKER, 'fred',
                                     start_date_extended, end_date_extended)
            # Convert percentage into decimal
            monthly_rf_data = monthly_rf_data / 100
            #Convert the monthly data to daily data
            rf_daily_rate = (1 + monthly_rf_data) ** (1 / AVERAGE_DAYS_IN_A_MONTH) -1
            rf_data = rf_daily_rate.resample('D').ffill()

        else:
            rf_data = yf.download(rf_name, start=start_date, end=end_date).dropna()
            rf_data = rf_data['Close']

        stock_returns = stock_data.pct_change().dropna()
        rf_data = rf_data.squeeze()
        rf_data = rf_data.reindex(stock_returns.index).fillna(method='ffill')
        excess_returns = stock_returns - rf_data

        # Annualization
        avg_returns = stock_returns.mean() * 252
        daily_geo_avg_returns = (np.prod(1 + stock_returns)) ** (1 / len(stock_returns)) - 1
        geo_avg_returns = (1 + daily_geo_avg_returns) ** 252 - 1
        avg_excess_returns = excess_returns.mean() * 252
        daily_geo_avg_excess_returns = (np.prod(1 + excess_returns)) ** (1 / len(excess_returns)) - 1
        geo_avg_excess_returns = (1 + daily_geo_avg_excess_returns) ** 252 - 1
        std_excess_returns = excess_returns.std() * np.sqrt(252)
        sharpe_ratio = avg_excess_returns / std_excess_returns
        min_excess_returns = excess_returns.min()
        max_excess_returns = excess_returns.max()
        skew_excess_returns = skew(excess_returns)
        kurtosis_excess_returns = kurtosis(excess_returns)


        stats = {
            'Annualized avg returns': avg_returns,
            'Annualized geo avg returns': geo_avg_returns,
            'Annualized avg excess returns': avg_excess_returns,
            'Annualized geo avg excess returns': geo_avg_excess_returns,
            'Annualized std excess returns': std_excess_returns,
            'Annualized Sharpe ratio': sharpe_ratio,
            'Min excess returns': min_excess_returns,
            'Max excess returns': max_excess_returns,
            'Skewness excess returns': skew_excess_returns,
            'Kurtosis excess returns': kurtosis_excess_returns,

        }

    except ZeroDivisionError as e:
        warnings.warn(str(e))
        stats = {key: 'No data' for key in [
            'Annualized avg returns', 'Annualized geo avg returns', 'Annualized avg excess returns',
            'Annualized geo avg excess returns', 'Annualized std excess returns', 'Annualized Sharpe ratio',
            'Min excess returns', 'Max excess returns', 'Skewness excess returns', 'Kurtosis excess returns'
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
    stats_df['Year'] = election_date.year
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

    results_df.to_excel('Performance_tables/Election_performance_metrics.xlsx', index=False)
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

# results_df = calculate_performance(election_dates, ticker_list, period_length=3, rf_name='german_3m')

import pandas as pd

def calculate_growth():
    file_path = 'Performance_tables/Election_performance_metrics.xlsx'

    df = pd.read_excel(file_path, sheet_name='Sheet1')

    pre_post_df = df[(df['Period type'] == 'Pre-election') | (df['Period type'] == 'Post-election')]

    pre_post_pivot = pre_post_df.pivot_table(
        index=['Index/stock name', 'Year'],
        columns='Period type',
        values=[col for col in df.columns if col not in ['Period type', 'Period length', 'Sector']],
        aggfunc='first'
    )

    pre_post_pivot = pre_post_pivot.apply(pd.to_numeric, errors='coerce')

    pre_post_pivot.fillna(0, inplace=True)

    growth_df_corrected = pre_post_pivot.apply(
        lambda x: (x.xs('Post-election', level='Period type') - x.xs('Pre-election', level='Period type')),
        axis=1
    )

    growth_df_corrected.reset_index(inplace=True)

    output_path = 'Performance_tables/Election_growth_metrics.xlsx'
    growth_df_corrected.to_excel(output_path, index=False)

    return growth_df_corrected


growth_df = calculate_growth()

