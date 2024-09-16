import pandas as pd
import numpy as np
import json


def transform_daily_indicator(data, indicator, **kwargs):
    data = json.loads(data)
    observations = data.get('observations', [])
    df = pd.DataFrame(observations)
    df.rename(columns={'date': 'observation_date', 'realtime_start': 'fetch_date_start',
              'realtime_end': 'fetch_date_end'}, inplace=True)
    df['observation_date'] = pd.to_datetime(df['observation_date'])
    df['fetch_date_start'] = pd.to_datetime(df['fetch_date_start'])
    df['fetch_date_end'] = pd.to_datetime(df['fetch_date_end'])

    df.replace('.', np.nan, inplace=True)
    df_cleaned = df.dropna()

    non_date_columns = df_cleaned.columns.difference(
        ['observation_date', 'fetch_date_start', 'fetch_date_end'])
    df_cleaned[non_date_columns] = df_cleaned[non_date_columns].apply(
        pd.to_numeric, errors='coerce')
    df_cleaned['year_month'] = df_cleaned['observation_date'].dt.to_period('M')
    df_monthly_avg = df_cleaned.groupby('year_month').mean().reset_index()
    df_monthly_avg['observation_date'] = df_monthly_avg['year_month'].dt.strftime(
        '%Y-%m-01')
    df_monthly_avg.drop(columns=['year_month'], inplace=True)
    df_monthly_avg['value'] = df_monthly_avg['value'].round(2)
    df_monthly_avg['indicator_series_id'] = indicator

    return df_monthly_avg
