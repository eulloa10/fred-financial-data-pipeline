import json
import pandas as pd
import numpy as np


def transform_monthly_indicator(data, indicator, **kwargs):
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
    df_cleaned['value'] = df_cleaned['value'].round(2)
    df_cleaned['indicator_series_id'] = indicator

    return df_cleaned
