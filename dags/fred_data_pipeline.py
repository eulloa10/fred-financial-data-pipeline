from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
from transformations.transform_daily_indicator import transform_daily_indicator
from transformations.transform_monthly_indicator import transform_monthly_indicator

fred_api_key = Variable.get('FRED_API_KEY')

indicators = ['DGS10', 'EFFR', 'CSUSHPINSA', 'UNRATE', 'CPIAUCSL',
              'PCE', 'JTSJOL', 'JTSHIR', 'JTSTSR', 'PSAVERT', 'CSCICP03USM665S']
indicators_all = ['DGS10', 'EFFR', 'CSUSHPINSA', 'UNRATE', 'CPIAUCSL',
                  'PCE', 'JTSJOL', 'JTSHIR', 'JTSTSR', 'PSAVERT', 'CSCICP03USM665S']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def fetch_fred_data(series_id, start_date=None, end_date=None, **kwargs):
    if end_date is None:
        end_date = datetime.now().replace(day=1) - timedelta(days=1)

    if start_date is None:
        start_date = end_date.replace(day=1)

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    print(start_date_str, end_date_str, fred_api_key)

    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={
        fred_api_key}&observation_start={start_date_str}&observation_end={end_date_str}&file_type=json'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.text
        print(f"Fetched data for {series_id} from {
              start_date_str} to {end_date_str}:")
        print(data)
        return data
    else:
        print(f"Failed to fetch data for {
              series_id}. Status Code: {response.status_code}")


dag = DAG(
    'fred_data_pipeline_dynamic',
    default_args=default_args,
    description='A dynamic pipeline to fetch data from FRED API for multiple indicators',
    schedule_interval=timedelta(days=1),
)

transformation_functions = {
    'DGS10': transform_daily_indicator,
    'EFFR': transform_daily_indicator,
    'CSUSHPINSA': transform_monthly_indicator,
    'UNRATE': transform_monthly_indicator,
    'CPIAUCSL': transform_monthly_indicator,
    'PCE': transform_monthly_indicator,
    'JTSJOL': transform_monthly_indicator,
    'JTSHIR': transform_monthly_indicator,
    'JTSTSR': transform_monthly_indicator,
    'PSAVERT': transform_monthly_indicator,
    'CSCICP03USM665S': transform_monthly_indicator
}

custom_start_date = datetime(2024, 1, 1)
custom_end_date = datetime(2024, 9, 15)

for indicator in indicators:
    fetch_task = PythonOperator(
        task_id=f'fetch_fred_data_{indicator}',
        python_callable=fetch_fred_data,
        op_kwargs={'series_id': indicator,
                   'start_date': custom_start_date, 'end_date': custom_end_date},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f'transform_data_{indicator}',
        python_callable=transformation_functions[indicator],
        op_kwargs={
            'data': "{{ task_instance.xcom_pull(task_ids='fetch_fred_data_" + indicator + "') }}",
            'indicator': indicator
        },
        dag=dag,
    )

    fetch_task >> transform_task
