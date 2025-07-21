# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from extract.fetch_weather import fetch_weather
# from transform.process_weather import process_weathe_data
# from load.load_to_storage import load_to_db
# import pandas as pd
#
#
# default_args = {
#     'owner': 'giorgi',
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }
#
# cities = [
#         'Tbilisi', 'Baku', 'Yerevan', 'Ankara', 'Athens',
#         'Sofia', 'Bucharest', 'Budapest', 'Vienna', 'Berlin',
#         'Paris', 'Madrid', 'Rome', 'Warsaw', 'Prague',
#         'Bratislava', 'Zagreb', 'Ljubljana', 'Belgrade', 'Skopje',
#         'Podgorica', 'Sarajevo'
#     ]
#
# def etl():
#     all_weather_data = []
#     for city in cities:
#         raw = fetch_weather(city)
#         processed = process_weathe_data(raw, city)
#         if processed:
#             all_weather_data.append(processed)
#     df = pd.DataFrame(all_weather_data)
#     load_to_db(df)
#
# with DAG(
#     dag_id='weather_etl_pipeline',
#     default_args=default_args,
#     description='ETL pipeline for weather data',
#     start_date=datetime(2024, 4, 1),
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:
#     run_etl = PythonOperator(
#         task_id='run_weather_etl',
#         python_callable=etl
#     )
# if __name__ == "__main__":
#         etl()

#######################################################################################################################


from extract.fetch_weather import fetch_weather
from transform.process_weather import process_weather_data
from load1.load_to_storage import load_to_db
from dotenv import load_dotenv
import pandas as pd

cities = [
    'Tbilisi', 'Baku', 'Yerevan', 'Ankara', 'Athens',
    'Sofia', 'Bucharest', 'Budapest', 'Vienna', 'Berlin',
    'Paris', 'Madrid', 'Rome', 'Warsaw', 'Prague',
    'Bratislava', 'Zagreb', 'Ljubljana', 'Belgrade', 'Skopje',
    'Podgorica', 'Sarajevo', 'Oslo', 'Helsinki', 'Copenhagen',
    'Tallinn', 'Riga', 'Vilnius', 'Luxembourg', 'Monaco',
    'Chisinau', 'Valletta', 'Andorra la Vella', 'Vaduz', 'San Marino',
    'Bern', 'Kyiv', 'London', 'Dublin', 'Reykjavik'
]


def etl():
    load_dotenv()
    all_weather_data = []
    for city in cities:
        raw = fetch_weather(city)
        processed = process_weather_data(raw, city)
        if processed:
            all_weather_data.append(processed)
    df = pd.DataFrame(all_weather_data)
    load_to_db(df)
    # print(all_weather_data)


if __name__ == "__main__":
    etl()
