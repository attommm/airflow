from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine


def extract_from():
    url = "https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue"
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html')

    table = soup.find_all("table")[2]
    table.find_all("th")

    col = [tab.text.strip() for tab in table.find_all("th")]

    df = pd.DataFrame(columns = col)

    data = table.find_all("tr")
    for dat in data[1:]:
        row_data = (dat.find_all("td"))
        ind_row_data = [row_dat.text.strip() for row_dat in row_data]
        # print(ind_row_data)
        lent = len(df)
        df.loc[lent] = ind_row_data

    df["Profits(USD millions)"] = df["Profits(USD millions)"].str.replace(",","").astype(int)
    # df["Profits(USD millions)"] = df["Profits(USD millions)"].astype(int)

    return df

def load_data(ti):
    df = ti.xcom_pull(task_ids = 'extract_from')
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    df.to_sql("rich_by_price", engine, if_exists="replace", index = False)

dag = DAG(
    dag_id = "beautiful",
    start_date = datetime( 2024, 11, 12),
    schedule_interval= "@daily",
    catchup = False
)

extract_from = PythonOperator(
    task_id = "extract_from",
    python_callable = extract_from,
    dag = dag
)

load_data = PythonOperator(
    task_id = "load_data",
    python_callable = load_data,
    dag = dag
)

extract_from >> load_data
