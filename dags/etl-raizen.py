from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

import logging
import urllib
import os 
import pandas as pd 
from subprocess import  Popen
import psycopg2 as pg

## Download XLS file from ANP

def download_xls():
        import urllib
        logging.info('Starting XLS download from ANP ... ')
        url = 'http://www.anp.gov.br/arquivos/dados-estatisticos/vendas-combustiveis/vendas-combustiveis-m3.xls'
        response = urllib.request.urlretrieve(url, 'vendas-combustiveis-m3.xls')        

# Use LibreOffice to convert file to XLS to view pivot table cached

def converter_xls():
    p = Popen(['libreoffice', '--headless', '--convert-to', 'xls', '--outdir',
               'raw_data', 'vendas-combustiveis-m3.xls'])
    print(['libreoffice', '--convert-to', 'ods', './raw_data/vendas-combustiveis-m3.xls'])
    p.communicate()
    
    
def transform(sheetName, tableName):
    os.popen('mkdir staging')    
    df = pd.read_excel('./raw_data/vendas-combustiveis-m3.xls', sheet_name=sheetName)
    df.columns = ['Combustível', 'Ano', 'Região', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Total']
    df = df.melt(id_vars=['Combustível', 'Ano', 'Região', 'UF'])
    df = df.loc[df['variable'] != 'Total']
    df['year_month'] = df['Ano'].astype(str) + '-' + df['variable']
    df['year_month'] = pd.to_datetime(df['year_month'])
    df = df.drop(labels=['variable', 'Região', 'Ano'], axis=1)
    df.columns = ['product', 'uf', 'volume', 'year_month']
    df['volume'] = pd.to_numeric(df['volume'])
    df = df.fillna(0)
    df['unit'] = 'm3'
    df.to_csv('./staging/' + tableName + '.csv')
    
dag = DAG('etl-raizen', description='ETL process to extract internal pivot caches from consolidated reports ANP',
          start_date=datetime(2020, 7, 1), catchup=False)


def sqlLoad(tableName):
    """
    we make the connection to postgres using the psycopg2 library, create
    the schema to hold our covid data, and insert from the local csv file
    """
    
    # attempt the connection to postgres
    try:
        dbconnect = pg.connect(
            database='airflow',
            user='airflow',
            password='airflow',
            host='postgres'
        )
    except Exception as error:
        print(error)
    
    # create the table if it does not already exist
    cursor = dbconnect.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            id integer PRIMARY KEY,
            product VARCHAR(256),
            uf VARCHAR(256),
            volume FLOAT,
            year_month date,
            unit VARCHAR(256)
        );
        
        TRUNCATE TABLE {tableName};
    """
    )
    dbconnect.commit()
    
    # insert each csv row as a record in our database
    with open(f"./staging/{tableName}.csv") as f:
        next(f) # skip the first row (header)
        for row in f:
            cursor.execute("""
                INSERT INTO {}
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}')
            """.format(
                    tableName,
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3],
            row.split(",")[4],
            row.split(",")[5])
            )
    dbconnect.commit()


download_anp = PythonOperator(
        task_id='download_anp', 
        python_callable=download_xls,
        dag=dag)

converter_xls  = PythonOperator(
        task_id='converter_xls', 
        python_callable=converter_xls,
        dag=dag)

extract_derivated_fuels  = PythonOperator(
        task_id='extract_derivated_fuels', 
        python_callable=transform,
        op_kwargs={'sheetName': 1, 'tableName': 'derivated_fuels'},
        dag=dag)


extract_diesel  = PythonOperator(
        task_id='extract_diesel', 
        python_callable=transform,
        op_kwargs={'sheetName': 2, 'tableName': 'diesel'},
        dag=dag)


loadDieselToSql = PythonOperator(
        task_id="loadDieselToSql",
        python_callable=sqlLoad,
        op_kwargs={'tableName': 'diesel'}
    )

loadDerivatedFuelToSQL = PythonOperator(
        task_id="loadDerivatedFuelToSql",
        python_callable=sqlLoad,
        op_kwargs={'tableName': 'derivated_fuels'}
    )
    
    
    
download_anp >> converter_xls >> [extract_derivated_fuels, extract_diesel] 
extract_diesel >> loadDieselToSql
extract_derivated_fuels >> loadDerivatedFuelToSQL