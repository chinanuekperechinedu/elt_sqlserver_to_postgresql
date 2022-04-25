from dotenv import load_dotenv
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os
import psycopg2 as psg
# from sqlalchemy.dialects.postgresql import psycopg2

load_dotenv()

user = os.getenv('DB_PG_USER')
pwd = os.getenv('DB_PG_PASSWORD')
driver = 'ODBC Driver 17 for SQL Server'
# driver = "{SQL Server Native Client 18.0}"
server = os.getenv('SERVER')
ms_user = os.getenv('MS_USERNAME')
ms_pwd = os.getenv('MS_PWD')
database = 'AdventureWorks2019'
host = os.getenv('PG_HOST')


# extract data from sql server
def extract():
    try:
        print('Start')
        src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server +
                                  ';DATABASE=' + database + ';UID=' + ms_user + ';PWD=' + ms_pwd)
        print('next')
        src_cursor = src_conn.cursor()
        src_cursor.execute("""SELECT ss.name + '.' + tb.name
                                FROM sys.tables AS tb
                                JOIN sys.schemas ss ON tb.schema_id = ss.schema_id
                                WHERE tb.name IN (
                                    'Person'
                                    ,'AddressType'
                                    ) """)
        print('try')
        src_tables = src_cursor.fetchall()
        print(src_tables)
        for tbl in src_tables:
            print(tbl[0])
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * from {tbl[0]}', src_conn)

            load(df, tbl[0])


    except Exception as ex:
        print('Data extract error: ' + str(ex))
    finally:
        src_conn.close()


def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{user}:{pwd}@{server}/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)

        # add elapsed time to final print out
        print("Data imported successfully")
    except Exception as ex:
        print("Data load error: " + str(ex))


if __name__ == '__main__':
    try:
        # call extract function
        extract()
    except Exception as e:
        print("Error while extracting data: " + str(e))
