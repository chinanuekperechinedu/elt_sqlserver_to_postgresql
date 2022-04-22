from dotenv import load_dotenv
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

load_dotenv()


DB_USER = os.getenv('DB_PG_USER')
DB_PASSWORD = os.getenv('DB_PG_PASSWORD')

print(DB_USER)
print(DB_PASSWORD)