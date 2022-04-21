from dotenv import load_dotenv
import os

load_dotenv()


DB_USER = os.getenv('DB_PG_USER')
DB_PASSWORD = os.getenv('DB_PG_PASSWORD')

print(DB_USER)
print(DB_PASSWORD)