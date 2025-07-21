import pandas as pd
from sqlalchemy import create_engine
from utils.logger import get_logger
from utils.config import engine
import warnings

# Logger configuration
logger = get_logger(__name__)

# Suppress the pandas warning globally for SQLAlchemy usage
warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

def load_to_db(df, table_name='weather_data'):
    try:
        # pandas DataFrame-ის დამატება SQL Server-ში
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Data has been loaded successfully")
    except Exception as e:
        print(f"Error: {e}")
