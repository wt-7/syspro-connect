from dotenv import load_dotenv
from sqlalchemy.engine import URL, create_engine
import pandas as pd
import os


def _init_connection():
    load_dotenv()
    con_str = os.environ.get("SYSPRO_CONNECTION_STRING")
    if con_str is None:
        raise ValueError(
            """
            Error loading SYSPRO_CONNECTION_STRING from .env. 
            Ensure that the variable is set and the file is in the project root.
            """
        )
    con_url = URL.create("mssql+pyodbc", query={"odbc_connect": con_str})
    return create_engine(con_url)


def _strip_obj_cols(df: pd.DataFrame) -> pd.DataFrame:
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)
