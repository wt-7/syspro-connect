from typing import Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine.cursor import CursorResult
from .utils import _init_connection, _strip_obj_cols

################################################################################


"""
Connects to a Syspro database from a SYSPRO_CONNECTION_STRING variable in a .env file in your project root.

Contains two APIs:
    
    1) Class based
    2) Functional
    
If multiple dataframes are needed, use a Syspro instance.
Streamlit cannot cache the Syspro instance, so use the get_df function instead
"""

################################################################################


class Syspro:
    """
    Syspro 8 API
    """

    def __init__(self):
        self.cnxn = _init_connection()

    def get_df(
        self, sql: str, *, params=None, stripped=True, dtypes: dict = {}
    ) -> pd.DataFrame:
        """
        Return a dataframe from an SQL query
        """

        df = pd.read_sql_query(sql, self.cnxn, params=params, dtype=dtypes)
        return df if not stripped else _strip_obj_cols(df)

    def get_query(self, query: str, **params) -> CursorResult:
        """
        Return a CursorResult from an SQL query
        """
        return self.cnxn.execute(text(query), **params)

    def get_single(self, query: str, **params) -> Any:
        """
        Return a single value from an SQL query. No error handling.
        """
        return self.cnxn.execute(text(query), **params).scalar()


################################################################################


def get_df(sql_str: str, *, stripped=True, dtypes: dict = {}) -> pd.DataFrame:
    """
    Return a dataframe from an SQL query
    """
    cnxn = _init_connection()
    df = pd.read_sql_query(sql_str, cnxn, dtype=dtypes)
    return df if not stripped else _strip_obj_cols(df)


def get_query(query: str) -> CursorResult:
    """
    Return a CursorResult from an SQL query
    """
    cnxn = _init_connection()
    return cnxn.execute(text(query))


################################################################################
