from typing import Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine.cursor import CursorResult
from .utils import _init_connection, _strip_obj_cols


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
