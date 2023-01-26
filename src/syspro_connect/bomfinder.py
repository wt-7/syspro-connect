import pandas as pd
from functools import lru_cache
from .syspro import Syspro


FBM_QUERY = """SELECT ParentPart, Component as stock_code, QtyPer FROM BomStructure"""
MERGE_QUERY = """SELECT StockCode as stock_code, Description, MaterialCost, LabourCost FROM InvMaster"""


class BOMFinder:
    """
    Find all components in a BOM.
    Loads the entire BOM structure into memory on instantiation.
    """

    def __init__(self, syspro: Syspro) -> None:
        self.syspro = syspro
        self.fbm = self.syspro.get_df(FBM_QUERY)

    def find(self, code: str) -> pd.DataFrame:
        """
        Find all components in a BOM.
        """
        if code not in self.valid_codes:
            return pd.DataFrame()

        return self._find_children(code).pipe(self._merge_details)

    @property
    def valid_codes(self):
        return set(self.fbm.ParentPart.unique())

    @lru_cache
    def _find_children(self, code: str) -> pd.DataFrame:
        children = self.fbm[self.fbm["ParentPart"] == code]
        for child in children.itertuples():
            grand_children = self._find_children(child.stock_code).assign(
                QtyPer=lambda x: x.QtyPer * child.QtyPer
            )
            children = pd.concat([children, grand_children])

        return children

    def _merge_details(self, df: pd.DataFrame) -> pd.DataFrame:
        codes_in_bom = set(df.stock_code.unique())
        # any code passed here is already valid
        param_placeholders = ",".join(["?"] * len(codes_in_bom))
        query_suffix = f" WHERE StockCode IN ({param_placeholders})"

        df = df.merge(
            self.syspro.get_df(
                sql=MERGE_QUERY + query_suffix,
                params=codes_in_bom,
            ),
            on="stock_code",
        )
        return df


if __name__ == "__main__":
    s = Syspro()
    b = BOMFinder(s)
