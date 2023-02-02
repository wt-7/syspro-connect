import pandas as pd
from functools import lru_cache
from .syspro import Syspro


QUERY = """
WITH cte AS (
    SELECT Component,
        ParentPart,
        CAST(QtyPer AS float) as QtyPer
    FROM BomStructure
    WHERE ParentPart = ?
    UNION ALL
    SELECT t.Component,
        t.ParentPart,
        CAST(t.QtyPer AS float) * cte.QtyPer as QtyPer
    FROM BomStructure t
        JOIN cte ON t.ParentPart = cte.Component
)
SELECT Component,
    [Description],
    QtyPer,
    InvMaster.MaterialCost,
    InvMaster.LabourCost,
    UnitCost,
    DateLastPurchase
FROM cte
    LEFT JOIN InvMaster ON cte.Component = InvMaster.StockCode
    LEFT JOIN InvWarehouse ON cte.Component = InvWarehouse.StockCode
ORDER BY MaterialCost DESC;
"""


class Bomfinder:
    def __init__(self, syspro: Syspro):
        self.syspro = syspro

    def get_bom(self, bom_num: str) -> pd.DataFrame:
        return self.syspro.get_df(QUERY, params=[bom_num])


if __name__ == "__main__":
    s = Syspro()
    b = Bomfinder(s)

    print(b.get_bom("79806"))


# TODO: remove this for the sql implementation
