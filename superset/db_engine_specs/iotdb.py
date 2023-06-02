from datetime import datetime

from superset.db_engine_specs.base import BaseEngineSpec
from superset.utils import core as utils
from sqlalchemy.engine.reflection import Inspector

from superset.models.core import Database

from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Match,
    NamedTuple,
    Optional,
    Pattern,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

class IoTDBEngineSpec(BaseEngineSpec):

    engine = "iotdb"
    engine_name = "Apache IoTDB"

    @classmethod
    def get_view_names(
        cls,
        database: "Database",
        inspector: Inspector,
        schema: Optional[str],
    ) -> Set[str]:
        """
        Get all the view names within the specified schema.

        Per the SQLAlchemy definition if the schema is omitted the database’s default
        schema is used, however some dialects infer the request as schema agnostic.

        Note that PyHive's Hive SQLAlchemy dialect does not adhere to the specification
        where the `get_view_names` method returns both real tables and views. Futhermore
        the dialect wrongfully infers the request as schema agnostic when the schema is
        omitted.

        :param database: The database to inspect
        :param inspector: The SQLAlchemy inspector
        :param schema: The schema to inspect
        :returns: The view names
        """

        return {}
    
    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATE_TRUNC('second', {col})",
        "PT1M": "DATE_TRUNC('minute', {col})",
        "PT1H": "DATE_TRUNC('hour', {col})",
        "P1D": "DATE_TRUNC('day', {col})",
        "P1W": "DATE_TRUNC('week', {col})",
        "P1M": "DATE_TRUNC('month', {col})",
        "P0.25Y": "DATE_TRUNC('quarter', {col})",
        "P1Y": "DATE_TRUNC('year', {col})",
    }

    @classmethod
    def alter_new_orm_column(cls, orm_col: "TableColumn") -> None:
        if orm_col.type == "TIMESTAMP":
            orm_col.python_date_format = "epoch_ms"

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime) -> Optional[str]:
        tt = target_type.upper()
        if tt == utils.TemporalType.TIMESTAMP:
            return f"{dttm.timestamp() * 1000}"
        return None

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "{col} * 1000"

    @classmethod
    def epoch_ms_to_dttm(cls) -> str:
        return "{col}"
