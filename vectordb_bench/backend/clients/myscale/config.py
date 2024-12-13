from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, IndexType, MetricType


class MyscaleConfig(DBConfig):
    def to_dict(self) -> dict:
        return {"db_label": "myscale"}


class MyscaleIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def index_param(self) -> dict:
        return {}

    def search_param(self) -> dict:
        return {}
