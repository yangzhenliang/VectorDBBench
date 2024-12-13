import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional, Tuple

from ..api import DBCaseConfig, VectorDB

import clickhouse_connect

log = logging.getLogger(__name__)

host_myscale = '124.71.232.93'
host_myscale = 'localhost'
port_myscale = 8123


class Myscale(VectorDB):
    def __init__(
            self,
            dim: int,
            db_config: dict,
            db_case_config: DBCaseConfig,
            collection_name: str = "MyscaleCollection",
            drop_old: bool = False,
            **kwargs,
    ):
        self.db_config = db_config
        self.case_config = db_case_config
        self.collection_name = collection_name

        self._primary_field = "pk"
        self._vector_field = "vector"

        tmp_client = clickhouse_connect.get_client(host=host_myscale, port=port_myscale, user='admin',
                                                   password='moqi.ai')
        if drop_old:
            log.info(f"Myscale client drop_old collection: {self.collection_name}")
            tmp_client.command("DROP TABLE IF EXISTS  " + self.collection_name)
            self._create_collection(dim, tmp_client)
        tmp_client = None
        log.info("Starting Myscale DB")

    @contextmanager
    def init(self) -> Generator[None, None, None]:
        """create and destroy connections to database.

        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
        """
        self.client = clickhouse_connect.get_client(host=host_myscale, port=port_myscale, user='admin',
                                                    password='moqi.ai')
        yield
        self.client = None
        del (self.client)

    def ready_to_load(self) -> bool:
        return True

    def optimize(self) -> None:
        pass

    def _create_collection(self, dim, client: int):
        log.info(f"Create collection: {self.collection_name}")
        client.command(f"""
                CREATE TABLE default.{self.collection_name}
                (
                    id    UInt32,
                    data Array(Float32),
                    CONSTRAINT check_length CHECK length(data) = {dim},
                    pk UInt32
                ) engine = MergeTree PRIMARY KEY id
                ORDER BY id""")

        client.command(
            f"""ALTER TABLE default.{self.collection_name} ADD VECTOR INDEX vec_idx data TYPE MSTG('metric_type=Cosine')""")
        try:
            pass

        except Exception as e:
            if "already exists!" in str(e):
                return
            log.warning(f"Failed to create collection: {self.collection_name} error: {e}")
            raise e from None

    def insert_embeddings(
            self,
            embeddings: list[list[float]],
            metadata: list[int],
            **kwargs: Any,
    ) -> Tuple[int, Optional[Exception]]:
        """Insert embeddings into the database.
        Should call self.init() first.
        """
        MYSCALE_BATCH_SIZE = 500
        try:
            for offset in range(0, len(embeddings), MYSCALE_BATCH_SIZE):
                vectors = embeddings[offset: offset + MYSCALE_BATCH_SIZE]
                ids = metadata[offset: offset + MYSCALE_BATCH_SIZE]
                data = [
                    (id, vector, id)
                    for id, vector in zip(ids, vectors)
                ]
                _ = self.client.insert(
                    table=self.collection_name,
                    data=data,
                    column_type_names=['UInt32', 'Array(Float32)', 'UInt32'],
                    column_names=['id', 'data', 'pk']
                )
        except Exception as e:
            log.info(f"Failed to insert data, {e}")
        return (len(embeddings), None)

    def search_embedding(
            self,
            query: list[float],
            k: int = 100,
            filters: dict | None = None,
            timeout: int | None = None,
            **kwargs: Any,
    ) -> list[int]:
        sql = f"""
            SELECT id, pk, distance(data, {query}) as dist 
            FROM default.{self.collection_name}
            """
        if filters:
            sql += f"WHERE id>{filters.get('id')}"
        sql += f" ORDER BY dist LIMIT {k}"
        result = self.client.query(sql)
        ret = [row['id'] for row in result.named_results()]
        return ret
