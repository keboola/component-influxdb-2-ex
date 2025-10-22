from enum import Enum
from pydantic import BaseModel, Field, computed_field


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Source(BaseModel):
    bucket: str = ""
    query: str = 'from(bucket: "{bucket}")|> range(start: {start})|> limit(n: {batch_size}, offset: {offset})'
    start: str = ""
    batch_size: int = 10_000


class Destination(BaseModel):
    table_name: str = ""
    name_tables_by_tag_value: bool = False
    load_type: LoadType = Field(default=LoadType.incremental_load)
    primary_key: list[str] = []

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load


class Configuration(BaseModel):
    url: str
    token: str = Field(alias="#token")
    org: str = ""
    source: Source = Field(default_factory=Source)
    destination: Destination = Field(default_factory=Destination)
    debug: bool = False
    duckdb_max_memory_mb: int = 256
    test_mode: bool = False
