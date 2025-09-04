from enum import Enum
from pydantic import BaseModel, Field


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Source(BaseModel):
    bucket: str = ""
    query: str = ""
    start_timestamp: str = ""
    batch_size: int = 10_000


class Destination(BaseModel):
    preserve_insertion_order: bool = True
    table_name: str = ""
    load_type: LoadType = Field(default=LoadType.incremental_load)


class Configuration(BaseModel):
    url: str = ""
    token: str = Field(alias="#token")
    org: str = ""
    source: Source
    destination: Destination
    debug: bool = False
