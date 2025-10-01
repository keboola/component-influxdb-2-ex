import logging
import os
import time
from collections import OrderedDict
import hashlib

import duckdb
import influxdb_client
import pandas as pd
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._influxdb = self.init_influxdb()
        self._duckdb = self.init_duckdb()
        self.primary_keys = {}
        state = self.get_state_file() or {}
        self.columns_cache = state.get("columns_cache") or {}
        self.last_run = state.get("last_run", 0)

    def run(self):
        start_time = int(time.time())
        self.download_data_to_tmp_tables()
        self.export_db_tables()
        self.write_state_file({"last_run": start_time, "columns_cache": self.columns_cache})

    def init_influxdb(self) -> influxdb_client.InfluxDBClient:
        return influxdb_client.InfluxDBClient(url=self.params.url, token=self.params.token, org=self.params.org)

    def init_duckdb(self) -> duckdb.DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
            "threads": 1,
            "max_memory": f"{self.params.duckdb_max_memory_mb}MB",
        }
        conn = duckdb.connect(config=config)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;").fetchall()

        return conn

    def download_data_to_tmp_tables(self):
        start = self.last_run if self.params.source.start == "last_run" else self.params.source.start

        tag_names = self.get_tag_names()

        offset = 0
        i = 0
        while True:
            i += 1
            tick = time.time()
            query = self.params.source.query.format(
                bucket=self.params.source.bucket,
                start=start,
                batch_size=self.params.source.batch_size,
                offset=offset,
            )
            logging.debug(f"Running query: {query}")
            res_tables = self._influxdb.query_api().query_data_frame(query)

            if isinstance(res_tables, pd.DataFrame) and res_tables.empty:
                return

            if not isinstance(res_tables, list):
                res_tables = [res_tables]

            offset += self.params.source.batch_size

            logging.debug(f"Fetched batch {i} with offset {offset} in {time.time() - tick:.2f}s")

            for current_table in res_tables:
                self.write_data_frame_to_db(current_table, tag_names)

    def write_data_frame_to_db(self, current_table, tag_names):
        tick = time.time()
        current_table.columns = [col.replace(".", "__") for col in current_table.columns]

        col_names = current_table.columns.tolist()
        pks = [c for c in col_names if c not in ["result", "table", "_start", "_stop", "_value", "_field"]]

        available_tag_names = [tag for tag in tag_names if tag in col_names] if tag_names else []
        if available_tag_names and self.params.destination.name_tables_by_tag_value:
            tags_values = current_table.loc[0, available_tag_names].tolist()
            table_name = "_".join(str(v) for v in tags_values)

        else:
            table_name = hashlib.md5(("_".join(pks) if pks else "out_table").encode()).hexdigest()

        self.primary_keys[table_name] = available_tag_names

        if "_measurement" in col_names:
            self.primary_keys[table_name].append("_measurement")

        select_clause = "SELECT * EXCLUDE('result','table','_start','_stop')"
        self._duckdb.sql(
            f'CREATE TABLE IF NOT EXISTS "{table_name}" AS {select_clause} FROM current_table WITH NO DATA;'
        )
        self._duckdb.sql(f'INSERT INTO "{table_name}" BY NAME {select_clause} FROM current_table;')
        logging.debug(f"Wrote batch to table {table_name} in {time.time() - tick:.2f}s")

    def get_tag_names(self) -> set[str]:
        """
        run query to export schema of long table and filter tag columns
        """
        res_helper = self._influxdb.query_api().query_data_frame(
            'from(bucket: "{bucket}")|> range(start: 0)|> limit(n: 0)'.format(bucket=self.params.source.bucket)
        )
        all_columns = set().union(*[df.columns for df in res_helper])
        tag_names = {col for col in all_columns if not col.startswith("_") and col not in {"result", "table"}}
        if self.params.destination.name_tables_by_tag_value and not tag_names:
            raise UserException("No tag columns found in the source bucket, cannot name tables by tag value.")
        return tag_names

    def export_db_tables(self):
        for current_table in self._duckdb.execute("SHOW TABLES;").fetchall():
            tick = time.time()
            current_table_name = current_table[0]

            # Get current table schema with column names and data types
            table_meta = self._duckdb.execute(f'DESCRIBE "{current_table_name}";').fetchall()
            current_columns = {c[0]: c[1] for c in table_meta}  # {column_name: datatype}

            # Handle schema expansion for existing tables
            self._ensure_schema_consistency(current_table_name, current_columns)

            # Re-fetch table metadata after potential schema changes to get the complete schema
            table_meta = self._duckdb.execute(f'DESCRIBE "{current_table_name}";').fetchall()
            complete_columns = {c[0]: c[1] for c in table_meta}  # Complete schema including added columns

            self.columns_cache[current_table_name] = complete_columns

            no_rows = self._duckdb.execute(f'SELECT COUNT (*) FROM "{current_table_name}";').fetchall()[0][0]
            logging.info(f"Writing {no_rows} rows, to the output table {current_table_name}")

            schema = OrderedDict(
                {c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_dtypes(c[1]))) for c in table_meta}
            )

            pks = self.primary_keys.get(current_table_name, [])

            out_table = self.create_out_table_definition(
                f"{current_table_name}.csv",
                schema=schema,
                primary_key=["_time"] + pks,
                incremental=self.params.destination.incremental,
                has_header=True,
            )

            try:
                q = f"COPY '{current_table_name}' TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
                self._duckdb.execute(q)
            except duckdb.ConversionException as e:
                raise UserException(f"Error during query execution: {e}") from e

            self.write_manifest(out_table)

            logging.debug(f"Exported table {current_table_name} in {time.time() - tick:.2f}s")

    def convert_dtypes(self, dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    def _ensure_schema_consistency(self, table_name: str, current_columns: dict[str, str]):
        """Add any missing columns from cache to maintain schema consistency across runs."""
        if table_name not in self.columns_cache:
            return  # New table, no previous schema to maintain

        cached_columns = self.columns_cache[table_name]
        missing_columns = set(cached_columns.keys()) - set(current_columns.keys())

        for col_name in missing_columns:
            stored_datatype = cached_columns[col_name]
            logging.info(f"Adding missing column '{col_name}' with type '{stored_datatype}' to table '{table_name}'")
            self._duckdb.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {stored_datatype};')

    @sync_action("list_buckets")
    def list_uc_tables(self):
        buckets_api = self._influxdb.buckets_api()
        buckets_iter = buckets_api.find_buckets_iter()
        return [SelectElement(b.name) for b in buckets_iter if b.type == "user"]

    @sync_action("list_organizations")
    def list_organizations(self):
        organizations_api = self._influxdb.organizations_api()
        orgs = organizations_api.find_organizations()
        return [SelectElement(o.name) for o in orgs]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
