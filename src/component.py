import hashlib
import logging
import os
import time
import warnings
from collections import OrderedDict

import duckdb
import influxdb_client
import pandas as pd
from influxdb_client.client.warnings import MissingPivotFunction
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement


from configuration import Configuration

warnings.simplefilter("ignore", MissingPivotFunction)

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._influx = influxdb_client.InfluxDBClient(url=self.params.url, token=self.params.token, org=self.params.org)
        self._duckdb = self.init_duckdb()
        self.primary_keys = {}
        self.columns_cache = {}
        self.last_run = 0

    def run(self):
        start_time = int(time.time())
        state = self.get_state_file() or {}
        self.columns_cache = state.get("columns_cache") or {}
        self.last_run = state.get("last_run", 0)
        self.download_data_to_tmp_tables()
        self.export_db_tables()
        self.write_state_file({"last_run": start_time, "columns_cache": self.columns_cache})

    def init_duckdb(self) -> duckdb.DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
            "threads": 1,
            "max_memory": f"{self.params.duckdb_max_memory_mb}MB",
        }
        conn = duckdb.connect(config=config)

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
            res_tables = self._influx.query_api().query_data_frame(query)

            if isinstance(res_tables, pd.DataFrame) and res_tables.empty:
                return

            #  returns a single DataFrame if the result contains only one table
            if not isinstance(res_tables, list):
                res_tables = [res_tables]

            offset += self.params.source.batch_size

            logging.debug(f"Fetched batch {i} with offset {offset} in {time.time() - tick:.2f}s")

            for current_table in res_tables:
                self.write_data_frame_to_db(current_table, tag_names)

    def write_data_frame_to_db(self, current_table, tag_names):
        tick = time.time()
        current_table.columns = [col.replace(".", "__") for col in current_table.columns]

        table_name = self.save_pk_return_table_name(current_table, tag_names)

        to_exclude = set(current_table.columns).intersection({"result", "table", "_start", "_stop"})

        select_clause = f"SELECT * EXCLUDE ('{"', '".join(to_exclude)}')"
        self._duckdb.sql(
            f'CREATE TABLE IF NOT EXISTS "{table_name}" AS {select_clause} FROM current_table WITH NO DATA;'
        )
        self._duckdb.sql(f'INSERT INTO "{table_name}" BY NAME {select_clause} FROM current_table;')
        logging.debug(f"Wrote batch to table {table_name} in {time.time() - tick:.2f}s")

    def save_pk_return_table_name(self, current_table, tag_names):
        col_names = current_table.columns.tolist()
        pks = [c for c in col_names if c not in ["result", "table", "_start", "_stop", "_value", "_field"]]
        available_tag_names = [tag for tag in tag_names if tag in col_names] if tag_names else []
        if available_tag_names and self.params.destination.name_tables_by_tag_value:
            tags_values = current_table.loc[0, available_tag_names].tolist()
            table_name = "_".join(str(v) for v in tags_values)
        else:
            table_name = "_".join([pk for pk in pks if pk not in ["_time", "_measurement"]]) if pks else "out_table"

        # Truncate and hash table names longer than 64 characters
        if len(table_name) > 64:
            name_hash = hashlib.md5(table_name.encode()).hexdigest()[:16]
            table_name = f"{table_name[:40]}__{name_hash}"

        self.primary_keys[table_name] = available_tag_names
        if "_measurement" in col_names:
            self.primary_keys[table_name].append("_measurement")
        return table_name

    def get_tag_names(self) -> set[str]:
        """
        run query to export schema of long table and filter tag columns
        """
        res_helper = self._influx.query_api().query_data_frame(
            'from(bucket: "{bucket}")|> range(start: 0)|> limit(n: 0)'.format(bucket=self.params.source.bucket)
        )
        if not isinstance(res_helper, list):
            res_helper = [res_helper]
        all_columns = set().union(*[df.columns for df in res_helper])
        tag_names = {col for col in all_columns if not col.startswith("_") and col not in {"result", "table"}}
        if self.params.destination.name_tables_by_tag_value and not tag_names:
            raise UserException("No tag columns found in the source bucket, cannot name tables by tag value.")
        return tag_names

    def export_db_tables(self):
        tables = self._duckdb.execute("SHOW TABLES;").fetchall()

        if len(tables) != 1 and self.params.destination.table_name:
            raise UserException("Parameter 'table_name' can be used only if the query returns single table.")

        for current_table in tables:
            tick = time.time()
            current_table_name = current_table[0]

            # replace time column with index in test mode
            if self.params.test_mode:
                self._duckdb.execute(f"""
                CREATE OR REPLACE TABLE "{current_table_name}" AS (
                SELECT ROW_NUMBER() OVER () AS _time, * EXCLUDE (_time)
                FROM "{current_table_name}"
                )
                """)

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

            table_name = current_table_name
            if self.params.destination.table_name:
                table_name = self.params.destination.table_name

            out_table = self.create_out_table_definition(
                f"{table_name}.csv",
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
        buckets_api = self._influx.buckets_api()
        buckets_iter = buckets_api.find_buckets_iter()
        res = [SelectElement(b.name) for b in buckets_iter if b.type == "user"]
        if not res:
            raise UserException("Unable to list buckets.")
        return res

    @sync_action("list_organizations")
    def list_organizations(self):
        organizations_api = self._influx.organizations_api()
        orgs = organizations_api.find_organizations()
        res = [SelectElement(o.name) for o in orgs]
        if not res:
            raise UserException("Unable to list organizations.")
        return res


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
