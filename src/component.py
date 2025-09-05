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

    def run(self):
        start_time = int(time.time())

        self.download_data_to_tmp_tables()

        self.write_tables()

        self.write_state_file({"last_run": start_time})

    def init_influxdb(self) -> influxdb_client.InfluxDBClient:
        return influxdb_client.InfluxDBClient(url=self.params.url, token=self.params.token, org=self.params.org)

    def init_duckdb(self) -> duckdb.DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {"temp_directory": DUCK_DB_DIR, "extension_directory": os.path.join(DUCK_DB_DIR, "extensions")}
        conn = duckdb.connect(config=config)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;").fetchall()

        return conn

    def download_data_to_tmp_tables(self):
        start_timestamp = (
            self.get_state_file().get("last_run")
            if self.params.source.start_timestamp == "last_run"
            else self.params.source.start_timestamp
        )

        offset = 0
        while True:
            res_tables = self._influxdb.query_api().query_data_frame(
                self.params.source.query.format(
                    bucket=self.params.source.bucket,
                    start_timestamp=start_timestamp,
                    batch_size=self.params.source.batch_size,
                    offset=offset,
                )
            )

            if isinstance(res_tables, pd.DataFrame) and res_tables.empty:
                return

            if not isinstance(res_tables, list):
                res_tables = [res_tables]

            offset += self.params.source.batch_size

            for current_table in res_tables:
                col_names = current_table.columns.tolist()
                tags_fields_names = [
                    col for col in col_names if not col.startswith("_") and col not in {"result", "table"}
                ]
                table_name = "_".join(tags_fields_names) if tags_fields_names else "out_table"

                select_clause = "SELECT * EXCLUDE('result','table','_start','_stop')"

                self._duckdb.sql(
                    f'CREATE TABLE IF NOT EXISTS "{table_name}" AS {select_clause} FROM current_table WITH NO DATA;'
                )
                self._duckdb.sql(f'INSERT INTO "{table_name}" {select_clause} FROM current_table;')

    def write_tables(self):
        for current_table in self._duckdb.execute("SHOW TABLES;").fetchall():
            current_table_name = current_table[0]

            no_rows = self._duckdb.execute(f'SELECT COUNT (*) FROM "{current_table_name}";').fetchall()[0][0]
            logging.info(f"Writing {no_rows} rows, to the output table {current_table_name}")

            table_meta = self._duckdb.execute(f'DESCRIBE "{current_table_name}";').fetchall()
            schema = OrderedDict(
                {c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_dtypes(c[1]))) for c in table_meta}
            )

            out_table = self.create_out_table_definition(
                f"{hashlib.md5(current_table_name.encode()).hexdigest()}.csv",
                schema=schema,
                primary_key=self.params.destination.primary_key,
                incremental=self.params.destination.incremental,
                has_header=True,
            )

            try:
                self._duckdb.execute(
                    f"COPY \"{current_table_name}\" TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
                )
            except duckdb.ConversionException as e:
                raise UserException(f"Error during query execution: {e}") from e

            self.write_manifest(out_table)

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
