import influxdb_client
from datadirtest import TestDataDir
from influxdb_client.client.write_api import SYNCHRONOUS


def run(context: TestDataDir):
    client = influxdb_client.InfluxDBClient(url="http://influxdb2:8086", token="token", org="org")
    write_api = client.write_api(write_options=SYNCHRONOUS)

    buck_id = client.buckets_api().find_buckets(name="bucket")
    if buck_id.buckets:
        client.buckets_api().delete_bucket(buck_id.buckets[0].id)

    client.buckets_api().create_bucket(bucket_name="bucket", org_id="org")

    for i in range(10):
        p = influxdb_client.Point("field").tag("category-tag", "a").field("temp", i)
        write_api.write(bucket="bucket", org="org", record=p)

        p = influxdb_client.Point("field").tag("category-tag", "a").field("temp", i).field("hum", i + 50)
        write_api.write(bucket="bucket", org="org", record=p)

        p = influxdb_client.Point("field").tag("category-tag", "b").field("temp", i)
        write_api.write(bucket="bucket", org="org", record=p)

        p = influxdb_client.Point("field").tag("category-tag", "b").field("temp", i).field("hum", i + 50)
        write_api.write(bucket="bucket", org="org", record=p)
