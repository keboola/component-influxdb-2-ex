import unittest

import mock
import urllib3
from keboola.component.exceptions import UserException

from component import QUERY_TIMEOUT_RETRY_ATTEMPTS, Component


def make_read_timeout() -> urllib3.exceptions.ReadTimeoutError:
    """Build the exception the InfluxDB client propagates when the HTTP read times out."""
    return urllib3.exceptions.ReadTimeoutError(
        urllib3.HTTPConnectionPool("influxdb.example.com", port=8086),
        "http://influxdb.example.com:8086/api/v2/query",
        "Read timed out. (read timeout=30)",
    )


class TestQueryDataFrameRetry(unittest.TestCase):
    """
    Covers the bounded retry added around InfluxDB queries.

    The component is not constructed (that needs a data dir and a live server), only the
    unbound _query_data_frame method is exercised against a stub holding the same attributes.
    """

    def setUp(self):
        self.stub = mock.MagicMock()
        self.stub.params.timeout = 10_000
        self.query_api = self.stub._influx.query_api.return_value

    def call(self):
        return Component._query_data_frame(self.stub, 'from(bucket: "b")|> range(start: 0)')

    @mock.patch("component.time.sleep")
    def test_successful_query_is_not_retried_and_result_is_passed_through(self, sleep_mock):
        self.query_api.query_data_frame.return_value = "result"

        self.assertEqual("result", self.call())
        self.assertEqual(1, self.query_api.query_data_frame.call_count)
        sleep_mock.assert_not_called()

    @mock.patch("component.time.sleep")
    def test_query_is_retried_after_a_timeout_and_the_later_result_is_returned(self, sleep_mock):
        self.query_api.query_data_frame.side_effect = [make_read_timeout(), "result"]

        self.assertEqual("result", self.call())
        self.assertEqual(2, self.query_api.query_data_frame.call_count)
        self.assertEqual(1, sleep_mock.call_count)

    @mock.patch("component.time.sleep")
    def test_persistent_timeout_still_fails_the_job_as_a_user_error(self, sleep_mock):
        self.query_api.query_data_frame.side_effect = make_read_timeout()

        with self.assertRaises(UserException) as ctx:
            self.call()

        self.assertIn("Could not reach the InfluxDB server in 3 attempts", str(ctx.exception))
        self.assertIn("10000 ms", str(ctx.exception))
        self.assertIn("Read timed out", str(ctx.exception))
        self.assertEqual(QUERY_TIMEOUT_RETRY_ATTEMPTS, self.query_api.query_data_frame.call_count)
        # No sleep after the final attempt.
        self.assertEqual(QUERY_TIMEOUT_RETRY_ATTEMPTS - 1, sleep_mock.call_count)

    @mock.patch("component.time.sleep")
    def test_backoff_between_attempts_is_exponential(self, sleep_mock):
        self.query_api.query_data_frame.side_effect = make_read_timeout()

        with self.assertRaises(UserException):
            self.call()

        delays = [call.args[0] for call in sleep_mock.call_args_list]
        self.assertEqual(sorted(delays), delays)
        self.assertEqual([2, 4], delays)

    @mock.patch("component.time.sleep")
    def test_non_timeout_errors_are_not_retried_or_reclassified(self, sleep_mock):
        self.query_api.query_data_frame.side_effect = ValueError("malformed Flux query")

        with self.assertRaises(ValueError):
            self.call()

        self.assertEqual(1, self.query_api.query_data_frame.call_count)
        sleep_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
