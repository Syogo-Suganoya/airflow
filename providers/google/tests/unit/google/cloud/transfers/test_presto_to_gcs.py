#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.google.cloud.transfers.presto_to_gcs import (
    PrestoToGCSOperator,
    _PrestoToGCSPrestoCursorAdapter,
)

TASK_ID = "test-presto-to-gcs"
PRESTO_CONN_ID = "my-presto-conn"
GCP_CONN_ID = "my-gcp-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
SQL = "SELECT * FROM memory.default.test_multiple_types"
BUCKET = "gs://test"
FILENAME = "test_{}.ndjson"

DESCRIPTION = [
    ("some_num", "INTEGER", None, None, None, None, None),
    ("some_str", "VARCHAR(20)", None, None, None, None, None),
]
ROWS = [[42, "mock_row_content_1"], [43, "mock_row_content_2"], [44, "mock_row_content_3"]]
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n',
]
CSV_LINES = [
    b"some_num,some_str\r\n",
    b"42,mock_row_content_1\r\n",
    b"43,mock_row_content_2\r\n",
    b"44,mock_row_content_3\r\n",
]
SCHEMA_FILENAME = "schema_test.json"
SCHEMA_JSON = b'[{"name": "some_num", "type": "INT64"}, {"name": "some_str", "type": "STRING"}]'


def _set_up_presto_cursor(mock_presto_hook):
    cursor = mock_presto_hook.return_value.get_conn.return_value.cursor.return_value
    cursor.description = DESCRIPTION
    cursor.fetchone.side_effect = [*ROWS, None]
    return cursor


class TestPrestoToGCSPrestoCursorAdapter:
    def test_description_peeks_first_row_without_consuming_it(self):
        cursor = mock.MagicMock()
        cursor.description = DESCRIPTION
        cursor.fetchone.side_effect = [*ROWS, None]
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.description == DESCRIPTION
        assert list(adapter) == ROWS

    def test_fetchmany_stops_at_end_of_result_set(self):
        cursor = mock.MagicMock()
        cursor.fetchone.side_effect = [*ROWS, None]
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.fetchmany(2) == ROWS[:2]
        assert adapter.fetchmany(5) == ROWS[2:]

    @pytest.mark.parametrize("method", ["execute", "executemany"])
    def test_execute_discards_peeked_rows(self, method):
        cursor = mock.MagicMock()
        cursor.fetchone.side_effect = [ROWS[0], ROWS[1]]
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)
        adapter.peekone()

        getattr(adapter, method)(SQL)

        getattr(cursor, method).assert_called_once_with(SQL)
        assert adapter.initialized is False
        assert adapter.fetchone() == ROWS[1]


class TestPrestoToGCSOperator:
    def test_init(self):
        op = PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        assert op.presto_conn_id == "presto_default"
        assert "presto_conn_id" in op.template_fields

    @pytest.mark.parametrize(
        ("field_type", "expected_bq_type"),
        [
            ("integer", "INT64"),
            ("DECIMAL(10, 2)", "NUMERIC"),
            ("TIMESTAMP WITH TIME ZONE", "STRING"),
            ("ARRAY(VARCHAR)", "STRING"),
        ],
    )
    def test_field_to_bigquery(self, field_type, expected_bq_type):
        op = PrestoToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)

        assert op.field_to_bigquery(("col", field_type)) == {"name": "col", "type": expected_bq_type}

    @pytest.mark.parametrize(
        ("export_format", "expected_mime_type", "expected_content"),
        [
            ("json", "application/json", b"".join(NDJSON_LINES)),
            ("csv", "text/csv", b"".join(CSV_LINES)),
        ],
        ids=["json", "csv"],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    @mock.patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    def test_execute(
        self, mock_presto_hook, mock_gcs_hook, export_format, expected_mime_type, expected_content
    ):
        uploads = {}

        def _capture_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            with open(tmp_filename, "rb") as file:
                uploads[obj] = (bucket, mime_type, gzip, file.read())

        mock_gcs_hook.return_value.upload.side_effect = _capture_upload
        cursor = _set_up_presto_cursor(mock_presto_hook)

        PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            export_format=export_format,
            presto_conn_id=PRESTO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(None)

        mock_presto_hook.assert_called_once_with(presto_conn_id=PRESTO_CONN_ID)
        cursor.execute.assert_called_once_with(SQL)
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN
        )
        assert uploads == {FILENAME.format(0): (BUCKET, expected_mime_type, False, expected_content)}

    @pytest.mark.parametrize(
        ("export_format", "lines"),
        [("json", NDJSON_LINES), ("csv", CSV_LINES)],
        ids=["json", "csv"],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    @mock.patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    def test_execute_with_file_splitting(self, mock_presto_hook, mock_gcs_hook, export_format, lines):
        if export_format == "csv":
            # Every CSV chunk repeats the header row.
            expected_uploads = {
                FILENAME.format(0): b"".join(lines[:3]),
                FILENAME.format(1): lines[0] + lines[3],
            }
        else:
            expected_uploads = {
                FILENAME.format(0): b"".join(lines[:2]),
                FILENAME.format(1): lines[2],
            }
        uploads = {}

        def _capture_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            with open(tmp_filename, "rb") as file:
                uploads[obj] = file.read()

        mock_gcs_hook.return_value.upload.side_effect = _capture_upload
        _set_up_presto_cursor(mock_presto_hook)

        PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            export_format=export_format,
            approx_max_file_size_bytes=len(expected_uploads[FILENAME.format(0)]),
        ).execute(None)

        assert uploads == expected_uploads

    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    @mock.patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    def test_execute_with_schema_file(self, mock_presto_hook, mock_gcs_hook):
        uploads = {}

        def _capture_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            with open(tmp_filename, "rb") as file:
                uploads[obj] = file.read()

        mock_gcs_hook.return_value.upload.side_effect = _capture_upload
        _set_up_presto_cursor(mock_presto_hook)

        PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME,
        ).execute(None)

        assert set(uploads) == {FILENAME.format(0), SCHEMA_FILENAME}
        assert uploads[SCHEMA_FILENAME] == SCHEMA_JSON
