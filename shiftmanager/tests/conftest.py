# -*- coding: utf-8 -*-
"""
Test fixture definitions.

These fixtures are automatically imported for test files in this directory.
"""

import collections
import random
import uuid

from mock import MagicMock, PropertyMock
import pytest
import psycopg2


@pytest.fixture
def mock_connection():
    mock_connection = PropertyMock()

    class MockCursor(object):

        def __init__(self, *args, **kwargs):

            self.statements = []
            self.return_rows = []
            self.cursor_position = 0

        def execute(self, statement, *args, **kwargs):
            self.statements.append(statement)

        def fetchone(self, *args, **kwargs):
            if self.cursor_position > len(self.return_rows) - 1:
                return None
            else:
                next_row = self.return_rows[self.cursor_position]
                self.cursor_position += 1
                return next_row

        def __enter__(self, *args, **kwargs):
            return self

        def __exit__(self, *args, **kwargs):
            return

        def __iter__(self):
            for row in self.return_rows:
                yield row

    mock_cursor = MockCursor()
    mock_connection_enter = MagicMock()
    mock_connection_enter.cursor.return_value = mock_cursor
    mock_connection.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.__enter__ = lambda x: mock_connection_enter
    mock_connection.__exit__ = MagicMock()
    return mock_connection


@pytest.fixture
def mock_s3():
    """Mock the S3 Connection, Bucket, and Key"""

    class MockBucket(object):

        s3keys = {}
        name = "com.simple.mock"

        def new_key(self, keypath):
            key_mock = MagicMock()
            self.s3keys[keypath] = key_mock
            return key_mock

        def delete_keys(self, keys):
            self.recently_deleted_keys = keys

        def reset(self):
            self.s3keys = {}
            self.recently_deleted_keys = []

    mock_S3 = MagicMock()
    mock_S3.get_bucket.return_value = MockBucket()
    return mock_S3


@pytest.fixture
def json_data():
    data = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4},
            {"a": 5}, {"a": 6}, {"a": 7}, {"a": 8},
            {"a": 9}, {"a": 10}, {"a": 11}, {"a": 12},
            {"a": 13}, {"a": 14}, {"a": 15}, {"a": 16}]
    return data


def mogrify(self, batch, parameters=None, execute=False):
    if isinstance(parameters, collections.Mapping):
        parameters = dict([
            (key, psycopg2.extensions.adapt(val).getquoted().decode('utf-8'))
            for key, val in parameters.items()])
    elif isinstance(parameters, collections.Sequence):
        parameters = [
            psycopg2.extensions.adapt(val).getquoted()
            for val in parameters]
    if parameters:
        return batch % parameters
    return batch


def id_col(self, table_name):
    if table_name == 'my_identity_table':
        return 'id_col'


@pytest.fixture
def shift(monkeypatch, mock_connection, mock_s3):
    """Patch psycopg2 with connection mocks, return conn"""
    import shiftmanager.redshift as rs

    monkeypatch.setattr('shiftmanager.Redshift.connection', mock_connection)
    monkeypatch.setattr('shiftmanager.Redshift.get_s3_connection',
                        lambda *args, **kwargs: mock_s3)
    monkeypatch.setattr('shiftmanager.Redshift.mogrify', mogrify)
    monkeypatch.setattr('shiftmanager.Redshift.execute', MagicMock())
    monkeypatch.setattr('shiftmanager.Redshift._get_identity', id_col)
    shift = rs.Redshift("", "", "", "",
                        aws_access_key_id="access_key",
                        aws_secret_access_key="secret_key",
                        security_token="security_token")
    shift.s3_conn = mock_s3
    return shift


@pytest.fixture
def postgres(monkeypatch, mock_connection, request, mock_s3):
    """
    Setup Postgres table with a few random columns of data for testing.
    Tear down table at end of text fixture context.
    """

    import shiftmanager.mixins.postgres as sp
    from shiftmanager.redshift import Redshift

    monkeypatch.setattr('shiftmanager.Redshift.connection', mock_connection)

    class PostgresRedshift(Redshift, sp.PostgresMixin):
        """We need the Redshift methods, but don't want to require Postgres
           for every single test. Thus, we create this dummy class"""

        def __init__(self, *args, **kwargs):
            super(PostgresRedshift, self).__init__(self, args, kwargs)

    pg = PostgresRedshift()
    pg.s3_conn = mock_s3
    conn = pg.create_pg_connection(database="shiftmanager",
                                   user="shiftmanager")
    cur = conn.cursor()

    # Just in case of an unclean exit
    drop_if_exists_query = "DROP TABLE IF EXISTS test_table;"

    cur.execute(drop_if_exists_query)

    # Temp table for test runs; schema is arbitrary.
    create_query = """CREATE TABLE test_table (
                          row_count integer,
                          uuid char(36),
                          name varchar(255));"""

    cur.execute(create_query)

    # Fill table with a few hundred rows of random data
    names = {"jill", "jane", "joe", "jim", "carol"}
    insert_statement = ["INSERT INTO test_table VALUES"]
    for i in range(0, 300, 1):
        name = random.sample(names, 1)[0]
        row_to_insert = " ({row_count}, '{uuid}', '{name}'),".format(
            row_count=i, uuid=uuid.uuid4(), name=name)
        insert_statement.append(row_to_insert)

    joined_insert = "".join(insert_statement)
    complete_insert = "{};".format(joined_insert[:-1])
    cur.execute(complete_insert)
    conn.commit()

    def teardown_pg():
        drop_query = "DROP TABLE test_table;"
        cur.execute(drop_query)
        conn.commit()
        cur.close()
        conn.close()

    request.addfinalizer(teardown_pg)
    return pg
