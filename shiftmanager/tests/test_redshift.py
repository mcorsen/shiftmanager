#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redshift tests

Test Runner: PyTest
"""

from mock import MagicMock
import pytest

import shiftmanager.redshift as rs


@pytest.fixture
def mocks():
    """Mock the psycopg2 connection"""
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


@pytest.fixture
def mock_conn(monkeypatch, mocks):
    """Patch psycopg2 with connection mocks, return conn"""
    monkeypatch.setattr('psycopg2.connect',
                        lambda *args, **kwargs: mocks)
    return mocks


def assert_execute(conn, expected):
    """Helper for asserting an executed SQL statement on mock connection"""
    conn.cursor().execute.assert_called_with(expected)


def test_redshift_transaction(mock_conn):

    with rs.redshift_transaction("redshift_host") as (conn, cur):
        pass

    cur.execute.assert_called_once_with("SET search_path = analytics")
    conn.commit.assert_called_with()


def test_random_password():
    for password in [rs.random_password() for i in range(0, 6, 1)]:
        assert len(password) < 65
        assert len(password) > 7
        for char in r'''\/'"@ ''':
            assert char not in password


def test_create_user(mock_conn):

    rs.create_user("dev", "swiper", "swiperpass")

    expected = """
    CREATE USER swiper
    PASSWORD 'swiperpass'
    IN GROUP analyticsusers;
    ALTER USER swiper
    SET wlm_query_slot_count TO 4;
    """

    assert_execute(mock_conn, expected)


def test_set_password(mock_conn):

    rs.set_password("dev", "swiper", "swiperpass")

    expected = """
    ALTER USER swiper
    PASSWORD 'swiperpass';
    """

    assert_execute(mock_conn, expected)


def test_dedupe(mock_conn):

    rs.dedupe("dev", "test")

    expected = """
        -- make all updates to this table block
        LOCK test;

        -- CREATE TABLE LIKE copies the dist key
        CREATE TEMP TABLE test_copied (LIKE test);

        -- move the data
        INSERT INTO test_copied SELECT DISTINCT * FROM test;
        DELETE FROM test;  -- slower than TRUNCATE, but transaction-safe
        INSERT INTO test (SELECT * FROM test_copied);
        DROP TABLE test_copied;
        """

    assert_execute(mock_conn, expected)