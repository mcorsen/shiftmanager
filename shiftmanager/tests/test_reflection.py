#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for ReflectionMixin.

Test Runner: PyTest
"""

import sqlalchemy as sa
import pytest


@pytest.fixture
def table():
    return sa.Table("my_table", sa.MetaData(),
                    sa.schema.Column("col1", sa.INTEGER))


@pytest.fixture
def complex_table():
    return sa.Table("my_complex_table", sa.MetaData(),
                    sa.schema.Column("col1", sa.INTEGER),
                    sa.schema.Column("col2", sa.INTEGER),
                    sa.schema.Column("col3", sa.INTEGER))


@pytest.fixture
def identity_table():
    return sa.Table("my_identity_table", sa.MetaData(),
                    sa.schema.Column("col1", sa.INTEGER),
                    sa.schema.Column("col2", sa.INTEGER),
                    sa.schema.Column("id_col", sa.INTEGER,
                                     info={'identity': [1, 1]}))


def cleaned(statement):
    text = str(statement)
    stripped_lines = [line.strip() for line in text.split('\n')]
    joined = '\n'.join([line for line in stripped_lines if line])
    return joined


class SqlTextMatcher(object):

    def __init__(self, text):
        self.text = text

    def __eq__(self, text):
        print(cleaned(self.text))
        print(cleaned(text))
        return cleaned(self.text) == cleaned(text)


def test_deep_copy_distinct(shift, table):
    statement = shift.deep_copy(table, distinct=True,
                                copy_privileges=False, analyze=False)
    expected = """
    LOCK TABLE my_table;
    ALTER TABLE my_table RENAME TO my_table$outgoing;
    CREATE TABLE my_table (
    col1 INTEGER
    );

    INSERT INTO my_table
    SELECT DISTINCT
        "col1"
    FROM my_table$outgoing;

    DROP TABLE my_table$outgoing;
    """
    assert(cleaned(statement) == cleaned(expected))


def test_cascade(shift, table):
    statement = shift.deep_copy(table, cascade=True,
                                copy_privileges=False, analyze=False)
    expected = """
    LOCK TABLE my_table;
    ALTER TABLE my_table RENAME TO my_table$outgoing;
    CREATE TABLE my_table (
    col1 INTEGER
    );

    INSERT INTO my_table
    SELECT
        "col1"
    FROM my_table$outgoing;

    DROP TABLE my_table$outgoing CASCADE;
    """
    assert(cleaned(statement) == cleaned(expected))


def test_deduplicate(shift, complex_table):
    statement = shift.deep_copy(complex_table,
                                deduplicate_partition_by="col1, col2",
                                deduplicate_order_by="col3 DESC",
                                copy_privileges=False, analyze=False)
    expected = """
    LOCK TABLE my_complex_table;
    ALTER TABLE my_complex_table RENAME TO my_complex_table$outgoing;

    CREATE TABLE my_complex_table (
    col1 INTEGER,
    col2 INTEGER,
    col3 INTEGER
    );

    INSERT INTO my_complex_table
    SELECT
        "col1",
        "col2",
        "col3"
    FROM (
        SELECT *, ROW_NUMBER()
        OVER (PARTITION BY col1, col2 ORDER BY col3 DESC)
        FROM my_complex_table$outgoing
    ) WHERE row_number = 1;

    DROP TABLE my_complex_table$outgoing;
    """
    assert(cleaned(statement) == cleaned(expected))


def test_identity(shift, identity_table):
    statement = shift.deep_copy(identity_table,
                                copy_privileges=False, analyze=False)

    expected = """
    LOCK TABLE my_identity_table;
    ALTER TABLE my_identity_table RENAME TO my_identity_table$outgoing;

    CREATE TABLE my_identity_table (
    col1 INTEGER,
    col2 INTEGER,
    id_col INTEGER IDENTITY(1,1)
    );

    INSERT INTO my_identity_table
    SELECT
        "col1",
        "col2"
    FROM my_identity_table$outgoing;

    DROP TABLE my_identity_table$outgoing;
    """
    assert(cleaned(statement) == cleaned(expected))
