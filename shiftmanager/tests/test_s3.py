#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for S3Mixin.

Test Runner: PyTest
"""

import gzip
import json
import os

from mock import ANY
import pytest


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


def assert_execute(shift, expected):
    """Helper for asserting an executed SQL statement on mock connection"""
    assert shift.execute.called
    shift.execute.assert_called_with(SqlTextMatcher(expected))


def test_connection_with_security_token(shift):
    shift.set_aws_credentials('my_access_key', 'my_secret_key', 'my_token')
    assert shift.security_token == 'my_token'


def test_connection_with_role(shift):
    shift.set_aws_role('my_account_id', 'my_role_name')
    assert shift.aws_account_id == 'my_account_id'
    assert shift.aws_role_name == 'my_role_name'


def test_jsonpaths(shift):

    test_dict_1 = {"one": 1, "two": {"three": 3}}
    expected_1 = {"jsonpaths": ["$['one']", "$['two']['three']"]}
    assert expected_1 == shift.gen_jsonpaths(test_dict_1)

    test_dict_2 = {"one": [0, 1, 2], "a": {"b": [0]}}
    expected_2 = {"jsonpaths": ["$['a']['b'][1]", "$['one'][1]"]}
    assert expected_2 == shift.gen_jsonpaths(test_dict_2, 1)


def chunk_checker(file_paths):
    """Ensure that we wrote and can read all 16 integers"""
    expected_numbers = list(range(1, 17, 1))
    result_numbers = []
    for filepath in file_paths:
        with gzip.open(filepath, 'rb') as f:
            decoded = f.read().decode("utf-8")
            res = [json.loads(x)["a"] for x in decoded.split("\n")
                   if x != ""]
            result_numbers.extend(res)

    assert expected_numbers == result_numbers


def test_chunk_json_slices(shift, json_data, tmpdir):
    data = json_data
    dpath = str(tmpdir)
    for slices in range(1, 19, 1):
        with shift.chunked_json_slices(data, slices, dpath) as (stamp, paths):
            assert len(paths) == slices
            chunk_checker(paths)
        with shift.chunked_json_slices(data, slices, dpath) as (stamp, paths):
            assert len(paths) == slices
            chunk_checker(paths)


def test_get_bucket(shift):
    def raise_error(*args):
        raise ValueError("doesn't match either of '*.s3.amazonaws.com',"
                         " 's3.amazonaws.com'")
    shift.get_s3_connection()
    shift.s3_conn.get_bucket.side_effect = raise_error
    with pytest.raises(ValueError):
        shift.get_bucket("bucket.with.dots")


def check_key_calls(s3keys, slices):
    """Helper for checking keys have been called correctly"""

    # Ensure we wrote the correct number of files, with the correct extensions
    assert len(s3keys) == (slices + 2)
    extensions = slices*["gz"]
    extensions.extend(["manifest", "jsonpaths"])
    extensions.sort()

    res_ext = [v.split(".")[-1] for v in s3keys.keys()]
    res_ext.sort()

    assert res_ext == extensions

    # Ensure each had contents set from file once, closed once
    for val in s3keys.values():
        val.set_contents_from_file.assert_called_once_with(ANY)
        val.close.assert_called_once_with()


def get_manifest_and_jsonpaths_keys(s3keys):
    manifest = ["s3://com.simple.mock/{}".format(x)
                for x in s3keys.keys() if "manifest" in x][0]
    jsonpaths = ["s3://com.simple.mock/{}".format(x)
                 for x in s3keys.keys() if "jsonpaths" in x][0]
    return manifest, jsonpaths


def test_copy_to_json(shift, json_data, tmpdir):

    jsonpaths = shift.gen_jsonpaths(json_data[0])

    # With cleanup
    shift.copy_json_to_table("com.simple.mock",
                             "tmp/tests/",
                             json_data,
                             jsonpaths,
                             "foo_table",
                             slices=5)

    # Get our mock bucket
    bukkit = shift.s3_conn.get_bucket("com.simple.mock")
    # 5 slices, one manifest, one jsonpaths
    check_key_calls(bukkit.s3keys, 5)
    mfest, jpaths = get_manifest_and_jsonpaths_keys(bukkit.s3keys)

    expect_creds = ("aws_access_key_id={};aws_secret_access_key={};token={}"
                    .format("access_key", "secret_key", "security_token"))
    expected = """
            COPY foo_table
            FROM '{manifest}'
            CREDENTIALS '{creds}'
            JSON '{jsonpaths}'
            MANIFEST GZIP TIMEFORMAT 'auto'
            """.format(manifest=mfest, creds=expect_creds,
                       jsonpaths=jpaths)

    assert_execute(shift, expected)

    # Did we clean up?
    assert set(bukkit.recently_deleted_keys) == set(bukkit.s3keys.keys())

    # Without cleanup
    bukkit.reset()
    shift.copy_json_to_table("com.simple.mock",
                             "tmp/tests/",
                             json_data,
                             jsonpaths,
                             "foo_table",
                             slices=4,
                             clean_up_s3=False)

    bukkit = shift.s3_conn.get_bucket("com.simple.mock")
    # 4 slices
    check_key_calls(bukkit.s3keys, 4)

    # Should not have cleaned up S3
    assert bukkit.recently_deleted_keys == []

    # Do not cleanup local
    bukkit.reset()
    dpath = str(tmpdir)
    shift.copy_json_to_table("com.simple.mock",
                             "tmp/tests/",
                             json_data,
                             jsonpaths,
                             "foo_table",
                             slices=10,
                             local_path=dpath,
                             clean_up_local=False)
    bukkit = shift.s3_conn.get_bucket("com.simple.mock")
    check_key_calls(bukkit.s3keys, 10)
    assert len(os.listdir(dpath)) == 10


def test_unload_table_to_s3(shift):
    bucket = 'com.simple.mock'
    keypath = 'tmp/tests/'
    table = 'foo_table'

    expect_col_str = r"""'{' ||
    CASE
        WHEN "foo" IS NULL THEN '"foo": null'
        WHEN "foo" THEN '"foo": true'
        ELSE '"foo": false'
    END || ', ' ||
    CASE
        WHEN "bar" IS NULL THEN '"bar": null'
        ELSE '"bar": ' || "bar"
    END || ', ' ||
    CASE
        WHEN "baz" IS NULL THEN '"baz": null'
        ELSE '"baz": "' ||
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE("baz", '\\', '\\\\'),
                                               '"', '\\"'),
                                               '\n', '\\n'),
                                               '\t', '\\t'),
                                               '\r', '\\r'),
                                               '\f', '\\f'),
                                               '\b', '\\b')
        || '"'
    END || '}'
    """
    expect_s3_path = 's3://' + os.path.join(bucket, keypath, table + '/')
    expect_creds = ("aws_access_key_id={};aws_secret_access_key={};token={}"
                    .format("access_key", "secret_key", "security_token"))
    expect_options = "MANIFEST GZIP ALLOWOVERWRITE"

    expected = """
    UNLOAD ($$SELECT {col_str} FROM {table}$$)
    TO '{s3_path}'
    CREDENTIALS '{creds}'
    {options};
    """.format(table=table, col_str=expect_col_str, s3_path=expect_s3_path,
               creds=expect_creds, options=expect_options)

    shift.unload_table_to_s3(bucket, keypath, table)
    assert_execute(shift, expected)
