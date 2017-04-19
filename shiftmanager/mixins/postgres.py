#!/usr/bin/env python

"""
Mixin classes for working with Postgres database exports
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import decimal
import json
import tempfile
import os
import shutil
import threading
from threading import Thread
import time

import psycopg2
import psycopg2.extras

from shiftmanager.memoized_property import memoized_property
from shiftmanager.mixins.s3 import S3Mixin


class PostgresMixin(S3Mixin):
    """The Postgres interaction base class for `Redshift`."""

    @memoized_property
    def pg_connection(self):
        """A `psycopg2.connect` connection to Postgres.

        Instantiation is delayed until the object is first used.
        """

        print("Connecting to %s..." % self.pg_args['host'])
        return psycopg2.connect(**self.pg_args)

    def pg_execute_and_commit_single_statement(self, statement):
        """Execute single Postgres statement"""
        with self.pg_connection as conn:
            with conn.cursor() as cur:
                cur.execute(statement)

    def create_pg_connection(self, **kwargs):
        """
        Create a `psycopg2.connect` connection to Redshift.

        See https://www.postgresql.org/docs/current/static/\
libpq-connect.html#LIBPQ-PARAMKEYWORDS
        for supported parameters.
        """

        # Use 'localhost' as default host rather than unix socket
        if 'host' not in kwargs:
            kwargs['host'] = 'localhost'

        self.pg_args = kwargs
        return self.pg_connection

    @property
    def aws_credentials(self):
        if self.aws_account_id and self.aws_role_name:
            template = ('aws_iam_role=arn:aws:iam::'
                        '{aws_account_id}:role/{role_name}')
            return template.format(aws_account_id=self.aws_account_id,
                                   role_name=self.aws_role_name)
        else:
            key_id = 'aws_access_key_id={};'.format(self.aws_access_key_id)
            secret_key_id = 'aws_secret_access_key={}'.format(
                self.aws_secret_access_key)
            template = '{key_id}{secret_key_id}'
            if self.security_token:
                template += ";token={security_token}".format(
                    security_token=self.security_token)
            return template.format(key_id=key_id,
                                   secret_key_id=secret_key_id)

    def _create_copy_statement(self, table_name, manifest_key_path):
        """Create Redshift copy statement for given table_name and
        the provided manifest_key_path.
        Parameters
        ----------
        table_name: str
            Redshift table name to COPY to
        manifest_key_path: str
            Complete S3 path to .manifest file
        Returns
        -------
        str
        """
        return """\
        COPY {table_name}
        FROM '{manifest_key_path}'
        CREDENTIALS '{aws_credentials}'
        MANIFEST
        TIMEFORMAT 'auto'
        GZIP
        JSON 'auto'
        """.format(table_name=table_name,
                   manifest_key_path=manifest_key_path,
                   aws_credentials=self.aws_credentials)

    def copy_table_to_redshift(self,
                               redshift_table_name,
                               bucket_name,
                               key_prefix,
                               pg_table_name=None,
                               pg_select_statement=None,
                               temp_file_dir=None,
                               cleanup_s3=True,
                               delete_statement=None,
                               manifest_max_keys=None,
                               line_bytes=104857600,
                               canned_acl=None):
        """
        Writes the contents of a Postgres table to Redshift.

        The approach here attempts to maximize speed and minimize local
        disk usage. The fastest method of extracting data from Postgres
        is the COPY command, which we use here, but pipe the output to
        the ``split`` and ``gzip`` shell utilities to create a series of
        compressed files. As files are created, a separate thread uploads
        them to S3 and removes them from local disk.

        Due to the use of external shell utilities, this function can
        only run on an operating system with GNU core-utils installed
        (available by default on Linux, and via homebrew on MacOS).

        Parameters
        ----------
        redshift_table_name: str
            Redshift table to which json files are to be written
        bucket_name: str
            The name of the S3 bucket to be written to
        key_prefix: str
            The key path within the bucket to write to
        pg_table_name: str
            Optional Postgres table name to be written to json if user
            does not want to specify subset
        pg_select_statement: str
            Optional select statement if user wants to specify subset of table
        temp_file_dir: str
            Optional Specify location of temporary files
        cleanup_s3: bool
            Optional Clean up S3 location on failure. Defaults to True.
        delete_statement: str or None
            When not None, this statement will be run in the same transaction
            as the (final) COPY statement.
            This is useful when you want to clean up a previous backfill
            at the same time as issuing a new backfill.
        manifest_max_keys: int or None
            If None, all S3 keys will be sent to Redshift in a single COPY
            transaction. Otherwise, this parameter sets an upper limit on the
            number of S3 keys included in the COPY manifest. If more keys were
            produced, then additional COPY statements will be issued.
            This is useful for particularly large loads that may timeout in
            a single transaction.
        line_bytes: int
            The maximum number of bytes to write to a single file
            (before compression); defaults to 100 MB
        canned_acl: str
            A canned ACL to apply to objects uploaded to S3
        """
        if not self.table_exists(redshift_table_name):
            raise ValueError("This table_name does not exist in Redshift!")

        bucket = self.get_bucket(bucket_name)

        backfill_timestamp = datetime.datetime.utcnow().strftime(
            "%Y-%m-%d_%H%M%S")

        final_key_prefix = key_prefix
        if not key_prefix.endswith("/"):
            final_key_prefix += "/"

        if pg_select_statement is None and pg_table_name is not None:
            pg_table_or_select = pg_table_name
        elif pg_select_statement is not None and pg_table_name is None:
            pg_table_or_select = '(' + pg_select_statement + ')'
        else:
            ValueError("Exactly one of pg_table_name or pg_select_statement "
                       "must be specified.")

        tmpdir = tempfile.mkdtemp(dir=temp_file_dir)

        # Here, we build a COPY statement that sends output into a Unix
        # pipeline. We use SQL dollar-quoting ($$) to avoid escaping quotes.
        # It goes through `split` and `gzip` to output compressed files.
        # The `sed` invocation at the end makes up for a quirk in Postgres
        # JSON output where backslashes are improperly doubled; for every pair
        # of backslashes we substitute a single backslash. Due to multiple
        # levels of quoting, a single backslash actually appears as 4
        # backslashes in the sed invocation.
        copy_statement = (
            r"COPY (SELECT row_to_json(x) FROM ({pg_table_or_select}) AS x) "
            r"TO PROGRAM $$"
            r"split - {tmpdir}/chunk_ --line-bytes={line_bytes} "
            r"""--filter='sed "s/\\\\\\\\/\\\\/g" | gzip > $FILE.json.gz'"""
            r"$$"
        ).format(pg_table_or_select=pg_table_or_select,
                 tmpdir=tmpdir, line_bytes=line_bytes)

        # Kick off a thread to upload files as they're produced
        s3_thread = S3UploaderThread(tmpdir, bucket, final_key_prefix,
                                     canned_acl)

        try:
            s3_thread.start()
            self.pg_execute_and_commit_single_statement(copy_statement)
            print("Finished extracting data from Postgres. "
                  "Waiting on uploads...")
            s3_thread.finish_uploads_and_exit()
            while s3_thread.is_alive():
                # We call join() in a loop with a 1 second timeout so that
                # a user hitting Ctrl-C will allow a KeyboardInterrupt
                # to be issued and we can exit. If we simply call join(),
                # it blocks and no exceptions can reach the main program.
                s3_thread.join(1)
            s3_keys = s3_thread.s3_keys
        except:
            s3_thread.abort()
            if cleanup_s3:
                print("Error while pulling data out of PostgreSQL. "
                      "Cleaning up S3...")
                for key in s3_thread.s3_keys:
                    bucket.delete_key(key)
                raise

        print("Uploads all done. Cleaning up temp directory " + tmpdir)
        shutil.rmtree(tmpdir)

        manifest_entries = [{
            'url': 's3://' + bucket.name + s3_path,
            'mandatory': True
        } for s3_path in s3_keys]

        start_idx = 0
        num_entries = len(manifest_entries)
        manifest_max_keys = manifest_max_keys or num_entries
        while (start_idx < num_entries):
            end_idx = min(num_entries, start_idx + manifest_max_keys)
            print("Using manifest_entries: start=%d, end=%d" %
                  (start_idx, end_idx))
            entries = manifest_entries[start_idx:end_idx]
            manifest = {'entries': entries}
            manifest_key_path = "".join([final_key_prefix, backfill_timestamp,
                                         str(start_idx), "-", str(end_idx),
                                         ".manifest"])
            s3_keys.append(manifest_key_path)

            print('Writing .manifest file to S3...')
            self.write_string_to_s3(json.dumps(manifest), bucket,
                                    manifest_key_path)
            complete_manifest_path = "".join(['s3://', bucket.name,
                                              manifest_key_path])
            statements = ""

            # Include the delete statement only on the last transaction.
            if delete_statement and end_idx == num_entries:
                statements += delete_statement + ';\n'

            statements += self._create_copy_statement(
                redshift_table_name, complete_manifest_path)

            print('Copying from S3 to Redshift...')
            try:
                self.execute(statements)
                start_idx = end_idx
            except:
                # Clean up S3 bucket in the event of any exception
                if cleanup_s3:
                    print("Error writing to Redshift! Cleaning up S3...")
                    for key in s3_keys:
                        bucket.delete_key(key)
                raise


class S3UploaderThread(Thread):
    """
    A thread that polls for files created in *dirpath*,
    uploads them to S3, and deletes them.

    When the thread finishes, a list of the keys uploaded is
    available through the *s3_keys* field.
    """
    def __init__(self, dirpath, bucket, key_prefix, canned_acl):
        """
        Create a thread.

        Parameters
        ----------
        dirpath: str
            Path to the directory to search for files to upload
        bucket: S3.Bucket
            Bucket for uploading files
        key_prefix: str
            Prefix for keys uploaded to S3
        canned_acl: str
            A canned ACL to set on keys uploaded to S3
        """
        Thread.__init__(self)
        self.daemon = True  # If main program aborts, thread will terminate
        self.dirpath = dirpath
        self.key_prefix = key_prefix
        self.canned_acl = canned_acl
        self.bucket = bucket
        self.s3_keys = []
        self._abort = threading.Event()
        self._file_creation_complete = threading.Event()

    def finish_uploads_and_exit(self):
        self._file_creation_complete.set()

    def abort(self):
        self._abort.set()

    def run(self):
        """
        Continuously polls for new files until
        *files_still_being_created* is set to False by the main thread.
        At that point, it will upload any remaining files and exit.
        """
        print("Started a thread for uploading files to S3.")
        while (True):
            files = sorted(os.listdir(self.dirpath))
            if self._file_creation_complete.is_set() and not files:
                return
            if files and not self._file_creation_complete.is_set():
                # The last listed file is the one being written to,
                # so let's skip it for now.
                files = files[:-1]
            for basename in files:
                if self._abort.is_set():
                    return
                filepath = os.path.join(self.dirpath, basename)
                complete_key_path = "".join([self.key_prefix, basename])
                print("Writing to S3: " + complete_key_path)
                boto_key = self.bucket.new_key(complete_key_path)
                boto_key.set_contents_from_filename(filepath, encrypt_key=True)
                if self.canned_acl is not None:
                    boto_key.set_canned_acl(self.canned_acl)
                self.s3_keys.append(complete_key_path)
                os.remove(filepath)
            time.sleep(1)


def serializer(obj):
    """
    JSON serializer with support for several non-core datatypes.
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError("Unserializable object {} of type {}"
                    .format(obj, type(obj)))
