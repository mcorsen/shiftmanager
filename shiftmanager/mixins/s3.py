#!/usr/bin/env python

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import contextmanager
import datetime
from io import StringIO
import json
import os
import gzip
from functools import wraps

from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat

from shiftmanager import util, queries


def check_s3_connection(f):
    """
    Check class for S3 connection, try to connect if one is not present.
    """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.s3_conn:
            print("Connecting to S3."
                  "\nIf you have not set your credentials in"
                  " the environment or on the class, you can use the "
                  "set_aws_credentials method")
            self.s3_conn = self.get_s3_connection()
        return f(self, *args, **kwargs)
    return wrapper


class S3Mixin(object):
    """The S3 interaction base class for `Redshift`."""

    def __init__(self, *args, **kwargs):
        self.s3_conn = None
        self.aws_account_id = None
        self.aws_role_name = None

    def set_aws_credentials(self, aws_access_key_id, aws_secret_access_key,
                            security_token=None):
        """
        Set AWS credentials. These will be required for any methods that
        need interaction with S3

        Parameters
        ----------
        aws_access_key_id : str
        aws_secret_access_key : str
        security_token : str or None
            Temporary security token (if using creds from STS)
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.security_token = security_token

    def set_aws_role(self, aws_account_id, aws_role_name):
        """
        Set AWS IAM role. This rote will be assumed by the Redshift cluster
        when reading from S3 during COPY statements. If not set, the access key
        and secret from set_aws_credentials will be used.

        Parameters
        ----------
        aws_account_id : str
        aws_role_name : str
        """
        self.aws_account_id = aws_account_id
        self.aws_role_name = aws_role_name

    def get_s3_connection(self, ordinary_calling_fmt=False):
        """
        Get new S3 Connection

        Parameters
        ----------
        ordinary_calling_fmt : bool
            Initialize connection with OrdinaryCallingFormat
        """

        args = []
        kwargs = {}
        # Amazon used to use the AWS_SECURITY_TOKEN, but is transitioning
        # to AWS_SESSION_TOKEN. boto2 still only supports the old version,
        # but we want to support both.
        security_token = (os.environ.get('AWS_SECURITY_TOKEN') or
                          os.environ.get('AWS_SESSION_TOKEN') or
                          self.security_token)
        if security_token:
            kwargs['security_token'] = security_token

        # Workaround https://github.com/boto/boto/issues/2836
        if ordinary_calling_fmt:
            kwargs["calling_format"] = OrdinaryCallingFormat()
        if self.aws_access_key_id and self.aws_secret_access_key:
            args += [self.aws_access_key_id, self.aws_secret_access_key]
        s3_conn = S3Connection(*args, **kwargs)

        # Cache the creds that this connection found
        provider = s3_conn.provider
        if security_token:
            self.set_aws_credentials(provider.access_key,
                                     provider.secret_key,
                                     provider.security_token)
        else:
            self.set_aws_credentials(provider.access_key,
                                     provider.secret_key)

        return s3_conn

    def write_dict_to_key(self, data, key, close=False):
        """
        Given a Boto S3 Key, write a given dict to that key as JSON.

        Parameters
        ----------
        data : dict
        key : boto.s3.Key
        close : bool, default False
            Close key after write
        """
        fp = StringIO()
        fp.write(json.dumps(data, ensure_ascii=False))
        fp.seek(0)
        key.set_contents_from_file(fp)
        if close:
            key.close()
        return key

    def write_string_to_s3(self, chunk, bucket, s3_key_path):
        """
        Given a string chunk that represents a piece of a CSV file, write
        the chunk to an S3 key.

        Parameters
        ----------
        chunk: str
            String blob representing a chunk of a larger CSV
        bucket: boto.s3.bucket.Bucket
            The bucket to be written to
        s3_key_path: str
            The key path to write the chunk to
        """
        boto_key = bucket.new_key(s3_key_path)
        boto_key.set_contents_from_string(chunk, encrypt_key=True)

    def write_file_to_s3(self, f, bucket, s3_key_path):
        """
        Given a string chunk that represents a piece of a CSV file, write
        the chunk to an S3 key.

        Parameters
        ----------
        chunk: str
            String blob representing a chunk of a larger CSV
        bucket: boto.s3.bucket.Bucket
            The bucket to be written to
        s3_key_path: str
            The key path to write the chunk to
        """
        boto_key = bucket.new_key(s3_key_path)
        boto_key.set_contents_from_file(f, encrypt_key=True)

    def write_filename_to_s3(self, filename, bucket, s3_key_path):
        """
        Given a string chunk that represents a piece of a CSV file, write
        the chunk to an S3 key.

        Parameters
        ----------
        chunk: str
            String blob representing a chunk of a larger CSV
        bucket: boto.s3.bucket.Bucket
            The bucket to be written to
        s3_key_path: str
            The key path to write the chunk to
        """
        boto_key = bucket.new_key(s3_key_path)
        boto_key.set_contents_from_filename(filename, encrypt_key=True)

    @check_s3_connection
    def get_bucket(self, bucket_name):
        """
        Get boto.s3.bucket. Caches existing buckets.

        Parameters
        ----------
        bucket_name : str
        """
        try:
            bucket = self.s3_conn.get_bucket(bucket_name)
        except ValueError as e:
            # Addressing https://github.com/boto/boto/issues/2836
            # We'd like to catch an ssl.CertificateError here, but that
            # doesn't exist on some python installs. Since CertificateError
            # is just an empty subclass of ValueError and we re-raise
            # if the exception's message doesn't match what we expect,
            # we aren't exposing ourselves too much by catching
            # all ValueErrors here.
            dot_msg = ("doesn't match either of '*.s3.amazonaws.com',"
                       " 's3.amazonaws.com'")
            if dot_msg in str(e):
                self.s3_conn = (
                    self.get_s3_connection(ordinary_calling_fmt=True))
                bucket = self.s3_conn.get_bucket(bucket_name)
            else:
                raise

        return bucket

    @staticmethod
    @contextmanager
    def chunked_json_slices(data, slices, directory=None, clean_on_exit=True):
        """
        Given an iterator of dicts, chunk them into *slices* and write to
        temp files on disk. Clean up when leaving scope.

        Parameters
        ----------
        data : iter of dicts
            Iterable of dictionaries to be serialized to chunks
        slices : int
            Number of chunks to generate
        dir : str
            Dir to write chunks to. Will default to $HOME/.shiftmanager/tmp/
        clean_on_exit : bool, default True
            Clean up chunks on disk when context exits

        Returns
        -------
        stamp : str
            Timestamp that prepends the filenames of chunks written to disc
        chunk_files : list
            List of filenames
        """

        # Ensure that files get cleaned up even on raised exception
        try:
            num_data = len(data)
            chunk_range_start = util.linspace(0, num_data, slices)
            chunk_range_end = chunk_range_start[1:]
            chunk_range_end.append(None)
            stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f")

            if not directory:
                user_home = os.path.expanduser("~")
                directory = os.path.join(user_home, ".shiftmanager", "tmp")

            if not os.path.exists(directory):
                os.makedirs(directory)

            chunk_files = []
            range_zipper = list(zip(chunk_range_start, chunk_range_end))
            for i, (inclusive, exclusive) in enumerate(range_zipper):

                # Get either a inc/excl slice,
                # or the slice to the end of the range
                if exclusive is not None:
                    sliced = data[inclusive:exclusive]
                else:
                    sliced = data[inclusive:]

                newlined = ""
                for doc in sliced:
                    newlined = "{}{}\n".format(newlined, json.dumps(doc))

                filepath = "{}.gz".format("-".join([stamp, str(i)]))
                write_path = os.path.join(directory, filepath)
                current_fp = gzip.open(write_path, 'wb')
                current_fp.write(newlined.encode("utf-8"))
                current_fp.close()
                chunk_files.append(write_path)

            yield stamp, chunk_files

        finally:
            if clean_on_exit:
                for filepath in chunk_files:
                    os.remove(filepath)

    @staticmethod
    def gen_jsonpaths(json_doc, list_idx=None):
        """
        Generate Redshift jsonpath file for given JSON document or dict.

        If an array is present, you can specify an index to use for that
        field in the jsonpaths result. Right now only a single index is
        supported.

        Results will be ordered alphabetically by default.

        Parameters
        ----------
        json_doc : str or dict
            Dictionary or JSON-able string
        list_idx : int
            Index for array position

        Returns
        -------
        Dict
        """
        if isinstance(json_doc, str):
            parsed = json.loads(json_doc)
        else:
            parsed = json_doc

        paths_set = util.recur_dict(set(), parsed, list_idx=list_idx)
        paths_list = list(paths_set)
        paths_list.sort()
        return {"jsonpaths": paths_list}

    @check_s3_connection
    def copy_json_to_table(self, bucket, keypath, data, jsonpaths, table,
                           slices=32, clean_up_s3=True, local_path=None,
                           clean_up_local=True):
        """
        Given a list of JSON-able dicts, COPY them to the given *table_name*

        This function will partition the blobs into *slices* number of files,
        write them to the s3 *bucket*, write the jsonpaths file, COPY them to
        the table, then optionally clean up everything in the bucket.

        Parameters
        ----------
        bucket : str
            S3 bucket for writes
        keypath : str
            S3 key path for writes
        data : iterable of dicts
            Iterable of JSON-able dicts
        jsonpaths : dict
            Redshift jsonpaths file. If None, will autogenerate with
            alphabetical order
        table : str
            Table name for COPY
        slices : int
            Number of slices in your cluster. This many files will be generated
            on S3 for efficient COPY.
        clean_up_s3 : bool
            Clean up S3 bucket after COPY completes
        local_path : str
            Local path to write chunked JSON. Defaults to
            $HOME/.shiftmanager/tmp/
        clean_up_local : bool
            Clean up local chunked JSON after COPY completes.
        """

        print("Fetching S3 bucket {}...".format(bucket))
        bukkit = self.get_bucket(bucket)

        # Keys to clean up
        s3_sweep = []

        # Ensure S3 cleanup on failure
        try:
            with self.chunked_json_slices(data, slices, local_path,
                                          clean_up_local) \
                    as (stamp, file_paths):

                manifest = {"entries": []}

                print("Writing chunks...")
                for path in file_paths:
                    filename = os.path.basename(path)
                    # Strip leading slash
                    if keypath[0] == "/":
                        keypath = keypath[1:]

                    data_keypath = os.path.join(keypath, filename)
                    data_key = bukkit.new_key(data_keypath)
                    s3_sweep.append(data_keypath)

                    with open(path, 'rb') as f:
                        data_key.set_contents_from_file(f)

                    manifest_entry = {
                        "url": "s3://{}/{}".format(bukkit.name, data_keypath),
                        "mandatory": True
                    }
                    manifest["entries"].append(manifest_entry)
                    data_key.close()

                stamped_path = os.path.join(keypath, stamp)

                def single_dict_write(ext, single_data):
                    kpath = "".join([stamped_path, ext])
                    complete_path = "s3://{}/{}".format(bukkit.name, kpath)
                    key = bukkit.new_key(kpath)
                    self.write_dict_to_key(single_data, key, close=True)
                    s3_sweep.append(kpath)
                    return complete_path

                print("Writing .manifest file...")
                mfest_complete_path = single_dict_write(".manifest", manifest)

                print("Writing jsonpaths file...")
                jpaths_complete_path = single_dict_write(".jsonpaths",
                                                         jsonpaths)

            creds = "aws_access_key_id={};aws_secret_access_key={}".format(
                self.aws_access_key_id, self.aws_secret_access_key)
            if self.security_token:
                creds += ';token={}'.format(self.security_token)

            statement = queries.copy_from_s3.format(
                table=table, manifest_key=mfest_complete_path,
                creds=creds, jpaths_key=jpaths_complete_path)

            print("Performing COPY...")
            self.execute(statement)

        finally:
            if clean_up_s3:
                bukkit.delete_keys(s3_sweep)
