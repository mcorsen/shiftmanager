"""
Defines a Redshift class which encapsulates a database connection
and utility functions for managing that database.
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os

import psycopg2

from shiftmanager.mixins import (AdminMixin, ReflectionMixin, PostgresMixin,
                                 S3Mixin)
from shiftmanager.memoized_property import memoized_property


class Redshift(AdminMixin, ReflectionMixin, PostgresMixin, S3Mixin):
    """Interface to Redshift.

    This class will default to environment params for all arguments.

    For methods requiring S3,  aws keys are not required if you have
    environmental params set for boto to pick up:
    http://boto.readthedocs.org/en/latest/s3_tut.html#creating-a-connection

    Parameters
    ----------
    database : str
        envvar equivalent: PGDATABASE
    user : str
        envvar equivalent: PGUSER
    password : str
        envvar equivalent: PGPASSWORD
    host : str
        envvar equivalent: PGHOST
    port : int
        envvar equivalent: PGPORT
    aws_access_key_id : str
        envvar equivalent: AWS_ACCESS_KEY_ID
    aws_secret_access_key : str
        envvar equivalent: AWS_SECRET_ACCESS_KEY
    security_token : str
        envvar equivalent: AWS_SECURITY_TOKEN or AWS_SESSION_TOKEN
    kwargs : dict
        Additional keyword arguments sent to psycopg2.connect
    """

    @memoized_property
    def connection(self):
        """A `psycopg2.connect` connection to Redshift.

        Instantiation is delayed until the object is first used.
        """
        print("Connecting to %s..." % self.host)
        return psycopg2.connect(user=self.user,
                                host=self.host,
                                port=self.port,
                                database=self.database,
                                password=self.password,
                                **self.pgkwargs)

    def __init__(self, database=None, user=None, password=None, host=None,
                 port=5439,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 security_token=None,
                 **kwargs):

        self.set_aws_credentials(aws_access_key_id, aws_secret_access_key,
                                 security_token)

        self.user = user or os.environ.get('PGUSER')
        self.host = host or os.environ.get('PGHOST')
        self.port = port or os.environ.get('PGPORT')
        self.database = database or os.environ.get('PGDATABASE')
        self.password = password or os.environ.get('PGPASSWORD')
        self.pgkwargs = kwargs

        self._all_privileges = None

        S3Mixin.__init__(self)

    def execute(self, batch, parameters=None):
        """
        Execute a batch of SQL statements using this instance's connection.

        Statements are executed within a transaction.

        Parameters
        ----------
        batch : str
            The batch of SQL statements to execute.
        parameters : list or dict
            Values to bind to the batch, passed to `cursor.execute`
        """
        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(batch, parameters)

    def mogrify(self, batch, parameters=None, execute=False):
        if execute:
            self.execute(batch, parameters)
        with self.connection as conn:
            with conn.cursor() as cur:
                mogrified = cur.mogrify(batch, parameters)
        return mogrified.decode('utf-8')

    def table_exists(self, table_name):
        """
        Check Redshift for whether a table exists.

        Parameters
        ----------
        table_name : str
            The name of the table for whose existence we're checking

        Returns
        -------
        boolean
        """
        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute("""select count (distinct tablename)
                               from pg_table_def
                               where tablename = '{}';""".format(table_name))

                table_count = cur.fetchone()[0]

        return table_count == 1
