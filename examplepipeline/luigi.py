from __future__ import absolute_import

import abc

import luigi
from luigi.configuration import LuigiConfigParser

from . import config as config_mod
from .config import PostgresConfig


class PostgresTarget(luigi.Target):

    def __init__(self, schema, table, column, the_type):
        self.schema = schema
        self.table = table
        self.column = column
        self.the_type = the_type

    def exists(self):
        config = PostgresConfig(luigi_config=LuigiConfigParser.instance())
        return config.does_column_exist(schema=self.schema, table=self.table, column=self.column)

    def as_tuple(self):
        return (self.schema, self.table, self.column, self.the_type)

class PostgresColumnTask(luigi.Task):
    @abc.abstractmethod
    def query(self, curs):
        pass

    def run(self):
        config = PostgresConfig(luigi_config=LuigiConfigParser.instance())
        with config as conn:
            with conn.cursor() as curs:
                schema, table, column, the_type = self.output().as_tuple()
                if not config_mod._does_table_exist(curs, table, schema):
                    curs.execute("CREATE SCHEMA IF NOT EXISTS {}".format(schema))
                    curs.execute("CREATE TABLE {}.{} ({} {})".format(
                        schema, table, column, the_type))
                else:
                    curs.execute("ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}".format(
                        schema, table, column, the_type))
                    curs.execute("ALTER TABLE {}.{} ADD COLUMN {} {}".format(
                        schema, table, column, the_type))

                self.query(curs)
            conn.commit()


class PostgresCreateBasicTableTask(PostgresColumnTask):
    schema = luigi.Parameter(default='public')
    table = luigi.Parameter()

    def requires(self):
        pass

    def query(self, curs):
        pass

    def output(self):
        return PostgresTarget(self.schema, self.table, 'id', 'serial primary key')
