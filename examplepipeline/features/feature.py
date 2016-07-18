import abc

from .. import config as config_mod


class Feature(object):
    """To make a feature, override requires, query, and output"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, config=None):
        self.config = config

    def set_config(self, config):
        self.config = config

    @abc.abstractmethod
    def name(self):
        """A human readable name for this feature"""
        pass

    @abc.abstractmethod
    def requires(self):
        """This should return a list of columns in the format::

            (schema_name, table_name, column_name)

        Will be used to determine if the task can be completed.

        :return: The list of columns needed to complete the task
        :rtype: list[(str, str, str)]
        """
        pass

    @abc.abstractmethod
    def query(self, curs):
        """
        This should be a general query. You should populate the columns that
        are returned in `output`. You get a cursor and can do what you will with
        it (including, e.g., bringing data down to the box and doing things with
        pandas).

        :param psycopg2.Cursor curs: A cursor connected to the database
        """
        pass

    @abc.abstractmethod
    def output(self):
        """
        This should return a list of columns in the format::

            (schema_name, table_name, column_name, type)

        Calling `run` will create these columns (and drop them if they exist) before
        `query` is called.

        :return: The list of columns this feature creates
        :rtype: list[(str, str, str, str)]
        """
        pass

    def ready(self):
        """
        :return: Whether the feature is ready to be generated
        :rtype: bool
        """
        return all(self.config.does_column_exist(table, column, schema=schema)
                   for schema, table, column in self.requires())

    def done(self):
        """
        :return: Whether the feature has been generated
        :rtype: bool
        """
        return all(self.config.does_column_exist(table, column, schema=schema)
                   for schema, table, column, _ in self.output())

    def run(self):
        """Create the feature, destroying previous computation if necessary"""
        with self.config as conn:
            with conn.cursor() as curs:
                for schema, table, column, the_type in self.output():
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
