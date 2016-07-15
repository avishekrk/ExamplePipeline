from collections import namedtuple


class PostgresConfig(object):
    def __init__(self, host=None, port=5432, database=None, user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def __enter__(self):
        self.connection = psycopg2.connect(host=self.host,
                                           self.port=port,
                                           database=self.database,
                                           user=self.user,
                                           password=self.password)
        return self.connection.__enter__()

    def __exit__(self, *args):
        self.connection.__exit__(self, *args)
