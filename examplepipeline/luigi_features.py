from __future__ import absolute_import

import luigi

from .luigi import PostgresColumnTask, PostgresTarget


class RowKeyFeature(PostgresColumnTask):
    schema = luigi.Parameter(default='public')
    table = luigi.Parameter()

    def requires(self):
        pass

    def query(self, curs):
        curs.execute("""
            INSERT INTO "{}"."{}" (id)
            SELECT id FROM raw_data.simple_example;
        """.format(self.schema, self.table))

    def output(self):
        return PostgresTarget(self.schema, self.table, 'id', 'integer primary key')


class AddFeature(PostgresColumnTask):

    to_add = luigi.Parameter(default=2)

    schema = luigi.Parameter(default='public')
    table = luigi.Parameter()

    def requires(self):
        return [RowKeyFeature(schema=self.schema, table=self.table)]

    def query(self, curs):
        curs.execute("""UPDATE "{}"."{}" sf
                           SET sw_plus_two = raw.something_wicked_29 + {}
                          FROM raw_data.simple_example raw
                         WHERE raw.id = sf.id;""".format(self.schema, self.table, self.to_add))

    def output(self):
        return PostgresTarget(self.schema, self.table, 'sw_plus_two', 'integer')
