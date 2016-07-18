from .luigi import PostgresColumnTask, PostgresTarget


class RowKeyFeature(PostgresColumnTask):

    def requires(self):
        pass

    def query(self, curs):
        curs.execute("""
            INSERT INTO features.simple_features (id)
            SELECT id FROM raw_data.simple_example;
        """)

    def output(self):
        return PostgresTarget('features', 'simple_features', 'id', 'integer')


class AddTwoFeature(PostgresColumnTask):

    def requires(self):
        return [RowKeyFeature()]

    def query(self, curs):
        curs.execute("""UPDATE features.simple_features sf
                           SET sw_plus_two = raw.something_wicked_29 + 2
                          FROM raw_data.simple_example raw
                         WHERE raw.id = sf.id;""")

    def output(self):
        return PostgresTarget('features', 'simple_features', 'sw_plus_two', 'integer')
