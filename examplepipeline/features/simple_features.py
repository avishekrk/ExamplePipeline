from .feature import Feature


class RowKeyFeature(Feature):
    def name(self):
        return 'Row Key'

    def requires(self):
        return [('raw_data', 'simple_example', 'id')]

    def output(self):
        return [('features', 'simple_features', 'id', 'INTEGER')]

    def query(self, curs):
        curs.execute("""
            INSERT INTO features.simple_features (id)
            SELECT id FROM raw_data.simple_example;
        """)


class AddTwoFeature(Feature):

    def name(self):
        return 'Add Two'

    def requires(self):
        return [('raw_data', 'simple_example', 'id'),
                ('raw_data', 'simple_example', 'something_wicked_29')]

    def output(self):
        return [('features', 'simple_features', 'sw_plus_two', 'INTEGER')]

    def query(self, curs):
        curs.execute("""UPDATE features.simple_features sf
                           SET sw_plus_two = raw.something_wicked_29 + 2
                          FROM raw_data.simple_example raw
                         WHERE raw.id = sf.id;""")
