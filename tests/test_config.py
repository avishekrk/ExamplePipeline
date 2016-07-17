from examplepipeline.config import PostgresConfig


def test_as_env_dict(postgres):
    config = PostgresConfig(**postgres.dsn())
    should_be_keys = {'PGHOST', 'PGUSER', 'PGPORT', 'PGDATABASE', 'PGPASSWORD'}
    assert set(config.as_env_dict()) <= should_be_keys
    for key in (should_be_keys - set(config.as_env_dict())):
        assert not getattr(config, key[2:].lower())


def test_context_manager(postgres):
    config = PostgresConfig(**postgres.dsn())
    with config as conn, conn.cursor() as curs:
        curs.execute("CREATE SCHEMA hello;")


def test_does_table_and_schema_exist(postgres):
    config = PostgresConfig(**postgres.dsn())
    with config as conn, conn.cursor() as curs:
        curs.execute("CREATE SCHEMA dte;")
        curs.execute("CREATE TABLE dte.my_table (col1 INTEGER, col2 INTEGER);")

    assert config.does_schema_exist('dte')
    assert not config.does_schema_exist('not_dte')
    assert config.does_table_exist(table='my_table', schema='dte')
    assert not config.does_table_exist(table='your_table', schema='dte')


def test_create_and_drop(postgres):
    config = PostgresConfig(**postgres.dsn())
    config.create_and_drop(schema='cad', table='my_table', create_statement="""
        CREATE TABLE cad.my_table (col1 TEXT);""")
    assert config.does_schema_exist('cad')
    assert config.does_table_exist(schema='cad', table='my_table')

    config.create_and_drop(schema='cad', table='my_table', create_statement="""
        CREATE TABLE cad.my_table (col1 TEXT);""")
    assert config.does_schema_exist('cad')
    assert config.does_table_exist(schema='cad', table='my_table')


def test_execute_in_psql(postgres):
    config = PostgresConfig(**postgres.dsn())
    statement = """CREATE SCHEMA eis; CREATE TABLE eis.my_table (col1 INTEGER);"""
    config.execute_in_psql(statement)
    assert config.does_schema_exist('eis')
    assert config.does_table_exist('my_table', 'eis')
