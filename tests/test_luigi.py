import os
import subprocess
import tempfile
import textwrap

from click.testing import CliRunner
import yaml

from examplepipeline import cli
from examplepipeline.config import PostgresConfig


def test_luigi(postgres):
    fixture_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')
    with open(os.path.join(fixture_path, 'inventory.yml'), 'r') as f:
        raw_inventory = f.read()
    cleaned_inventory = raw_inventory.format(simple_filename=os.path.join(
        fixture_path, 'Simple Example.csv'))

    runner = CliRunner()
    config = PostgresConfig(**postgres.dsn())
    with tempfile.NamedTemporaryFile('wt') as conf_file, \
            tempfile.NamedTemporaryFile('wt') as ini_conf_file, \
            tempfile.NamedTemporaryFile('wt') as inv_file:
        yaml.dump({'postgres':
                      {key[2:].lower(): value for key, value in config.as_env_dict().items()}},
                  conf_file, default_flow_style=False)
        conf_file.flush()

        ini_conf_file.write(textwrap.dedent("""
        [postgres]
        host={host}
        user={user}
        database={database}
        port={port}
        """).format(**{key[2:].lower(): value for key, value in config.as_env_dict().items()}))
        ini_conf_file.flush()

        inv_file.write(cleaned_inventory)
        inv_file.flush()

        result = runner.invoke(cli.load_command,
                               ['--config', conf_file.name,
                                '--inventory', inv_file.name,
                                '--schema', 'raw_data',
                                '--verbose'])
        assert result.exit_code == 0

        env = os.environ.copy()
        env['LUIGI_CONFIG_PATH'] = ini_conf_file.name
        p = subprocess.Popen(['luigi', '--local-scheduler',
                              '--module', 'examplepipeline.luigi_features',
                              'RowKeyFeature'], env=env)
        assert p.wait() == 0

        p = subprocess.Popen(['luigi', '--local-scheduler',
                              '--module', 'examplepipeline.luigi_features',
                              'AddTwoFeature'], env=env)
        assert p.wait() == 0

        assert config.does_column_exist('simple_features', 'id', 'features')
        assert config.does_column_exist('simple_features', 'sw_plus_two', 'features')

        with config as conn, conn.cursor() as curs:
            curs.execute("SELECT id, sw_plus_two FROM features.simple_features")
            assert {(1, 3), (2, 7)} == set(curs.fetchall())
