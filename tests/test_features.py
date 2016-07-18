
from __future__ import print_function
import traceback

import os
import tempfile

from click.testing import CliRunner
import yaml

from examplepipeline import cli
from examplepipeline.config import PostgresConfig

def test_feature_registry(postgres):
    fixture_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')

    # Load the simple example into postgres
    with open(os.path.join(fixture_path, 'inventory.yml'), 'r') as f:
        raw_inventory = f.read()
    cleaned_inventory = raw_inventory.format(simple_filename=os.path.join(
        fixture_path, 'Simple Example.csv'))

    runner = CliRunner()
    config = PostgresConfig(**postgres.dsn())
    with tempfile.NamedTemporaryFile('wt') as conf_file, \
            tempfile.NamedTemporaryFile('wt') as inv_file:
        yaml.dump({'postgres':
                      {key[2:].lower(): value for key, value in config.as_env_dict().items()}},
                  conf_file, default_flow_style=False)
        conf_file.flush()

        inv_file.write(cleaned_inventory)
        inv_file.flush()

        result = runner.invoke(cli.load_command,
                               ['--config', conf_file.name,
                                '--inventory', inv_file.name,
                                '--schema', 'raw_data',
                                '--verbose'])
        assert result.exit_code == 0

        result = runner.invoke(cli.features_command,
                               ['--config', conf_file.name])
        if result.exit_code != 0:
            traceback.print_tb(result.exc_info[2])
            print(result.output)
            assert False

        assert config.does_column_exist('simple_features', 'id', 'features')
        assert config.does_column_exist('simple_features', 'sw_plus_two', 'features')

        with config as conn, conn.cursor() as curs:
            curs.execute("SELECT id, sw_plus_two FROM features.simple_features")
            assert {(1, 3), (2, 7)} == set(curs.fetchall())

        result = runner.invoke(cli.features_command,
                               ['--config', conf_file.name,
                                '--resume'])
        assert result.exit_code == 0
        assert 'Skipping' in result.output
