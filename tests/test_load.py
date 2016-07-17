from __future__ import print_function

import os
import tempfile

from click.testing import CliRunner
import yaml

from examplepipeline import cli
from examplepipeline.config import PostgresConfig


def test_load(postgres):
    fixture_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')
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

    config.does_table_exist(schema='raw_data', table='simple_example_no_spaces')
    config.does_table_exist(schema='raw_data', table='simple_example')
