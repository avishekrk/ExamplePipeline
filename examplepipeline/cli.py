import datetime
import os
import re
import sys

import click
import pandas as pd
import yaml
from six.moves.configparser import ConfigParser

from .config import PostgresConfig


@click.group()
def cli():
    """Run your pipeline"""
    pass


def basic_cleaning(to_clean):
    cleaned = to_clean.strip()
    cleaned = re.findall(r'^\d*(.*)$', cleaned)[0]
    cleaned = re.sub(r'[\s.]+', '_', cleaned)
    cleaned = re.sub(r'[\[\]\(\)]', '', cleaned)
    return cleaned


@cli.command('load')
@click.option('--config', '-c', nargs=1,
              type=click.Path(exists=True, dir_okay=False, file_okay=True),
              help="The configuration file")
@click.option('--inventory', '-i', nargs=1,
              type=click.Path(exists=True, dir_okay=False, file_okay=True),
              help="An inventory of files to upload")
@click.option('--schema', '-s', nargs=1, default='raw_data',
              help="The schema to load the tables into. Default is 'raw_data'")
@click.option('--resume', '-r', is_flag=True, default=False,
              help="Set this flag if you want to reload *only* those tables in the "
                   "inventory which don't currently exists")
@click.option('--verbose', '-v', is_flag=True, default=False,
              help="More verbose output")
def load_command(config, inventory, schema, resume, verbose):
    """Load data from csvs into the postgres database.

    Also does some basic cleaning (e.g., lower casing names and removing spaces).
    You need to specify a config file, e.g.::

        [postgres]
        host=<SOME HOST>
        port=<SOME PORT>
        user=<SOME USER>
        database=<SOME DATABASE>
        password=<SOME PASSWORD>

    and an inventory of files to upload. The inventory has the format::

        inventory:
          - file_name: file_to_upload.csv
            table_name: what_to_name_the_table
            column_map:
              "original column name":
                name: new_column_name
                type: date|some_python_type

    All options *except* the `file_name` are optional, and if not specified
    will be filled in with defaults. E.g., by default, table names and columns
    get cleaned into lower_snake_case and the type is whatever pandas guesses
    it is after 100 rows. (Note in particular that pandas usually doesn't detect
    dates very well from csvs.)

    WARNING: If you run this, it will drop all the tables that already exist
    in the schema with the given names **unless** you call with `--resume`.
    """
    if config.endswith('.yaml') or config.endswith('.yml'):
        with open(config, 'r') as f:
            config = yaml.load(f)
            postgres_config = PostgresConfig(**config['postgres'])
    elif config.endswith('.ini') or config.endswith('.cfg'):
        config = ConfigParser()
        config.read([config])
        postgres_config = PostgresConfig(luigi_config=config)
    else:
        raise click.BadParameter("--config must be either a yaml file or an ini file")

    with open(inventory, 'r') as f:
        inventory = yaml.load(f)
    inventory = inventory['inventory']

    if verbose:
        click.echo("Found {} tables to upload".format(len(inventory)))
        click.echo("Verifying existence of inputs.")

    do_not_exist = [desc['file_name'] for desc in inventory
                    if not os.path.isfile(desc['file_name'])]
    if do_not_exist:
        click.echo("The following files in the inventory don't exist: "
                   "{}".format(','.join(do_not_exist)))
        click.echo("Please fix and try again.")
        sys.exit(1)

    postgres_config = PostgresConfig(**config['postgres'])

    for file_description in inventory:
        file_name = file_description['file_name']
        basic_file_name = os.path.basename(file_name).rsplit('.', 1)[0]
        table_name = file_description.get('table_name') or basic_cleaning(basic_file_name)

        if resume and postgres_config.does_table_exist(schema=schema, table=table_name):
            click.echo("Table {}.{} exists. Skipping.".format(schema, table_name))
            continue

        column_map = file_description.get('column_map', {})
        df = pd.read_csv(file_name, nrows=100)

        # Redo types
        for column_name, value in column_map.items():
            new_type = value['type']
            if new_type == 'date':
                df[column_name] = df[column_name].apply(pd.Timestamp)
            elif new_type.startswith('date'):
                fmt_str = re.findall(r'^date\((.*)\)$', new_type)[0]
                df[column_name] = df[column_name].apply(
                    lambda x: datetime.datetime.strftime(x, fmt_str))
            else:
                df[column_name] = df[column_name].astype(new_type)
        click.echo("Column map " + str(column_map))
        renamed_columns = {}
        for column_name in df.columns:
            if column_name in column_map and 'name' in column_map[column_name]:
                renamed_columns[column_name] = column_map[column_name]['name']
            else:
                renamed_columns[column_name] = basic_cleaning(column_name)
        df = df.rename(columns=renamed_columns)
        df = df.reset_index()
        index_column_name = df.columns[0]
        df = df.rename(columns={index_column_name: 'id'})

        sql_statement = pd.io.sql.get_schema(df, 'REPLACE ME')
        sql_statement = sql_statement.replace('"REPLACE ME"', '"{}"."{}"'.format(schema, table_name))
        sql_statement = sql_statement.replace('"id" INTEGER', '"id" SERIAL PRIMARY KEY')

        if verbose:
            click.echo("Creating table with statement:")
            click.echo(sql_statement)

        postgres_config.create_and_drop(table=table_name, create_statement=sql_statement,
                                       schema=schema)
        copy_statement = (
            "\copy \"{schema}\".\"{table_name}\" ({column_names}) FROM '{file_name}' "
            "WITH CSV HEADER".format(
                schema=schema,
                table_name=table_name,
                column_names=','.join(df.columns[1:]),
                file_name=file_name))
        postgres_config.execute_in_psql(copy_statement)


if __name__ == '__main__':
    cli()
