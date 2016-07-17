import datetime
import os
import re
import sys

import click
import pandas as pd
import yaml

from .config import PostgresConfig


@click.group()
def cli():
    """Run your pipeline"""
    pass


def basic_cleaning(to_clean):
    cleaned = re.findall(r'^\d*(.*)$', to_clean)[0]
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
    with open(config, 'r') as f:
        config = yaml.load(f)

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
    click.echo(postgres_config.as_env_dict())

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
        renamed_columns = {column_name: column_map.get('column_name') or
                              basic_cleaning(column_name)}
        df = df.rename(columns=renamed_columns)
        df = df.reset_index()
        index_column_name = df.columns[0]
        df = df.rename(columns={index_column_name: 'id'})

        sql_statement = pd.io.sql.get_schema(df, '{}.{}'.format(schema, table_name))
        sql_statement = sql_statement.replace('"id" INTEGER', '"id" INTEGER PRIMARY KEY')

        if verbose:
            click.echo("Creating table with statement:")
            click.echo(sql_statement)

        postgres_config.create_and_drop(table=table_name, create_statement=sql_statement,
                                       schema=schema)
        postgres_config.execute_in_psql(
            "\copy {table_name} ({column_names}) FROM '{file_name}' "
            "WITH FORMAT CSV HEADER".format(
                table_name=table_name,
                column_names=','.join(df.columns),
                file_name=file_name))


if __name__ == '__main__':
    cli()
