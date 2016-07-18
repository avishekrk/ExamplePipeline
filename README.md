# ExamplePipeline

This is a simple example of a pipeline that takes data from a bunch of CSVs
according to an inventory, uploads them to Postgres according to a config,
and then allows you to run various luigi tasks on top of that.

## Installation

The easiest installation is into a virtualenv and then to `pip install .`.

## Basic layout

This pipeline is divided into two parts: the part that loads csvs into a
postgres database, and then a luigi pipeline that operates on them to
create features, labels, and run models.

For the first part, you'll create an inventory of csvs to upload and basic
modifications to make to them. If you want to do further cleaning, you should
either set that up in a luigi pipeline or modify the `load` function in
`examplepipeline.cli`.

For the second part, you can add features in `examplepipeline.luigi_features.py`
and then register them in `examplepipeline.feature_registry.py`. By registering
your features, it will be easy to run them all at once.

## How to run

Currently, it runs through tests. The most complete run can be found in
`tests/test_luigi.py`. Most of the work going on there is to setup a
running postgres instance that you can test with. But if you want to
try yourself, do the following:

### Create a config

You'll need a config file called `luigi.cfg` in your working directory. It should look like:

```
[postgres]
host=<SOMEHOST>
port=<SOMEPORT>
user=<SOMEUSER>
database=<SOMEDATABASE>
password=<SOMEPASSWORD>
```

### Gather your csvs

Put all your csvs in one place. A simple example may be found in `tests/fixtures/Simple Example.csv`.

### Create an inventory of your csvs

You'll need to create an inventory of the csvs you want to load. This is a yaml file that
looks like:

```yaml
inventory:
  - file_name: file_to_upload.csv
    table_name: what_to_name_the_table
    column_map:
      "original column name":
        name: new_column_name
        type: date|some_python_type
```

All options *except* the `file_name` are optional, and if not specified
will be filled in with defaults. E.g., by default, table names and columns
get cleaned into lower_snake_case and the type is whatever pandas guesses
it is after 100 rows. (Note in particular that pandas usually doesn't detect
dates very well from csvs.)

### Load your csvs

To load your csvs, simply run

```
pipeline --config luigi.cfg --inventory inventory.yml --schema <YOUR_SCHEMA>
```

### Run luigi

After this, you may do what you want with luigi. If you uploaded what's in the `tests/fixtures`
folder, then there's an example already setup in `examplepipeline.feature_registry.py`. You
can run this with

```
luigi --local-scheduler --module examplepipeline.feature_registry LuigiFeatureRegistry
```

### Adding on

Try adding some csvs to the inventory or some features to the feature registry. See what
happens!
