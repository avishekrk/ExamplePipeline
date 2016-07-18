"""
This file is used to maintain a list of all features that are available
to the user to be created. By default, features are created in the order
they appear in this registry.
"""
from __future__ import absolute_import

import luigi

from . import luigi_features


class LuigiFeatureRegistry(luigi.Task):
    """ Register your features here in requires """

    def requires(self):
        return [luigi_features.RowKeyFeature(schema='features', table='features'),
                luigi_features.AddFeature(schema='features', table='features', to_add=2),
                luigi_features.RowKeyFeature(schema='features', table='labels'),
                luigi_features.AddFeature(schema='features', table='labels', to_add=5)]

    def output(self):
        pass

    def run(self):
        pass
