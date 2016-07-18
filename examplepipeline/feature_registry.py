"""
This file is used to maintain a list of all features that are available
to the user to be created. By default, features are created in the order
they appear in this registry.
"""
from __future__ import absolute_import

import luigi

from . import luigi_features
from .features import simple_features

FEATURE_REGISTRY = (simple_features.RowKeyFeature(), simple_features.AddTwoFeature())

class LuigiFeatureRegistry(luigi.Task):
    """ Register your classes here in requires """

    def requires(self):
        return [luigi_features.RowKeyFeature(), luigi_features.AddTwoFeature()]

    def output(self):
        pass

    def run(self):
        pass
