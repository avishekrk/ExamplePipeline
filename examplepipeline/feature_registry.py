"""
This file is used to maintain a list of all features that are available
to the user to be created. By default, features are created in the order
they appear in this registry.
"""

from .features.simple_features import RowKeyFeature, AddTwoFeature

FEATURE_REGISTRY = (RowKeyFeature(), AddTwoFeature())
