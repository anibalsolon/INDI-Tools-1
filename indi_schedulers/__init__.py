# schedulers/__init__.py
#
# Contibuting authors (please append):
# Daniel Clark, 2016
# Jon Clucas, 2020

'''
The schedulers package contains various utilities developed for
managing batch job submissions to cluster schedulers
'''
# Import packages
from . import cluster_templates
try:
    from importlib import metadata
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata as metadata

__version__ = metadata.version('INDI-tools')
__all__ = [
    'cluster_templates',
    '__version__'
]
