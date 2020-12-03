# indi_aws/__init__.py
#
# Contibuting authors (please append):
# Daniel Clark
# Cameron Craddock
# Jon Clucas
'''
The awsutils package contains various utilities developed for
interaction with the AWS cloud
'''
try:
    from importlib import metadata
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata as metadata

__version__ = metadata.version('INDI-tools')
