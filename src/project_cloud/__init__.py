__author__ = 'Efrei Students'
__version__ = '0.1.0'

from importlib.metadata import PackageNotFoundError, version

try:
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:
    __version__ = 'unknown'
finally:
    del version, PackageNotFoundError