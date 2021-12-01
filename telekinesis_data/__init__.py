from .distributed import Dist, Branch

from pkg_resources import get_distribution

__version__ = get_distribution(__name__).version

__all__ = [
  "__version__",
  "Dist",
  "Branch"
]