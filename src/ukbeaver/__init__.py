import importlib.metadata

try:
    # Reads the version defined in pyproject.toml or git tag
    __version__ = importlib.metadata.version("ukbeaver")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"


# Expose subpackages or key modules for easy import
from .data import imaging  # Assuming you want to expose functions from imaging.py
from .data import tabular
from .util import schema
from .util import category
# If 'statis' has content, e.g., from .statis import some_function

# Optional: Define __all__ to control what 'from ukbeaver import *' imports
__all__ = ['imaging', 'tabular', 'schema', 'category']  # List public names here
