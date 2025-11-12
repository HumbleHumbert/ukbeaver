# Optional: Define package version (or use __version__ from pyproject.toml if dynamic)
__version__ = "0.1.0"  # Replace with your version

# Expose subpackages or key modules for easy import
from .data import imaging  # Assuming you want to expose functions from imaging.py
from .data import tabular
from .util import schema
from .util import category
# If 'statis' has content, e.g., from .statis import some_function

# Optional: Define __all__ to control what 'from ukbeaver import *' imports
__all__ = ['imaging', 'tabular', 'schema', 'category']  # List public names here
