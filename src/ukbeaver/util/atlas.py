import polars as pl
from pathlib import Path
from polars.selectors import ends_with
import os,  requests
import warnings

class FSLAtlas:
    def __init__(
        self,
    ) -> None:
        # Global cache dir
        cache_dir = Path.home() / ".ukbeaver"
        self.atlas_cache = cache_dir / "atlas"
        self.atlas_cache.mkdir(exist_ok=True)


        self.raw_url = "https://git.fmrib.ox.ac.uk/fsl/data_standard/-/raw/master/"
        self.api_url = "https://git.fmrib.ox.ac.uk/api/v4/projects/fsl%2Fdata_standard/repository/tree"

    def get_list(self, ):
        params = {
            "ref": "master",
            "per_page": 100,  # Get up to 100 files
            "recursive": False
        }
        response = requests.get(self.api_url, params, timeout=10)
        data = response.json()

        atlases = [
            item['name'] for item in data
            if item['type'] == 'blob' and item['name'].endswith('.nii.gz')
        ]

        return atlases

    def get_atlas(self, filename):
        if not filename.endswith('.nii.gz'):
            filename += '.nii.gz'

        local_path = self.atlas_cache / filename

        if local_path.exists():
            return str(local_path)
        else:
            with open(local_path, "wb") as f:
                f.write(requests.get(self.raw_url + filename).content)
            return str(local_path)
