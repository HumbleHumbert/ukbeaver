import polars as pl
from pathlib import Path
import os,  requests
import warnings

class Schema:
    def __init__(
        self,
    ) -> None:
        # Global cache dir
        cache_dir = Path.home() / ".ukbeaver"
        self.dict_path = cache_dir / "schemas"
        self.dict_path.mkdir(exist_ok=True, parents=True)

        urls = [
            "https://biobank.ndph.ox.ac.uk/ukb/scdown.cgi?fmt=txt&id=1",
            "https://biobank.ndph.ox.ac.uk/ukb/scdown.cgi?fmt=txt&id=3",
            "https://biobank.ndph.ox.ac.uk/ukb/scdown.cgi?fmt=txt&id=13",
        ]
        if not os.listdir(self.dict_path):
            for url in urls:
                schema_id = url.split("id=")[-1]
                file_name = f"schema_{schema_id}.txt"
                with open(self.dict_path / file_name, "wb") as f:
                    f.write(requests.get(url).content)
                print(f"Downloaded field dictionary to {self.dict_path}")

        self.schema_1 = self.dict_path / "schema_1.txt"
        self.schema_3 = self.dict_path / "schema_3.txt"
        self.schema_13 = self.dict_path / "schema_13.txt"


    def get_schema(self, id):

        if id == 1:
            # Prepare the schema 1
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="CSV malformed")
                table = pl.read_csv(
                    self.schema_1,
                    separator="\t",
                    ignore_errors=True
                )
        else:
            table = pl.read_csv(
                getattr(self, f'schema_{id}'), separator='\t',encoding='latin1'
            )

        return table
