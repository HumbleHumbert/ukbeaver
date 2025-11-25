import polars as pl
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple
import os, re, requests
import warnings
from ukbeaver.util.schema import Schema

class Imaging:
    def __init__(
        self,
        img_dir: str,
    ) -> None:

        self.img_dir = img_dir

        # Global cache dir
        cache_dir = Path.home() / ".ukbeaver"
        self.buffer_path = cache_dir / "parquets"
        self.buffer_path.mkdir(exist_ok=True)


    def get_df(self,):


        img_pqt = self.buffer_path / "img_info.parquet"
        if img_pqt.exists():
            return pl.read_parquet(img_pqt)

        else:
            img_dir = self.img_dir

            filenames = [f for f in os.listdir(img_dir)]
            pattern = r"(?P<eid>\d+)_(?P<modality>\d+)_(?P<instance>\d+)_(?P<array>\d+)_(?:(?:\d+_)+)?(?P<specs>.+)$"

            df = (pl.DataFrame({"filename": filenames})
                .lazy()  # Use Lazy API for query optimization
                .with_columns([
                    # Construct full path (string concatenation is fast in Polars)
                    (pl.lit(img_dir) + "/" + pl.col("filename")).alias("file_path"),

                    # Extract metadata using regex
                    pl.col("filename").str.extract_groups(pattern).alias("meta")
                ])
                .unnest("meta")  # Unpack the struct created by regex into columns
                .with_columns([
                    # Cast numeric strings to Integers
                    pl.col("eid").cast(pl.Int64),
                    pl.col("modality").cast(pl.Int32),
                    pl.col("instance").cast(pl.Int32),
                    pl.col("array").cast(pl.Int32),
                    # specs remains string
                ])
                # --- LOGIC FOR DUPLICATES ---
                .unique(subset=["eid", "modality", "instance", "array", "specs"], keep="first")
                .collect() # Execute
            )

            df.write_parquet(img_pqt)

            return df
