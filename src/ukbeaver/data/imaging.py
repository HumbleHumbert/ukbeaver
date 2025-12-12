import os
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Union, Optional
import polars as pl

class Imaging:
    def __init__(
        self,
        # Now accepts a list of paths or a dict {name: path}
        img_dirs: Union[List[str], Dict[str, str]], 
    ) -> None:

        # Normalize input to a dict for easier handling
        if isinstance(img_dirs, list):
            self.img_map = {str(p): str(p) for p in img_dirs}
        else:
            self.img_map = img_dirs

        # Regex Pattern
        self.pattern = r"(?P<eid>\d+)_(?P<modality>\d+)_(?P<instance>\d+)_(?P<array>\d+)_(?:(?:\d+_)+)?(?P<specs>.+)$"

        # Global cache dir
        cache_base = Path.home() / ".ukbeaver"
        self.buffer_path = cache_base / "parquets"
        self.buffer_path.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, dir_path: str) -> Path:
        """Generates a unique cache filename based on the directory path hash."""
        # We hash the path so we don't get filename conflicts
        path_hash = hashlib.md5(dir_path.encode('utf-8')).hexdigest()
        # You might want to keep a short alias in the name for debuggability
        safe_name = Path(dir_path).name[-10:]
        return self.buffer_path / f"cache_{safe_name}_{path_hash}.parquet"

    def _scan_directory(self, dir_path: str) -> pl.DataFrame:
        """Scans a single directory and returns the processed DataFrame."""
        print(f"Scanning directory: {dir_path} ...")

        # Use os.scandir for better performance on large directories than os.walk
        # if you only need the top level. If recursive, walk is fine.
        filenames = [f for _, _, files in os.walk(dir_path) for f in files]

        if not filenames:
            return pl.DataFrame()

        q = (
            pl.DataFrame({"filename": filenames})
            .lazy()
            .with_columns([
                (pl.lit(dir_path) + "/" + pl.col("filename")).alias("file_path"),
                pl.col("filename").str.extract_groups(self.pattern).alias("meta")
            ])
            .unnest("meta")
            .with_columns([
                pl.col("eid").cast(pl.Int64),
                pl.col("modality").cast(pl.Int32),
                pl.col("instance").cast(pl.Int32),
                pl.col("array").cast(pl.Int32),
            ])
            # Logic for duplicates within this specific folder
            .unique(subset=["eid", "modality", "instance", "array", "specs"], keep="first")
        )
        return q.collect()

    def get_df(self, refresh_dirs: Optional[List[str]] = None, force_all: bool = False) -> pl.DataFrame:
        """
        Args:
            refresh_dirs: List of specific directory paths (keys) to force re-scan.
            force_all: If True, ignore all caches and re-scan everything.
        """
        dfs = []
        refresh_set = set(refresh_dirs) if refresh_dirs else set()

        for name, dir_path in self.img_map.items():
            cache_file = self._get_cache_path(dir_path)

            # --- INTELLIGENT CACHING LOGIC ---
            should_scan = False

            if force_all or (name in refresh_set) or (dir_path in refresh_set):
                should_scan = True
            elif not cache_file.exists():
                should_scan = True
            else:
                # OPTIONAL: Check modification times (mtime)
                # If directory was modified AFTER the parquet file, trigger a scan.
                # Note: On very large folders, checking mtime can be slightly expensive,
                # but usually negligible compared to parsing filenames.
                dir_mtime = Path(dir_path).stat().st_mtime
                cache_mtime = cache_file.stat().st_mtime
                if dir_mtime > cache_mtime:
                    print(f"Detect changes in {name}, reloading...")
                    should_scan = True

            # --- EXECUTION ---
            if should_scan:
                df_part = self._scan_directory(dir_path)
                if not df_part.is_empty():
                    df_part.write_parquet(cache_file)
                    dfs.append(df_part)
            else:
                # Load from cache
                dfs.append(pl.read_parquet(cache_file))

        if not dfs:
            return pl.DataFrame() # Return empty if nothing found anywhere

        # Concatenate all partial DataFrames
        # Vertical concatenation is very fast in Polars
        final_df = pl.concat(dfs, how="vertical")

        # Optional: Global unique check if duplicates exist across different folders
        final_df = final_df.unique(subset=["eid", "modality", "instance", "array", "specs"], keep="first")

        return final_df
