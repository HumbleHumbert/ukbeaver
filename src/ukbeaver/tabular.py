import polars as pl
import pandas as pd
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any
import re, requests

class Phenotype:
    def __init__(
        self,
        pheno_table: str,
    ) -> None:
        self.pheno_table = pheno_table.strip()
        self.supplement_table = 'dictionaries/field.txt'

        # Global cache dir
        cache_dir = Path.home() / ".ukbeaver"
        cache_dir.mkdir(exist_ok=True)
        self.supplement_table = cache_dir / "field.txt"

        if not self.supplement_table.exists():
            url = "https://biobank.ndph.ox.ac.uk/ukb/scdown.cgi?fmt=txt&id=1"
            with open(self.supplement_table, "wb") as f:
                f.write(requests.get(url).content)
            print(f"Downloaded field dictionary to {self.supplement_table}")


    def get_datatype(self) -> Dict[str, pl.DataType]:
        # mapping value_type → Polars dtype (stays the same)
        value_type_map = {
            11: pl.Int64,
            21: pl.Categorical,
            22: pl.Categorical,
            31: pl.Float64,
            41: pl.Utf8,
            51: pl.Datetime("ns"),
            61: pl.Utf8,
            101: pl.Utf8,
            201: pl.Utf8,
        }

        field_table = pd.read_csv(self.supplement_table, sep='\t')
        # %%
        income_field_name = pl.scan_csv(self.pheno_table, separator='\t').collect_schema().names()
        # %%
        income_field_id = [m[0] if (m := re.findall(r'\d+', x)) else None for x in income_field_name]
        # %%

        # %%
        df_codes = pd.DataFrame({
            "field_name": income_field_name,
            "field_id": income_field_id
        })
        # unify the dtypes before merge
        df_codes["field_id"] = df_codes["field_id"].astype("Int64")  # nullable integer
        field_table["field_id"] = field_table["field_id"].astype("Int64")

        income_dtype_table = df_codes.merge(field_table, how='left', on='field_id')
        # %%
        income_dtype_table['dtype'] = income_dtype_table['value_type'].map(value_type_map)
        # %%
        dtype_map = dict(zip(income_dtype_table["field_name"], income_dtype_table["dtype"]))

        # always include eid
        dtype_map["eid"] = pl.Utf8

        return dtype_map

    def get_df(self, fids: Optional[list[str]] = None, ins: Optional[int] = None) -> tuple[Any, dict[Any, Any]]:
        df = pl.scan_csv(
            self.pheno_table,
            separator="\t",
            schema_overrides=self.get_datatype(),
            ignore_errors=True,
        )

        # Always keep eid
        income_field_name = df.collect_schema().names()
        must_keep = {"eid"}

        if fids:
            filtered_cols = set()
            for field_id in fids:
                # --- Step 1: Broadly select ALL columns related to the Field ID ---
                broad_pattern = re.compile(rf"^p{field_id}(_[ia]\d+.*)?$")
                all_related_cols = [col for col in income_field_name if broad_pattern.match(col)]
                filtered_cols.update(all_related_cols)
            filtered_cols.update(must_keep)  # ensure eid included
            df = df.select(list(filtered_cols))

        if ins:
            instance_substring = f"_i{ins}"
            filtered_cols = set()
            for col in df.collect_schema().names():
                # Keep the column if it contains the target instance substring
                if instance_substring in col:
                    filtered_cols.add(col)
                # Also keep it if it's a non-instanced field AND the target instance is 0
                elif "_i" not in col:
                    filtered_cols.add(col)

            filtered_cols.update(must_keep)  # ensure eid included
            if filtered_cols:
                df = df.select(list(filtered_cols))

        # get field id map
        field_map = defaultdict(list)
        # This regex captures the numeric part of the field ID
        id_extractor = re.compile(r"^p(\d+)")

        for col_name in df.collect_schema().names():
            if col_name == 'eid':
                continue

            match = id_extractor.match(col_name)
            if match:
                # The first captured group is the number
                field_id = int(match.group(1))
                field_map[field_id].append(col_name)

        return df.collect(), dict(field_map)

