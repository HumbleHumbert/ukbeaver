import polars as pl
from polars import Int64, Float64, Utf8, Datetime, Categorical
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple
import os, re, requests
import warnings
from ukbeaver.util.schema import Schema

class Phenotype:
    def __init__(
        self,
        pheno_table: Path,
    ) -> None:

        sa = Schema()
        self.table_1 = sa.get_schema(1)
        self.table_3 = sa.get_schema(3)
        self.table_13 = sa.get_schema(13)

        self.pheno_table = pheno_table

    def get_datatype(self) -> tuple[
        (Dict[str, pl.DataType], List[str])
    ]:
        # mapping value_type → Polars dtype (stays the same)
        value_type_map = {
            11: Int64,
            21: Utf8,       # Choice
            22: Utf8,       # Choice
            31: Float64,
            41: Utf8,
            51: Datetime,
            61: Utf8,
            101: Utf8,
            201: Utf8,
        }

        field_property = self.table_1.select(["field_id", "value_type"])

        income_field_name = pl.scan_csv(self.pheno_table, separator='\t').collect_schema().names()
        income_field_id = [m[0] if (m := re.findall(r'\d+', x)) else None for x in income_field_name]


        income_table = pl.DataFrame(
            {
                "field_name": income_field_name,
                "field_id": income_field_id,
            },
        )
        # unify the dtypes before merge
        income_table = income_table.with_columns(pl.col("field_id").cast(pl.Int64))
        income_dtype_table = income_table.join(field_property, on="field_id")

        dtype_list = [value_type_map.get(v, Utf8) for v in income_dtype_table['value_type']]
        dtype_map = dict(zip(income_dtype_table['field_name'], dtype_list))

        # always include eid
        dtype_map["eid"] = Utf8

        # prepare the Categorical
        categorical_fields = [
            n for n, v in zip(income_dtype_table['field_name'], income_dtype_table['value_type'])
    if v in (21, 22)
]

        return dtype_map, categorical_fields

    def get_df(self, fids: Optional[list[str]] = None, ins: Optional[str] = None) -> tuple[pl.DataFrame, dict[Any, Any]]:

        dtype_map, categorical_fields = self.get_datatype()
        missing_strings = ['Do not know', 'Prefer not to answer', ]

        df = pl.scan_csv(
            self.pheno_table,
            separator="\t",
            schema_overrides=dtype_map,
            ignore_errors=True,
            null_values=missing_strings
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

        df = df.collect()

        current_cols = set(df.columns)
        for col in categorical_fields:
            if col in current_cols:
                df = df.with_columns(
                    pl.col(col).cast(pl.Categorical)
                )

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

        return df, dict(field_map)

