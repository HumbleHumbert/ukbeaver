import polars as pl
from polars import Int64, Float64, Utf8, Date, Categorical
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
            21: Categorical,       # Choice
            22: Categorical,       # Choice
            31: Float64,
            41: Utf8,
            51: Date,
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
        dtype_map["eid"] = Int64

        # prepare the Categorical
        categorical_fields = [
            n for n, v in zip(income_dtype_table['field_name'], income_dtype_table['value_type'])
    if v in (21, 22)
]

        return dtype_map, categorical_fields

    def get_df(self, fids: Optional[list[str]] = None, ins: Optional[str] = None) -> tuple[pl.DataFrame, dict[Any, Any]]:

        dtype_map, categorical_fields = self.get_datatype()
        missing_strings = ['Do not know', 'Prefer not to answer', 'null']

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

    def get_dummies(self, df: pl.DataFrame):

        cat_cols = [c for c,dtype in zip(df.columns, df.dtypes) if dtype == pl.Categorical]
        df_dummies = df.to_dummies(columns=cat_cols, drop_nulls=True)

        for col in cat_cols:
            # Get all dummy columns for this categorical variable
            dummy_cols = [
                c for c in df_dummies.columns if c.startswith(f"{col}_")           ]
            if dummy_cols:
                # Sort alphabetically to ensure consistent selection
                dummy_cols_sorted = sorted(dummy_cols)

                # Drop the first dummy column (alphabetically)
                col_to_drop = dummy_cols_sorted[0]
                df_dummies = df_dummies.drop(col_to_drop)

                # print(f"Dropped first dummy for {col}: {col_to_drop}")

        return df_dummies

    def get_icd_dates(self,):
        df, _ = self.get_df(fids=['41270', '41280'])

        # --- Step A: Process the Dates (Unpivot) ---
        q_dates = (
            df.select(pl.col("eid"), pl.col("^p41280_a.*$")) # Select only EID and date cols
            .unpivot(index="eid", variable_name="col_name", value_name="date")
            .filter(pl.col("date").is_not_null()) # Remove empty dates to save memory
            .with_columns(
                # Extract the trailing number from 'p41280_a0' -> 0
                pl.col("col_name").str.extract(r"(\d+)$").cast(pl.Int32).alias("idx")
            )
        )

        # --- Step B: Process the ICD Codes (Explode) ---
        q_codes = (
            df.select(
                pl.col("eid"),
                pl.col("p41270").cast(pl.String)
                .str.split("|")  # Split string into a list
            )
            .explode("p41270") # Turn list into rows
            .with_columns(
                # Create an index (0, 1, 2) for each code per user
                (pl.col("eid").cum_count().over("eid") - 1).alias("idx")
            )
            .with_columns(
                # Clean the code: "B95.6 Staphylococcus" -> "B95.6"
                pl.col("p41270")
                .str.strip_chars()
                .str.split(" ")
                .list.get(0)
                .alias("icd_code")
            )
        )

        # --- Step C: Join and Pivot ---
        final_df = (
            q_codes.join(q_dates, on=["eid", "idx"])
            .pivot(
                on="icd_code",
                index="eid",
                values="date",
                aggregate_function="first" # Use 'min' or 'first' if duplicates exist
            )
        )

        return final_df
