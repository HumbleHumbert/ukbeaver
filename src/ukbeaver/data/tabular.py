from pandas import DataFrame
import polars as pl
from polars import Int64, Float64, Utf8, Datetime, Categorical
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any, List
import os, re, requests
import warnings

class Phenotype:
    def __init__(
        self,
        pheno_table: Path,
    ) -> None:
        self.pheno_table = pheno_table
        self.dict_path = 'dictionaries/field.txt'

        # Global cache dir
        cache_dir = Path.home() / ".ukbeaver"
        self.dict_path = cache_dir / "dictionaries"
        self.dict_path.mkdir(exist_ok=True)

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

        # Prepare the schema 1
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="CSV malformed")
            self.table_1 = pl.read_csv(
                self.schema_1,
                separator="\t",
                ignore_errors=True
            )

    # Make the dicrtory tree
    def get_category(self):
        table_13 = pl.read_csv(self.schema_13, separator='\t', encoding='latin1')
        table_3 = pl.read_csv(self.schema_3, separator='\t', encoding='latin1')
        table_1 = self.table_1

        # Build the tree (adjacency list) from Table 1
        tree: defaultdict[int, list[int]]= defaultdict(list)

        t13_group = (
            table_13
            .sort(['parent_id', 'showcase_order'])
            .group_by('parent_id', maintain_order=True)
            .agg(pl.col('child_id'))
        )

        for parent, child in zip (t13_group['parent_id'], t13_group['child_id']):
            tree[int(parent)].extend([int(c) for c in child])

        # Group the field id
        t1_group = (
            table_1
            .sort(['main_category'])
            .group_by('main_category', maintain_order=True)
            .agg(pl.col('field_id'))
        )

        for category, fields in zip(t1_group['main_category'], t1_group['field_id']):
            cat_id = int(category)
            field_list = [int(f) for f in fields]
            tree[cat_id].extend(field_list)

        return tree

    # Step 3: Build title-to-id lookup (case-insensitive)
    def build_title_lookup(df_cat: pl.DataFrame) -> Dict[str, int]:
        return {
            row["title"].lower(): int(row["category_id"])
            for row in df_cat.select("category_id", pl.col("title").str.to_lowercase()).iter_rows(named=True)
        }

    # Step 4: Recursive descendant fetcher (all descendants: subcats + fields)
    def get_descendants(tree: Dict[int, List[int]], start_id: int) -> List[int]:
        descendants: List[int] = []
        def _walk(node: int):
            if node in tree:
                for child in tree[node]:
                    descendants.append(child)
                    _walk(child)  # Recurse for deeper levels
        _walk(start_id)
        return descendants

    # New: Step 5: Recursive field-only fetcher (only leaf fields in the subtree)
    def get_fields_under_category(tree: Dict[int, List[int]], start_id: int) -> List[int]:
        fields: List[int] = []
        def _walk(node: int):
            if node in tree:
                for child in tree[node]:
                    if child not in tree:  # Leaf check: if no entry in tree, it's a field (no children)
                        fields.append(child)
                    else:
                        _walk(child)  # Recurse only if it's a subcategory
        _walk(start_id)
        return fields

    # Step 6: Query helpers (title -> results)
    def get_all_ids_under_title(
        title: str, title_to_id: Dict[str, int], tree: Dict[int, List[int]]
    ) -> List[int]:
        title_lower = title.lower()
        start_id = title_to_id.get(title_lower)
        if start_id is None:
            return []
        return get_descendants(tree, start_id)

    def get_fields_under_title(
        title: str, title_to_id: Dict[str, int], tree: Dict[int, List[int]]
    ) -> List[int]:
        title_lower = title.lower()
        start_id = title_to_id.get(title_lower)
        if start_id is None:
            return []
        return get_fields_under_category(tree, start_id)



    def get_datatype(self) -> Dict[str, pl.DataType]:
        # mapping value_type → Polars dtype (stays the same)
        value_type_map = {
            11: Int64,
            21: Categorical,
            22: Categorical,
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

        return dtype_map

    def get_df(self, fids: Optional[list[str]] = None, ins: Optional[str] = None) -> tuple[Any, dict[Any, Any]]:
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

