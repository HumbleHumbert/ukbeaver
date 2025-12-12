from pandas import DataFrame
import polars as pl
from polars import Int64, Float64, Utf8, Datetime, Categorical
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any, List
import os, re, requests
import warnings
from ukbeaver.util.schema import Schema

class Category:
    def __init__(
        self,
    ) -> None:
        sa = Schema()

        self.table_1 = sa.get_schema(1)
        self.table_3 = sa.get_schema(3)
        self.table_13 = sa.get_schema(13)

    # Make the dicrtory tree
    def get_tree(self) -> Dict[int, List[int]]:

        # Build the tree (adjacency list) from Table 1
        tree: defaultdict[int, list[int]]= defaultdict(list)

        t13_group = (
            self.table_13
            .sort(['parent_id', 'showcase_order'])
            .group_by('parent_id', maintain_order=True)
            .agg(pl.col('child_id'))
        )

        for parent, child in zip (t13_group['parent_id'], t13_group['child_id']):
            tree[int(parent)].extend([int(c) for c in child])

        # Group the field id
        t1_group = (
            self.table_1
            .sort(['main_category'])
            .group_by('main_category', maintain_order=True)
            .agg(pl.col('field_id'))
        )

        for category, fields in zip(t1_group['main_category'], t1_group['field_id']):
            cat_id = int(category)
            field_list = [int(f) for f in fields]
            tree[cat_id].extend(field_list)

        return tree


    def title2id(self, title) -> int:
        seeker =  {
            row["title"].lower(): int(row["category_id"])
            for row in self.table_3.select("category_id", pl.col("title").str.to_lowercase()).iter_rows(named=True)
        }

        return seeker.get(title, 0)

    # Recursive descendant fetcher (all descendants: subcats + fields)
    def get_descendants(self, start_id: int) -> List[int]:

        tree = self.get_tree()
        descendants: List[int] = []
        def _walk(node: int):
            if node in tree:
                for child in tree[node]:
                    descendants.append(child)
                    _walk(child)  # Recurse for deeper levels
        _walk(start_id)
        return descendants

    # New: Step 5: Recursive field-only fetcher (only leaf fields in the subtree)
    def get_fields_under_category(self, start_id: int) -> List[int]:
        tree = self.get_tree()
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


