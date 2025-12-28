import polars as pl
from collections import defaultdict
from typing import Dict, List, Set

# Assuming this exists as per your code
from ukbeaver.util.schema import Schema 

class Category:
    def __init__(self) -> None:
        sa = Schema()
        
        # Load data eagerly
        self.table_1 = sa.get_schema(1)   # Fields
        self.table_3 = sa.get_schema(3)   # Categories (Descriptions)
        self.table_13 = sa.get_schema(13) # Hierarchy

        # --- Pre-compute Maps for O(1) Lookups ---
        
        # 1. Title Map (Lower case title -> Category ID)
        # We drop nulls and convert to lowercase for case-insensitive lookup
        self.title_map: Dict[str, int] = dict(
            self.table_3.select(
                pl.col("title").str.to_lowercase().alias("key"),
                pl.col("category_id").alias("val")
            ).drop_nulls().iter_rows()
        )

        # 2. Subcategory Map (Parent Cat ID -> List of Child Cat IDs)
        # Explicitly cast to Int64 to ensure Python int compatibility
        t13_agg = (
            self.table_13
            .sort(['parent_id', 'showcase_order'])
            .group_by("parent_id")
            .agg(pl.col("child_id"))
        )
        # Convert to python dict {int: list[int]}
        self.subcategory_map: Dict[int, List[int]] = dict(
            zip(t13_agg["parent_id"].to_list(), t13_agg["child_id"].to_list())
        )

        # 3. Field Map (Category ID -> List of Field IDs)
        # This keeps fields separate from categories to avoid ID collisions
        t1_agg = (
            self.table_1
            .group_by("main_category")
            .agg(pl.col("field_id"))
        )
        self.field_map: Dict[int, List[int]] = dict(
            zip(t1_agg["main_category"].to_list(), t1_agg["field_id"].to_list())
        )

    def get_id_by_title(self, title: str) -> int:
        """Returns category ID for a given title (case-insensitive). Returns -1 if not found."""
        return self.title_map.get(title.lower(), -1)

    def get_descendant_categories(self, start_id: int) -> List[int]:
        """
        Returns a list of all sub-category IDs under the start_id (recursive),
        INCLUDING the start_id itself.
        """
        # If the category doesn't strictly exist in our tree, return just itself 
        # (it might be a leaf category with no children)
        visited: List[int] = []
        stack: List[int] = [start_id]
        
        # Iterative DFS is generally safer than recursion for very deep trees (avoids recursion limit)
        while stack:
            current_id = stack.pop()
            visited.append(current_id)
            
            # If this category has children, add them to the stack
            if current_id in self.subcategory_map:
                # Extend in reverse order so they pop in correct order (optional, purely for traversal order)
                children = self.subcategory_map[current_id]
                stack.extend(reversed(children))
                
        return visited

    def get_fields_under_category(self, start_id: int) -> List[int]:
        """
        Returns ALL field IDs under a category and all its sub-categories.
        """
        all_fields: List[int] = []
        
        # 1. Get the category and all its sub-categories
        relevant_categories = self.get_descendant_categories(start_id)
        
        # 2. Collect fields for each of those categories
        for cat_id in relevant_categories:
            if cat_id in self.field_map:
                all_fields.extend(self.field_map[cat_id])
                
        return all_fields

    def get_fields_by_title(self, title: str) -> List[int]:
        """Helper to go directly from Title string -> List of Field IDs"""
        cat_id = self.get_id_by_title(title)
        if cat_id == -1:
            print(f"Warning: Category '{title}' not found.")
            return []
        return self.get_fields_under_category(cat_id)
