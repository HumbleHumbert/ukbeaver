import polars as pl
from pathlib import Path

def find_project_root(marker_file='pyproject.toml') -> Path:
    """
    Traverse up from the current file's directory to find the project root
    containing the specified marker file.
    """
    current_path = Path(__file__).resolve().parent
    while current_path != current_path.parent:  # Stop at filesystem root
        if (current_path / marker_file).exists():
            return current_path
        current_path = current_path.parent
    raise FileNotFoundError(f"Project root with {marker_file} not found.")


def find_imagings(df: pl.DataFrame, target_string: str) -> list[str]:
    # 1. Identify value columns (exclude 'eid')
    value_cols = [c for c in df.columns if c.startswith("s1t3_a")]

    found_cols = [] # Initialize a list to store results

    # 2. Check each column for the string
    for col_name in value_cols:
        # Check if ANY row in this column contains the target string
        has_target = df.select(
            pl.col(col_name).str.contains(target_string).fill_null(False).any()
        ).item()

        if has_target:
            print(f"Found '{target_string}' in column: {col_name}")
            found_cols.append(col_name) # Add to list

    return found_cols # Return the list, indentation aligned with 'def'
