import re
import pandas as pd


def format_key(key: str) -> str:
    """
    Convert camelCase or PascalCase to UPPER_SNAKE_CASE.
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', key).upper()


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace invalid characters in column names with underscores and uppercase them.
    """
    df.columns = (
        df.columns
        .astype(str)
        .str.replace(r'[^A-Za-z0-9_]', '_', regex=True)
        .str.upper()
    )
    return df