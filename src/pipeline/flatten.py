import pandas as pd
from snowflake.snowpark.functions import col


def flatten_json(record_content: dict, session) -> pd.DataFrame:
    """
    Flatten the nested JSON payload for a single record into a transposed pandas DataFrame.
    """
    try:
        # Create a one-row Snowpark DataFrame and call the FLATTEN table function
        record_df = session.create_dataframe(
            [[record_content]], schema=["RECORD_CONTENT"]
        )
        flat_df = (
            record_df
            .join_table_function("flatten", col("RECORD_CONTENT")["payload"])
            .select(
                col("value")["spn"].as_("spn"),
                col("value")["time"].as_("time"),
                col("value")["value"].as_("value"),
                col("value")["pgn"].as_("pgn"),
                col("value")["data"].as_("data"),
                col("value")["lamps"].as_("lamps"),
            )
        )
        # Convert to pandas and transpose so each field is a column
        return flat_df.to_pandas().T
    except Exception as e:
        print(f"Error flattening JSON: {e}")
        return None