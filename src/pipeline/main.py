import json
import pandas as pd
from snowflake.snowpark import Session
from .flatten   import flatten_json
from .processor import process_record
from .utils     import sanitize_column_names
from .schema    import update_table_schema


def main(session: Session) -> str:
    load_success = False
    try:
        query = """
            SELECT *
              FROM OMEGA_PROD_DB.ITD_GCS.ADDITIONAL_OBD_DATA
             WHERE RECORD_METADATA:SnowflakeConnectorPushTime::string::timestamp_ntz
                   < DATEADD(DAY, -1, CURRENT_DATE())
               AND RECORD_METADATA:SnowflakeConnectorPushTime IS NOT NULL
               AND REPLACE(RECORD_CONTENT:eventId, '"','')::string NOT IN (
                   SELECT DISTINCT EVENT_ID
                     FROM OMEGA_PROD_DB.ITD_GCS.AOBD_DETAIL
               )
             LIMIT 2000
        """
        df = session.sql(query)
        rows = []
        for rec in df.to_local_iterator():
            try:
                content = json.loads(rec["RECORD_CONTENT"])
                trans_df = flatten_json(content, session)
                if trans_df is not None:
                    rows.append(process_record(trans_df, content))
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")

        if rows:
            final_df = pd.DataFrame(rows)
            final_df = sanitize_column_names(final_df)

            update_table_schema(session, final_df.columns)

            cols_q = """
                SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_CATALOG = 'OMEGA_TEST_DB'
                   AND TABLE_SCHEMA  = 'ITD_GCS'
                   AND TABLE_NAME    = 'AOBD_DETAIL'
                 ORDER BY ORDINAL_POSITION
            """
            cols = [r["COLUMN_NAME"] for r in session.sql(cols_q).collect()]
            final_df = final_df.reindex(columns=cols)

            session.create_dataframe(final_df).write.save_as_table(
                "OMEGA_TEST_DB.ITD_GCS.AOBD_DETAIL", mode="append"
            )
            load_success = True

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        return "AOBD table updated successfully." if load_success else "No new records to load."