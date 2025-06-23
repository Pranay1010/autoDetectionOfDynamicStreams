CREATE OR REPLACE PROCEDURE MY_DB.TEST_SCHEMA.LOAD_ON_BOARD_DIAGNOSTICS_BATCH()
RETURN STRING
LANGUAGE  PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as  snowpark 
from snowflake.snowpark.functions import col 
import pandas as pd 
import json 
import re

def flatten_json(record_content, session):
    try:
        record_content_df = session.create_dataframe([[record_content]], schema=["RECORD_CONTENT"])
        flattened_df = record_content_df.join_table_function(
            "flatten", col("RECORD_CONTENT")["payload"]
        ).select(
            col("value")["spn"].as_("spn"),
            col("value")["time"].as_("time"),
            col("value")["value"].as_("value"),
            col("value")["pgn"].as_("pgn"),
            col("value")["data" ].as_("data"),
            col("value")["lamps"].as_("lamps")
        )
        flattened_df = flattened_df.to_pandas()
        return flattened_df.T
    except Exception as e:
        print(f"Error flattening JSON: {e}")
        return None

def format_key(key):
# Capitalize and separate words with underscores
    key = re.sub(r'(?<1^)(?=[A-Z])','_',key).upper()
    return key

def parse_nested_dict(consolidated_row, data_entry, prefix):
    for key, value in data_entry.items():
        formatted_key = format_key(key)
        new_prefix = f"{prefix}_{formatted_key}"
    if isinstance(value, dict):
        parse_nested_dict(consolidated_row, value, new_prefix)
    else:
        consolidated_row[new_prefix] = value

def process_record(transposed_df, record_content):
    consolidated_row = {}
    key_counter = {}
    # Extract additional fields
    consolidated_row = {'BODY': json.dumps(record_content)}
    
    consolidated_row['DSN'] = record_content.get('dsn')
    consolidated_row['EVENT_ID'] = record_content.get('eventId')
    consolidated_row['VIN'] = record_content.get('eventHeader', {}).get('emitterId')
    consolidated_row['EMITTER_TYPE'] = record_content.get('eventHeader', {}).get('emitterType')
    consolidated_row['EVENT_LEDGER_EVENT_TYPE'] = record_content.get('eventHeader', {}).get('eventLedgerEventType')
    consolidated_row['EVENT_TYPE'] = record_content.get('eventHeader', {}).get('eventType')
    consolidated_row['OCCURRED'] = record_content.get ('eventHeader', {}).get('occurred')

# event_header = record _content get ('eventHeader, (f)
# for-key, value in event_header.items ():
#   formatted key = format_ key(key)
#   consolidated_row[f*EVENT HEADER_(formatted key)] - value 

    for i in range(transposed_df.shape[1]):
        pgn = transposed_df.iloc[3, i]
            if pd.notnull(transposed_df.iloc[0, i]): # Check if SPN is not null
                spn= transposed_df.iloc[0, i]
                consolidated_row[f'PGN_{Pgn}_SPN{spn}_TIME'] = transposed_df.i1oc[1, i]
                consolidated_row[f'PGN_{pgn}_SPN{spn}_VALUE'] = transposed_df.i1oc[2, 1] if transposed_df.shape[0] > 1 else None
            else:
                # Handle DATA column
                data_entries= transposed_df.iloc[4, 1] 
                if isinstance(data_entries, str):
                    try:
                        date_entries = json.loads(data_entries)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in data column: {e}")
                        continue
                        
                if isinstance(data_entries, dict):
                    parse_nested_dict(consolidated_row, data_entries, "DATA")
                    data_entries = [data_entries]

                if isinstance(data_entries, list):
                    for data_entry in data_entries:
                        if isinstance(data_entry, list):
                            for entry in data_entry:
                                spn = entry.get("spn")
                                time = entry.get("time")
                                value = str(entry.get("value"))
                                time_key = f'DATA_PGN_{pgn}_SPN_{spn}_TIME'
                                value_key = f'DATA_PGN_{pgn}_SPN_{spn}_VALUE'
                                consolidated_row[time_key] = time 
                                pgn_spn_key = f"{pgn}_{spn}"
                                if pgn_spn_key in key_counter:
                                    key_counter[pgn_spn_key] += 1
                                    value_key += f"_{key_counter[pgn_spn_key]}"
                                    consolidated_row[value_key] = value
                                else:
                                    key_counter[pgn_spn_key] = 0
                                    consolidated_row[value_key] = value

                        elif isinstance(data_entry, dict):
                            spn = data_entry.get("spn")
                            time = data_entry.get("time")
                            value = data_entry.get("value")
                            consolidated_row[f'DATA_PGN_{pgn}_SPN_{spn}_TIME'] = time
                            consolidated_row[f'DATA_PGN_{pgn}_SPN_{spn}_VALUE'] = value
        # Handle LAMPS column
        lamps_entries = transposed_df.iloc[5, i]
        if isinstance(lamps_entries, str):
            try:
                lamps_entries = json.loads(lamps_entries)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON in lamps column: {e}")
                continue
            
        if isinstance(lamps_entries, dict):
            lamps_entries = [lamps_entries] 
            
        if isinstance(lamps_entries, list): 
            for lamp_entry in lamps_entries:
                spn = lamp_entry.get("spn")
                time = lamp_entry.get("time")
                value = lamp_entry.get("value")
                consolidated_row[f'LAMP_PGN_{pgn}_SPN_{spn}_TIME'] = time
                consolidated_row[f'LAMP_PGN_{Pgn}_SPN_{spn}_VALUE'] = value

    return consolidated_row

def sanitize_column_names(df):
    df.columns = df.columns.astype(str).str.replace('[^a-ZA-Z0-9_]','_',regex=True).str.upper()
    return df

def update_table_schema(session, new_columns):
    existing_columns_query = '''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'ON_BOARD_DIAGNOSTICS_DETAIL'
        AND TABLE_SCHEMA = 'TEST_SCHEMA'
        AND TABLE CATALOG = 'MY_DB'
    '''
    existing_columns_df = session.sql(existing_columns_query).collect()
    existing_columns = {row['COLUMN_NAME'] for row in existing_columns_df}
    
    columns_to_add = set(new_columns) - existing_columns

    for column in columns_to_add:
        alter_table_query = f'''
            ALTER TABLE MY_DB.TEST_SCHEMA.OBD_DETAIL
            ADD COLUMN {column} STRING
            '''
            session.sql(alter_table_query).collect()
            print(f"Added new column: {column}")

def main(session: snowpark.Session):
    load_success = False
try:
    # Query to fetch data
    DR =  '''SELECT * FROM "MY_DB"."TEST_SCHEMA"."ON_BOARD_DIAGNOSTICS" WHERE RECORD_METADATA:Snowf1akeConnectorPushTime::string::timestamp_ntz < DATEADD(DAY, -1, CURRENT_DATE()) AND RECORD_METADATA:Snowf1akeConnectorPushTime is not null AND REPLACE(RECORD_CONTENT:eventId, '"','')::string NOT IN (SELECT DISTINCT EVENT_ID FROM "MY_DB"."TEST_SCHEMA"."ON_BOARD_DIAGNOSTICS_DETAIL") limit 2000
    '''
    dataframe = session.sql(DR)
    all_consolidated_rows = []
    for record in dataframe.to_local_iterator():
        try:
            record_content = json.loads(record['RECORD_CONTENT']) 
            transposed_df = flatten_json(record_content, session) 
            if transposed_df is not None:
                consolidated_row = process_record(transposed_df, record_content) 
                all_consolidated_rows.append(consolidated_row)
        except json.JSONDecodeError as e:
            print(f"Error parsing RECORD_CONTENT: {e}")
            continue
            
        if all_consolidated_rows:
            final_consolidated_df = pd.DataFrame(all_consolidated_rows)
            final_consolidated_df = sanitize_column_names(final_consolidated_df)

            new_columns = final_consolidated_df.columns
            update_table_schema(session, new_columns)

            table_columns_query = ''' 
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'ON_BOARD_DIAGNOSTICS_DETAIL'
                AND TABLE_SCHEMA = 'TEST_SCHEMA'
                AND TABLE_CATALOG = 'MY_DB'
                ORDER BY ORDINAL_POSITION
            '''
            table_columns_df = session.sql(table_columns_query).collect()
            table_columns = [row['COLUMN_NAME'] for row in table_columns_df]

            final_consolidated_df = final_consolidated_df.reindex(columns=table_columns)
            new_snowpark_df = session.create_dataframe(final_consolidated_df)
            new_snowpark_df.write.save_as_table("MY_DB.TEST_SCHEMA.ON_BOARD_DIAGNOSTICS_DETAIL", mode="append")
            
            load_success = True

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if load_success:
            return "AOBD table updated successfully."
$$;