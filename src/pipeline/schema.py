def update_table_schema(session, new_columns):
    """
    Compare new_columns to existing table schema and issue ALTER TABLE
    statements for any missing columns.
    """
    existing_q = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = 'OMEGA_TEST_DB'
          AND TABLE_SCHEMA  = 'ITD_GCS'
          AND TABLE_NAME    = 'AOBD_DETAIL'
    """
    existing = {row["COLUMN_NAME"] for row in session.sql(existing_q).collect()}
    for col in set(new_columns) - existing:
        alter = f"""
            ALTER TABLE OMEGA_TEST_DB.ITD_GCS.AOBD_DETAIL
            ADD COLUMN {col} STRING
        """
        session.sql(alter).collect()
        print(f"Added new column: {col}")