# src/utils/db_utils.py

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def delete_all_comments(conn_id="postgres_default"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM comment;")
            conn.commit()

def insert_comments(df: pd.DataFrame, conn_id="postgres_default"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO comment (content, created_date, type, origin_type)
                VALUES (%s, %s, %s, %s)
            """
            for _, row in df.iterrows():
                type_value = None if pd.isna(row["type"]) else int(row["type"])
                cursor.execute(sql, (row["content"], row["created_date"], type_value, row["origin_type"]))
            conn.commit()
