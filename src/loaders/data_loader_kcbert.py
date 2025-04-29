import pandas as pd
from plugins.hooks.aws_rds_hook import AWSRDSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.constants import TableNames

def load_unsmile_dataset(train_path, valid_path):
    train = pd.read_csv(train_path, sep="\t")
    valid = pd.read_csv(valid_path, sep="\t")
    return pd.concat([train, valid], ignore_index=True)

def transform_unsmile_dataset(df):
    def check_label(row):
        if row["여성/가족"] == 1: return 0
        elif row["남성"] == 1: return 1
        elif row["성소수자"] == 1: return 2
        elif row["인종/국적"] == 1: return 3
        elif row["연령"] == 1: return 4
        elif row["지역"] == 1: return 5
        elif row["종교"] == 1: return 6
        elif row["기타 혐오"] == 1: return 7
        elif row["악플/욕설"] == 1: return 8
        elif row["개인지칭"] == 1: return 9
        else: return 10

    df = df.rename(columns={"문장": "content"})
    df["created_date"] = "2024-06-01 00:00:00"
    df["type"] = df.apply(check_label, axis=1)
    columns_to_drop = ["여성/가족", "남성", "성소수자", "인종/국적", "연령", "지역", "종교", "기타 혐오", "악플/욕설", "clean", "개인지칭"]
    df["origin_type"] = df[columns_to_drop].apply(lambda row: ", ".join(row.index[row==1]), axis=1)
    df = df.drop(columns=columns_to_drop)
    return df

def get_last_fetch_time():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        result = pd.read_sql("SELECT MAX(created_date) AS created_date FROM comment", conn)
    return str(result.iloc[0]["created_date"])

def fetch_new_comments_from_rds(table_name, last_fetch_time):
    hook = AWSRDSHook(conn_id="aws_default")
    conn, _ = hook.get_conn()
    df = pd.read_sql(f"""
        SELECT content, created_date
        FROM {table_name}
        WHERE content IS NOT NULL AND created_date > '{last_fetch_time}'
    """, con=conn)
    return df

def clean_comment_content(content: str) -> str:
    if content.startswith("{"):
        return content.split('"')[3]
    elif content.startswith("<p>"):
        return content.replace("<p>", "").replace("</p>", "")
    return content
