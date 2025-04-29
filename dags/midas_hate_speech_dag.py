# from pathlib import Path
# from datetime import datetime

# import pandas as pd

# from plugins.hooks.aws_rds_hook import AWSRDSHook

# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import BranchPythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator 

# ROOT_DIR = Path(__file__).resolve().parents[1]
# NOW_TIME = datetime.now().strftime("%y%m%d")

# @dag(
#     dag_id="midas_hate_speech_dag", 
#     schedule_interval="@daily",
#     start_date=datetime(2024, 10, 1),
#     catchup=False,
# )
# def midas_hate_speech_dag():
#     tables = ["recycle_board", "recycle_comment", "question_board", "question_comment"]

#     @task
#     def delete_all_data_from_comment():
#         hook = PostgresHook(postgres_conn_id="postgres_default")
#         conn = hook.get_conn()
#         cursor = conn.cursor()
        
#         cursor.execute("DELETE FROM comment;")
#         conn.commit() 

#         cursor.close()
#         conn.close()


#     def check_init_dataset():
#         hook = PostgresHook(postgres_conn_id="postgres_default")
#         conn = hook.get_conn()
#         cursor = conn.cursor()
        
#         cursor.execute("SELECT COUNT(*) FROM comment")
#         result = cursor.fetchone()[0]

#         return "process_init_dataset" if not result else "skip_init_dataset"

#     @task
#     def process_init_dataset(**context):
#         def check_label(label):
#             if label["여성/가족"] == 1:     return 0
#             elif label["남성"] == 1:        return 1
#             elif label["성소수자"] == 1:    return 2
#             elif label["인종/국적"] == 1:   return 3
#             elif label["연령"] == 1:        return 4
#             elif label["지역"] == 1:        return 5
#             elif label["종교"] == 1:        return 6
#             elif label["기타 혐오"] == 1:   return 7
#             elif label["악플/욕설"] == 1:   return 8
#             elif label["개인지칭"] == 1:    return 9
#             else:                          return 10

#         # https://github.com/smilegate-ai/korean_unsmile_dataset/tree/main
#         origin_train_dataset = pd.read_csv(Path(ROOT_DIR)/"data"/"hatespeech"/"unsmile_train_v1.0.tsv", sep="\t")
#         origin_valid_dataset = pd.read_csv(Path(ROOT_DIR)/"data"/"hatespeech"/"unsmile_valid_v1.0.tsv", sep="\t")

#         origin_dataset_df = pd.concat([origin_train_dataset, origin_valid_dataset])
#         origin_dataset_df.rename(columns={"문장": "content"}, inplace=True)
        
#         origin_dataset_df["created_date"] = "2024-06-01 00:00:00"
#         origin_dataset_df["type"] = origin_dataset_df.apply(check_label, axis=1)
#         columns_to_combine = ["여성/가족", "남성", "성소수자", "인종/국적", "연령", "지역", "종교", "기타 혐오", "악플/욕설", "clean", "개인지칭"]
#         origin_dataset_df["origin_type"] = origin_dataset_df[columns_to_combine].apply(lambda row: ", ".join(row.index[row==1]), axis=1)
#         origin_dataset_df.drop(columns=columns_to_combine, inplace=True)

#         context["ti"].xcom_push(key="origin_dataset_json", value=origin_dataset_df.to_json(orient="records"))


#     @task
#     def insert_init_dataset(**context):
#         origin_dataset_json = context["ti"].xcom_pull(key="origin_dataset_json")
#         origin_dataset_df = pd.read_json(origin_dataset_json)

#         hook = PostgresHook(postgres_conn_id="postgres_default")
#         conn = hook.get_conn()
#         cursor = conn.cursor()

#         insert_sql = """
#             INSERT INTO comment (content, created_date, type, origin_type)
#             VALUES (%s, %s, %s, %s)
#         """

#         for _, row in origin_dataset_df.iterrows():
#             type_value = None if pd.isna(row["type"]) else row["type"]

#             cursor.execute(insert_sql, (row["content"], row["created_date"], type_value, row["origin_type"]))

#         conn.commit()
#         cursor.close()
#         conn.close()

    
#     @task
#     def get_last_fetch_time_from_postgres(**context):
#         hook = PostgresHook(postgres_conn_id="postgres_default")
#         conn = hook.get_conn()
        
#         row = pd.read_sql('''
#             SELECT MAX(created_date) AS created_date 
#             FROM comment
#         ''', con=conn)

#         last_fetch_time = row["created_date"].iloc[0]
        
#         context["ti"].xcom_push(key="last_fetch_time", value=str(last_fetch_time))


#     @task
#     def fetch_data_by_table_from_aws_rds(table, **context):
#         last_fetch_time = context["ti"].xcom_pull(key="last_fetch_time")

#         hook = AWSRDSHook("aws_default")
#         conn, _ = hook.get_conn()
        
#         content_df = pd.read_sql(f'''
#             SELECT content, created_date AS date
#             FROM {table}
#             WHERE content IS NOT NULL AND created_date > '{last_fetch_time}'
#         ''', con=conn)

#         context["ti"].xcom_push(key=f"{table}_content_json", value=content_df.to_json(orient="records"))


#     @task
#     def process_content_data_by_table(table, **context):
#         def extract_content(content):
#             '''
#             format : 
#                 recycle_board : content
#                 recycle_comment : {"comment": "content"}
#                 question_board : <p> content </p>
#                 question_comment : {"comment": "content"}
#             to :
#                 content
#             '''
#             if content[0] == "{":
#                 content = content.split('"')[3]
#             elif content.startswith("<p>"):
#                 content = content.replace("<p>", "").replace("</p>", "")
#             return content

#         comment_json = context["ti"].xcom_pull(key=f"{table}_content_json")
#         comment_df = pd.read_json(comment_json)

#         comment_df["content"] = comment_df["content"].apply(lambda content: extract_content(content))
        
#         context["ti"].xcom_push(key=f"{table}_comment_json", value=comment_df.to_json(orient="records"))


#     @task
#     def insert_data_to_postgres(**context):
#         comment_df = pd.DataFrame()

#         for table in tables:
#             comment_json = context["ti"].xcom_pull(key=f"{table}_comment_json")
            
#             if comment_json:
#                 new_df = pd.read_json(comment_json)
#                 comment_df = pd.concat([comment_df, new_df], ignore_index=True)

#         print(comment_df)

        
#     check_init_dataset = BranchPythonOperator(
#         task_id="check_init_dataset",
#         python_callable=check_init_dataset
#     )
#     process_init_dataset = process_init_dataset()
#     insert_init_dataset = insert_init_dataset()
#     skip_init_dataset = DummyOperator(task_id="skip_init_dataset")
#     get_last_fetch_time_from_postgres = get_last_fetch_time_from_postgres()
#     insert_data_to_postgres = insert_data_to_postgres()
    
    
#     trigger_train_model = TriggerDagRunOperator(
#         task_id="trigger_train_model",
#         trigger_dag_id="midas_training_dag", 
#         wait_for_completion=False,
#     )


#     '''
#     Task sequence
#     '''
#     fetch_tasks, process_tasks = [], []
#     for table in tables:
#         fetch_data = fetch_data_by_table_from_aws_rds.override(task_id=f"fetch_{table}_content_from_aws_rds")(table=table)
#         process_data = process_content_data_by_table.override(task_id=f"process_{table}_content")(table=table)
        
#         fetch_data >> process_data
        
#         fetch_tasks.append(fetch_data)
#         process_tasks.append(process_data)

#     check_init_dataset >> [process_init_dataset, skip_init_dataset]
#     process_init_dataset >> insert_init_dataset
#     skip_init_dataset >> get_last_fetch_time_from_postgres >> fetch_tasks
#     process_tasks >> insert_data_to_postgres >> trigger_train_model
 


# @dag(
#     dag_id="midas_training_dag", 
#     schedule_interval=None,
#     max_active_tasks=1,
#     catchup=False
# )
# def midas_training_dag():

#     @task
#     def train_kcbert_model():
#         from models.kcbert.model import KcbertModel

#         model = KcbertModel(Path(ROOT_DIR)/"params.yaml")
#         model.train()


#     save_model_to_s3 = LocalFilesystemToS3Operator(
#         task_id="save_model_to_s3",
#         filename=Path(ROOT_DIR)/"data"/"model"/"kcbert"/"state_dict"/f"{NOW_TIME}.pt",
#         dest_key=f"s3://midas-bucket-1/models/kcbert/model/{NOW_TIME}",
#         aws_conn_id="aws_default",
#         replace=True,
#         retries=3
#     )


#     '''
#     Task sequence
#     '''
#     train_kcbert_model() >> save_model_to_s3



# midas_hate_speech_dag()
# midas_training_dag()

# dags/midas_hate_speech_dag.py

from pathlib import Path
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from src.loaders.data_loader_kcbert import (
    load_unsmile_dataset,
    transform_unsmile_dataset,
    get_last_fetch_time,
    fetch_new_comments_from_rds,
    clean_comment_content
)
from src.utils.db_utils import delete_all_comments, insert_comments
from src.utils.constants import XComKeys, TableNames

ROOT_DIR = Path(__file__).resolve().parents[1]
NOW_TIME = datetime.now().strftime("%y%m%d")

TABLES = [TableNames.RECYCLE_BOARD, TableNames.RECYCLE_COMMENT, TableNames.QUESTION_BOARD, TableNames.QUESTION_COMMENT]

@dag(
    dag_id="midas_hate_speech_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 1),
    catchup=False,
)
def midas_hate_speech_dag():
    @task
    def branch_check_init_dataset():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM comment")
                result = cursor.fetchone()[0]
                return "process_init_dataset" if result == 0 else "skip_init_dataset"

    @task
    def process_init_dataset(context):
        unsmile_df = load_unsmile_dataset(
            train_path=ROOT_DIR / "data/hatespeech/unsmile_train_v1.0.tsv",
            valid_path=ROOT_DIR / "data/hatespeech/unsmile_valid_v1.0.tsv"
        )
        processed_df = transform_unsmile_dataset(unsmile_df)
        context["ti"].xcom_push(key=XComKeys.ORIGIN_DATASET_JSON, value=processed_df.to_json(orient="records"))

    @task
    def insert_init_dataset(context):
        import pandas as pd
        origin_dataset_json = context["ti"].xcom_pull(key=XComKeys.ORIGIN_DATASET_JSON)
        df = pd.read_json(origin_dataset_json)
        delete_all_comments()
        insert_comments(df)

    @task
    def get_last_fetch_time_task(context):
        last_fetch_time = get_last_fetch_time()
        context["ti"].xcom_push(key=XComKeys.LAST_FETCH_TIME, value=last_fetch_time)

    @task
    def fetch_data_by_table_task(table_name: str, context):
        last_fetch_time = context["ti"].xcom_pull(key=XComKeys.LAST_FETCH_TIME)
        new_comments_df = fetch_new_comments_from_rds(table_name, last_fetch_time)
        context["ti"].xcom_push(key=f"{table_name}_content_json", value=new_comments_df.to_json(orient="records"))

    @task
    def process_content_data_task(table_name: str, context):
        import pandas as pd
        comment_json = context["ti"].xcom_pull(key=f"{table_name}_content_json")
        comment_df = pd.read_json(comment_json)
        comment_df["content"] = comment_df["content"].apply(clean_comment_content)
        context["ti"].xcom_push(key=f"{table_name}_comment_json", value=comment_df.to_json(orient="records"))

    @task
    def insert_data_to_postgres_task(context):
        import pandas as pd
        comment_df = pd.DataFrame()
        for table in TABLES:
            comment_json = context["ti"].xcom_pull(key=f"{table}_comment_json")
            if comment_json:
                df = pd.read_json(comment_json)
                comment_df = pd.concat([comment_df, df], ignore_index=True)

        if not comment_df.empty:
            insert_comments(comment_df)

    check_init_dataset = BranchPythonOperator(
        task_id="check_init_dataset",
        python_callable=branch_check_init_dataset
    )

    process_init = process_init_dataset()
    insert_init = insert_init_dataset()
    skip_init = DummyOperator(task_id="skip_init_dataset")
    get_last_fetch_time = get_last_fetch_time_task()
    insert_data_to_postgres = insert_data_to_postgres_task()

    fetch_tasks = []
    process_tasks = []

    for table in TABLES:
        fetch_task = fetch_data_by_table_task.override(task_id=f"fetch_{table}")(table_name=table)
        process_task = process_content_data_task.override(task_id=f"process_{table}")(table_name=table)
        fetch_task >> process_task
        fetch_tasks.append(fetch_task)
        process_tasks.append(process_task)

    trigger_train_model = TriggerDagRunOperator(
        task_id="trigger_train_model",
        trigger_dag_id="midas_training_dag",
        wait_for_completion=False,
    )

    check_init_dataset >> [process_init, skip_init]
    process_init >> insert_init
    skip_init >> get_last_fetch_time >> fetch_tasks
    process_tasks >> insert_data_to_postgres >> trigger_train_model

midas_hate_speech_dag()
