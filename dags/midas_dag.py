from pathlib import Path
from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable

from operators.process import GoogleSheetToCSVOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ROOT_DIR = Path(__file__).resolve().parents[1]
NOW_TIME = datetime.now().strftime("%y%m%d")


# load images
# def _connect_aws_s3():
#     from airflow.hooks.S3_hook import S3Hook

#     s3_hook = S3Hook(aws_conn_id='aws_s3')
#     bucket = 'midas-bucket-1'

#     output_path = os.path.expanduser('~/data/test')
#     os.makedirs(output_path, exist_ok=True)

#     for image in s3_hook.list_keys(bucket_name=bucket):
#         print(image)
#         # if not os.path.exists(os.path.join(output_path, image)) and '/' not in image:
#         #     download_image_path = s3_hook.download_file(key=image, bucket_name=bucket, local_path=output_path)
#         #     new_image_path = os.path.join('/'.join(download_image_path.split('/')[:-1]), image)

#         #     os.rename(src=download_image_path, dst=new_image_path)


@dag(
    dag_id="midas_dag", 
    schedule_interval="@daily",
    start_date=datetime(2024, 5, 1),
    catchup=False,
)
def midas_dag():
    download_gdrive_file = GoogleSheetToCSVOperator(
        task_id="download_gdrive_file",
        gcp_conn_id="google_cloud_default",
        gsheet_id=Variable.get("gcp_sheet_id"),
        range=Variable.get("gcp_sheet_name"),
        file_name=f"recycle_solution_{NOW_TIME}.csv",
        save_path=Path(ROOT_DIR)/"data"/"solutions",
    )

    process_recycle_solution = SparkSubmitOperator(
        task_id='process_recycle_solution',
        application=str(Path(ROOT_DIR)/"spark"/"app"/"process_recycle_solution.py"),
        conn_id="spark_default",
        conf={
            "spark.master":"spark://spark:7077"
        },
    )

    # Task sequence
    download_gdrive_file >> process_recycle_solution

midas_dag()