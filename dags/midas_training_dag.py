# dags/midas_training_dag.py

from pathlib import Path
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from src.models.kcbert.model import KcbertModel

ROOT_DIR = Path(__file__).resolve().parents[1]
NOW_TIME = datetime.now().strftime("%y%m%d")

@dag(
    dag_id="midas_training_dag",
    schedule_interval=None,
    start_date=datetime(2024, 10, 1),
    max_active_tasks=1,
    catchup=False,
)
def midas_training_dag():

    @task
    def train_kcbert_model_task():
        """
        KcBERT 모델 학습을 수행하는 Task
        """
        model = KcbertModel(param_path=ROOT_DIR / "config" / "params.yaml")
        model.train()

    save_model_to_s3_task = LocalFilesystemToS3Operator(
        task_id="save_model_to_s3",
        filename=str(ROOT_DIR / "data" / "model" / "kcbert" / "state_dict" / f"{NOW_TIME}.pt"),
        dest_key=f"models/kcbert/model/{NOW_TIME}.pt",
        bucket_name="midas-bucket-1",
        aws_conn_id="aws_default",
        replace=True,
        retries=3,
    )

    train_kcbert_model_task() >> save_model_to_s3_task

midas_training_dag()
