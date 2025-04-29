# src/loaders/data_loader_yolo.py

import os
import pandas as pd
import pytz
import yaml
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from plugins.hooks.aws_rds_hook import AWSRDSHook

ROOT_DIR = Path(__file__).resolve().parents[2]
NOW_TIME = datetime.now().strftime("%y%m%d")

def get_s3_images_by_month(bucket_name: str, prefix: str):
    """
    S3에서 이번 달 업로드된 이미지 리스트를 가져온다.
    (참고용: 현재 로직에서는 가져오기만 하고 저장은 안함)
    """
    s3_hook = S3Hook(aws_conn_id="aws_default")
    kst = pytz.timezone("Asia/Seoul")

    now = datetime.now().astimezone(kst).replace(hour=0, minute=0, second=0, microsecond=0)
    from_datetime = now.replace(day=1)
    to_datetime = (from_datetime + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)

    images = [
        image for image in s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=prefix,
            from_datetime=from_datetime,
            to_datetime=to_datetime
        )
    ]

    print(f"[INFO] S3 images fetched: {len(images)}개")
    return images

def get_verified_images_from_rds():
    """
    AWS RDS에서 검증된 이미지와 카테고리를 가져온다.
    (waste, category 테이블 조인)
    """
    hook = AWSRDSHook(conn_id="aws_default")
    conn, _ = hook.get_conn()

    df = pd.read_sql('''
        SELECT w.id, w.image_url, c.name AS category
        FROM waste w
        INNER JOIN category c ON w.id = c.waste_id
        WHERE w.image_url IS NOT NULL
    ''', con=conn)

    output_path = ROOT_DIR / "data" / "images"
    os.makedirs(output_path, exist_ok=True)
    df.to_csv(output_path / f"images_{NOW_TIME}.csv", index=False)

    print(f"[INFO] Verified images saved to {output_path / f'images_{NOW_TIME}.csv'}")

def prepare_yolo_dataset():
    """
    YOLO 학습용 데이터셋을 준비한다.
    - 검증된 이미지를 기반으로 YOLO 포맷 txt 파일을 생성
    """
    with open(ROOT_DIR / "config" / "recycle.yaml") as f:
        image_classes = yaml.safe_load(f)["names"]

    verified_images_path = ROOT_DIR / "data" / "images" / f"images_{NOW_TIME}.csv"
    df = pd.read_csv(verified_images_path)

    output_label_dir = ROOT_DIR / "data" / "yolo" / "labels"
    os.makedirs(output_label_dir, exist_ok=True)

    for idx, row in df.iterrows():
        image_id = row["id"]
        category = row["category"]

        label_id = image_classes.index(category) if category in image_classes else None
        if label_id is not None:
            label_file_path = output_label_dir / f"{image_id}.txt"
            with open(label_file_path, "w") as f:
                f.write(f"{label_id} 0.5 0.5 1.0 1.0")  # 임시 전체 bbox로 마킹 (개발 편의상)
    
    print(f"[INFO] YOLO labels saved in {output_label_dir}")

