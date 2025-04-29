# 📘 MIDAS 모델 학습 및 데이터 파이프라인 구축

## 1. 프로젝트 개요

- **프로젝트명**: MIDAS 모델 학습 및 데이터 파이프라인 구축  
- **프로젝트 기간**: 2025.03 ~ 2025.05  
- **목적 및 배경**:  
  "분리위키" 서비스를 지원하기 위해  
  혐오 표현 분류 및 재활용 이미지 분류 모델을  
  주기적으로 학습하고 자동 저장하는 파이프라인을 구축함.  
  데이터 수집부터 모델 학습, S3 저장까지 자동화하여  
  **지속적으로 갱신 가능한 AI 파이프라인**을 목표로 함.


## 2. 기술 스택

| 구분 | 사용 기술 |
|:--|:--|
| Language | Python 3.10, PySpark |
| Workflow Orchestration | Apache Airflow |
| Distributed Processing | Apache Spark |
| Database | AWS RDS (MySQL) |
| Storage | AWS S3 |
| 모델 학습 | HuggingFace Transformers (KcBERT), Ultralytics YOLOv8 |
| Containerization | Docker, Docker Compose |
| 기타 | Pandas, SQLAlchemy, TensorBoard, PyYAML |


## 3. 주요 기능 및 흐름

- Airflow DAG으로 **일간 데이터 수집 및 모델 학습 스케줄링**
- Spark를 통한 **대용량 데이터 전처리 및 DB 동기화**
- 학습된 모델은 자동으로 S3에 저장
- 저장된 모델은 FastAPI 서버에서 실시간 서빙
- 추후 SQS 기반 자동 reload 기능 계획 중



## 4. 시스템 아키텍처

> 🔔 여기에 DAG 중심의 시스템 구조도 삽입 (Airflow → Spark → Train → S3)

**구성 설명**:

- Google Sheet, RDS에 분산된 데이터를 Airflow에서 통합 수집  
- Spark를 통해 전처리 후 DB에 적재  
- KcBERT 및 YOLO 모델을 학습하고 `.pt` 파일로 저장  
- 저장된 모델은 S3에 업로드  
- TensorBoard로 실시간 모니터링 가능  



## 5. 상세 기능 설명

### 5.1 DAG: midas_hate_speech_dag

- `comment` 테이블 기준 신규 수집 여부 판단
- Google Sheet 또는 RDS에서 원본 텍스트 데이터 수집
- 전처리된 텍스트를 PostgreSQL에 저장 (Airflow 내 자동화)

### 5.2 DAG: midas_training_dag

- KcBERT 모델을 학습하고 S3에 저장
- F1 score, PR curve, 학습 로그를 TensorBoard에 기록

### 5.3 DAG: midas_yolo_dag

- YOLO 학습용 이미지 및 json 메타데이터 전처리
- YOLOv8 모델 학습 및 평가
- 학습 완료된 모델을 S3로 업로드



## 6. 문제 해결 경험

| 이슈 | 해결 방법 |
|:--|:--|
| DAG 간 XCom 데이터 용량 초과 | 파일 기반 전달로 대체 or task 병합 |
| Spark JDBC 연동 시 insert 오류 | null 처리 및 중복 key 방지 로직 추가 |
| 모델 저장 경로 혼동 | S3 저장 시 날짜 기반 폴더로 버전 분리 |
| 리소스 과다 사용 | Docker Compose + Spark 클러스터 분리 구성 |



## 7. 추후 개선 계획

- SQS 기반 DAG 트리거 및 모델 자동 reload 연동  
- MLflow 기반 모델 버전 관리 및 rollback 지원  
- Slack Alert 연동으로 DAG 실패 모니터링 강화  
- Airflow Variable 관리 자동화 (변수 불일치 방지)



## 8. 마무리 및 느낀 점

이번 프로젝트를 통해  
단순한 코드 작성이 아닌  
**운영 환경에서의 모델 학습 자동화, 데이터 전처리, 리소스 관리**까지 직접 경험할 수 있었습니다.

- Spark + Airflow 기반 파이프라인 설계 능력을 키웠고,  
- AWS RDS/S3 연동을 통해 클라우드 기반 ML 파이프라인을 완성해봤습니다.  
- Airflow를 통해 일정 주기로 데이터와 모델을 갱신하고,  
- 학습 결과를 TensorBoard로 시각화하는 흐름은 **현업 수준의 실무 경험**이었습니다.

앞으로는 **MLflow + Prometheus + Grafana**를 연동하여  
더 강력한 모니터링 및 운영이 가능한 구조로 확장하고 싶습니다.
