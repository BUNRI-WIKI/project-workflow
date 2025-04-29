## 프로젝트 개요

"분리위키" 서비스를 위해  
YOLO 기반 이미지 분리배출 모델과  
KcBERT 기반 혐오 표현 분류 모델을  
**자동화된 데이터 파이프라인**과 함께 구축했습니다.

이 프로젝트를 통해  
**모델 학습 → 저장 → 배포 → 모니터링**까지  
풀 사이클을 경험할 수 있었습니다.

---

## 핵심 기능

| 기능 | 설명 |
|:---|:---|
| 모델 학습 자동화 | Airflow + Spark로 데이터 전처리 및 모델 학습 |
| FastAPI 서버 구축 | YOLO, KcBERT 모델을 API로 서빙 |
| 모델 저장 | 학습된 모델을 AWS S3에 저장 |
| 데이터 저장 | 데이터는 AWS RDS에 저장 |
| 모델 모니터링 | TensorBoard로 학습 로그 시각화 |
| 클라우드 연동 | AWS S3, RDS, SQS 활용 |
| 컨테이너화 | Docker Compose 기반 통합 배포 |

---

## 시스템 구조

```plaintext
Google Sheets, RDS, S3
        ↓
Airflow DAG (Trigger)
        ↓
Spark 데이터 전처리
        ↓
모델 학습 (YOLO / KcBERT)
        ↓
모델 S3 저장
        ↓
FastAPI 서버 서빙
        ↓
TensorBoard 모니터링


config/            # 설정 파일 (params.yaml 등)
dags/              # Airflow DAG 파일
plugins/           # Airflow Custom Hooks, Operators
src/
  loaders/         # 데이터 로더
  models/          # YOLO, KcBERT 모델 코드
  utils/           # 공통 유틸 함수
spark/
  app/             # Spark 전처리 스크립트
