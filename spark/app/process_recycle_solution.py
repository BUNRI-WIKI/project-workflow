# '''
# target이 기존 데이터베이스에 없는 name, tag, category를 확인하고 없다면 추가

# target : 
# RecycleSolution.csv
#     name : 분비배출 대상
#     material : 재질(공백으로 구분)
#     tag : 태그(#으로 구분)
#     solution : 분리배출 방법
#     imgUrl : 이미지 URL


# exist : 
#     pass

# Doesn't exist :
#     INSERT INTO waste
#     VALUES (created_date, modified_date, image_url, name, solution, state, writer_id) 
#         created_date = datetime.now()
#         modified_date = datetime.now()
#         image_url = imgUrl
#         name = name
#         solution = solution
#         state = 1
#         writer_id = 1(admin)

#     INSERT INTO tag
#     VALUES (name, waste_id)
#         name = tag
#         waste_id = waste_id
    
#     INSERT INTO category
#     VALUES (name, waste_id)
#         name = material
#         waste_id = waste_id
# '''
# from pathlib import Path
# ROOT_DIR = Path(__file__).resolve().parents[2]

# from datetime import datetime

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit, col, split, explode

# from airflow.models import Variable

# # config AWS RDS
# AWS_RDS_URL = "jdbc:mysql://{0}:{1}/{2}".format(Variable.get('aws_rds_host'), 3306, Variable.get('aws_rds_database'))
# AWS_RDS_USER = Variable.get('aws_rds_user')
# AWS_RDS_PASSWORD = Variable.get('aws_rds_password')
# AWS_RDS_DRIVER = "com.mysql.cj.jdbc.Driver"
# RECYCLE_SOLUTION_DIR = f"{str(ROOT_DIR)}/data/solutions/recycle_solution_{datetime.now().strftime('%y%m%d')}.csv"

# # Create Spark Session
# spark = (SparkSession.builder 
#         .appName("RecycleSolution") 
#         .master("local[*]") 
#         .getOrCreate())

# # JDBC Reader Settings
# reader = (spark.read.format("jdbc")
#         .option("url", AWS_RDS_URL)
#         .option("user",AWS_RDS_USER)
#         .option("password", AWS_RDS_PASSWORD)
#         .option('driver', AWS_RDS_DRIVER))

# # Load CSV file
# rs_df = (spark.read.format("csv")
#         .option("header", "true")
#         .option("nullValue", "")
#         .load(RECYCLE_SOLUTION_DIR)
# )

# # Filter valid data
# filtered_df = (rs_df
#         .filter(col("solution").isNotNull())
#         .filter(col("tag").isNotNull())
#         .filter(col("material").isNotNull()))

# # Load existing data from RDS
# waste_rds_df = reader.option("dbtable", "waste").load()

# # Check and insert new waste data
# new_waste_df = (filtered_df
#         .join(waste_rds_df, on="name", how="left_anti"))
# new_waste_df.show()

# if new_waste_df.count() > 0:
#     new_waste_insert_df = (new_waste_df
#         .withColumn("created_date",     lit(datetime.now()))
#         .withColumn("modified_date",    lit(datetime.now()))
#         .withColumn("state",            lit(1))
#         .withColumn("writer_id",        lit(1))
#         .select("created_date", "modified_date", col("imgUrl").alias("image_url"), "name", "solution", "state", "writer_id"))
    
#     new_waste_insert_df.write.format("jdbc") \
#         .option("url", AWS_RDS_URL) \
#         .option("dbtable", "waste") \
#         .option("user", AWS_RDS_USER) \
#         .option("password", AWS_RDS_PASSWORD) \
#         .option("driver", AWS_RDS_DRIVER) \
#         .mode("append") \
#         .save()

# # Reload updated waste data
# waste_rds_df = reader.option("dbtable", "waste").load()

# # Load existing tag and category data
# tag_rds_df = reader.option("dbtable", "tag").load().select(col("waste_id").alias("id"), col("name").alias("tag"))
# category_rds_df = reader.option("dbtable", "category").load().select(col("waste_id").alias("id"), col("name").alias("material"))

# # Explode and insert new tag data
# new_tags_df = (filtered_df
#         .withColumn("tag", explode(split("tag", "#")))
#         .filter(col("tag") != "")
#         .join(waste_rds_df, on="name")
#         .join(tag_rds_df, on=["id", "tag"], how="left_anti")
#         .select(col("id").alias("waste_id"), col("tag").alias("name")))
# new_tags_df.show()

# if new_tags_df.count() > 0:
#     new_tags_df.write.format("jdbc") \
#         .option("url", AWS_RDS_URL) \
#         .option("dbtable", "tag") \
#         .option("user",  AWS_RDS_USER) \
#         .option("password", AWS_RDS_PASSWORD) \
#         .option("driver", AWS_RDS_DRIVER) \
#         .mode("append") \
#         .save()

# # Explode and insert new category data
# new_categories_df = (filtered_df
#         .withColumn("material", explode(split("material", " ")))
#         .join(waste_rds_df, on="name")
#         .join(category_rds_df, on=["id", "material"], how="left_anti")
#         .select(col("id").alias("waste_id"), col("material").alias("name")))
# new_categories_df.show()

# if new_categories_df.count() > 0:
#     new_categories_df.write.format("jdbc") \
#         .option("url", AWS_RDS_URL) \
#         .option("dbtable", "category") \
#         .option("user", AWS_RDS_USER) \
#         .option("password", AWS_RDS_PASSWORD) \
#         .option("driver", AWS_RDS_DRIVER) \
#         .mode("append") \
#         .save()




# src/spark/app/process_recycle_solution.py

from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, current_timestamp
from airflow.models import Variable

ROOT_DIR = Path(__file__).resolve().parents[2]
NOW_TIME = datetime.now().strftime('%y%m%d')

# 환경 설정
AWS_RDS_URL = f"jdbc:mysql://{Variable.get('aws_rds_host')}:3306/{Variable.get('aws_rds_database')}"
AWS_RDS_USER = Variable.get('aws_rds_user')
AWS_RDS_PASSWORD = Variable.get('aws_rds_password')
AWS_RDS_DRIVER = "com.mysql.cj.jdbc.Driver"

RECYCLE_SOLUTION_PATH = ROOT_DIR / "data" / "solutions" / f"recycle_solution_{NOW_TIME}.csv"

# Spark 세션 생성 (Docker에서는 spark://spark_master:7077)
spark = (SparkSession.builder
         .appName("ProcessRecycleSolution")
         .master("spark://spark_master:7077")  # local[*] -> spark cluster
         .getOrCreate())

spark.sparkContext.setLogLevel("INFO")

def save_to_rds(df, table_name):
    if df.count() > 0:
        df.write.format("jdbc") \
            .option("url", AWS_RDS_URL) \
            .option("dbtable", table_name) \
            .option("user", AWS_RDS_USER) \
            .option("password", AWS_RDS_PASSWORD) \
            .option("driver", AWS_RDS_DRIVER) \
            .mode("append") \
            .save()
        print(f"[INFO] Inserted into {table_name}: {df.count()} rows")
    else:
        print(f"[INFO] No new records for {table_name}")

# 1. CSV 파일 로드
rs_df = (spark.read.format("csv")
         .option("header", "true")
         .option("nullValue", "")
         .load(str(RECYCLE_SOLUTION_PATH)))

filtered_df = (rs_df
               .filter(col("solution").isNotNull())
               .filter(col("tag").isNotNull())
               .filter(col("material").isNotNull()))

# 2. Waste 테이블 로드
waste_df = (spark.read.format("jdbc")
            .option("url", AWS_RDS_URL)
            .option("dbtable", "waste")
            .option("user", AWS_RDS_USER)
            .option("password", AWS_RDS_PASSWORD)
            .option("driver", AWS_RDS_DRIVER)
            .load())

# 3. 새 waste 데이터 insert
new_waste_df = (filtered_df
    .join(waste_df, on="name", how="left_anti")
    .withColumn("created_date", current_timestamp())
    .withColumn("modified_date", current_timestamp())
    .withColumn("state", lit(1))
    .withColumn("writer_id", lit(1))
    .select(
        "created_date",
        "modified_date",
        col("imgUrl").alias("image_url"),
        "name",
        "solution",
        "state",
        "writer_id"
    ))

save_to_rds(new_waste_df, "waste")

# 4. waste 다시 불러오기 (id 연결용)
waste_df = (spark.read.format("jdbc")
            .option("url", AWS_RDS_URL)
            .option("dbtable", "waste")
            .option("user", AWS_RDS_USER)
            .option("password", AWS_RDS_PASSWORD)
            .option("driver", AWS_RDS_DRIVER)
            .load())

# 5. tag 테이블 업데이트
tag_df = (spark.read.format("jdbc")
          .option("url", AWS_RDS_URL)
          .option("dbtable", "tag")
          .option("user", AWS_RDS_USER)
          .option("password", AWS_RDS_PASSWORD)
          .option("driver", AWS_RDS_DRIVER)
          .load()
          .select(col("waste_id").alias("id"), col("name").alias("tag")))

new_tag_df = (filtered_df
    .withColumn("tag", explode(split("tag", "#")))
    .filter(col("tag") != "")
    .join(waste_df, on="name")
    .join(tag_df, on=["id", "tag"], how="left_anti")
    .select(col("id").alias("waste_id"), col("tag").alias("name")))

save_to_rds(new_tag_df, "tag")

# 6. category 테이블 업데이트
category_df = (spark.read.format("jdbc")
               .option("url", AWS_RDS_URL)
               .option("dbtable", "category")
               .option("user", AWS_RDS_USER)
               .option("password", AWS_RDS_PASSWORD)
               .option("driver", AWS_RDS_DRIVER)
               .load()
               .select(col("waste_id").alias("id"), col("name").alias("material")))

new_category_df = (filtered_df
    .withColumn("material", explode(split("material", " ")))
    .filter(col("material") != "")
    .join(waste_df, on="name")
    .join(category_df, on=["id", "material"], how="left_anti")
    .select(col("id").alias("waste_id"), col("material").alias("name")))

save_to_rds(new_category_df, "category")

print("[INFO] Recycle solution processing completed.")
