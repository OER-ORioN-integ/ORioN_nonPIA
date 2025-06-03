import sys

from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Glue Context の初期化
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "GROUP_BY_COLUMNS", "ORDER_BY_COLUMNS", "INPUT_S3_DIR", "OUTPUT_S3_DIR"]
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# CSV ファイルの S3 パス（入力と出力）
# --INPUT_PATH s3://your-bucket/path/to/input/
# --OUTPUT_PATH s3://your-bucket/path/to/output/
# --GROUP_BY_COLUMNS work_dt,user_id
# --ORDER_BY_COLUMNS work_dt,-user_id
group_by_columns = args.get("GROUP_BY_COLUMNS", "").split(",")
order_by_columns = args.get("ORDER_BY_COLUMNS", "").split(",")
input_s3_dir = args.get("INPUT_S3_DIR", None)
output_s3_dir = args.get("OUTPUT_S3_DIR", None)

# Glue DynamicFrame に読み込み
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_s3_dir]},
    format="csv",
    format_options={"withHeader": True},
)

# DynamicFrame を Spark DataFrame に変換
df = datasource.toDF()


# 列の値ごとにレコード数を集計
grouped_df = df.groupBy(*group_by_columns).count()

# ソート順序を指定
sort_columns = []
for column in order_by_columns:
    if column.startswith("-"):  # 引数で "-" が付いている場合は降順
        sort_columns.append(F.col(column[1:]).desc())
    else:  # それ以外は昇順
        sort_columns.append(F.col(column).asc())

if len(sort_columns) > 0:
    # 列で昇順にソート
    grouped_df = grouped_df.orderBy(*sort_columns)

# 結果を S3 に保存
grouped_df.write.mode("overwrite").csv(output_s3_dir, header=True)

print("Job completed successfully. Results saved to:", output_s3_dir)
