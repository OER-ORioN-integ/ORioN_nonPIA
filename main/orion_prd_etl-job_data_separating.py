import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, substring, translate


def now_to_str(format="%Y/%m/%dT%H:%M:%S"):
    return datetime.strftime(datetime.now(), format)


def output_csv(df, output_bucket_name, tmp_dir_s3_url, tmp_dir_prefix, output_file_key):
    # 一時領域に出力
    df.coalesce(1).write.options(
        encoding="UTF-8", lineSep="\n", sep=",", quoteAll=True, header=True
    ).mode("overwrite").csv(tmp_dir_s3_url)

    print(f"{now_to_str()} DEBUG (output_csv) listed target:{output_bucket_name}/{tmp_dir_prefix}")

    # 一時領域のファイルを取得
    response = s3_client.list_objects_v2(Bucket=output_bucket_name, Prefix=tmp_dir_prefix)
    print(f"{now_to_str()} DEBUG (output_csv) response:{response}")

    # リネームして出力
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            # ソース情報設定
            copy_source = {"Bucket": output_bucket_name, "Key": obj["Key"]}
            # コピーを実行
            print(f"{now_to_str()} DEBUG (output_csv) copy_source:{copy_source}")
            print(
                f"{now_to_str()} DEBUG (output_csv) copy_output:{output_bucket_name}/{output_file_key}"
            )
            s3_resource.meta.client.copy(
                CopySource=copy_source,
                Bucket=output_bucket_name,
                Key=output_file_key,
                Config=transfer_config,
            )
            # 削除を実行
            s3_client.delete_object(Bucket=output_bucket_name, Key=obj["Key"])


print(f"{now_to_str()} INFO (main) start")

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=256 * 1024 * 1024, multipart_chunksize=256 * 1024 * 1024
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "OUTPUT_BUCKET_NAME", "OUTPUT_DATA_NAME", "INPUT_FILE_PATH", "OUTPUT_DIR_PREFIX"],
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

output_bucket_name = args.get("OUTPUT_BUCKET_NAME", "orion-prd-dm-bdash")
output_data_name = args.get("OUTPUT_DATA_NAME", None)
input_file_path = args.get("INPUT_FILE_PATH", None)
output_dir_prefix = args.get("OUTPUT_DIR_PREFIX", None)

print(f"{now_to_str()} DEBUG (main) args:{args}")
print(f"{now_to_str()} DEBUG (main) output_bucket_name:{output_bucket_name}")
print(f"{now_to_str()} DEBUG (main) output_data_name:{output_data_name}")
print(f"{now_to_str()} DEBUG (main) input_file_path:{input_file_path}")
print(f"{now_to_str()} DEBUG (main) output_dir_prefix:{output_dir_prefix}")

# データ読み込み
df = spark.read.options(
    encoding="UTF-8", escape="\\", header=True, lineSep="\n", sep=",", quote='"'
).csv(input_file_path)

# work_dt列からYYYYMMを抽出（最初の7文字から"/"を削除して生成）
df = df.withColumn("YYYYMM", translate(substring(col("work_dt"), 1, 7), "/", ""))

yyyymms = [row["YYYYMM"] for row in df.select("YYYYMM").distinct().collect()]

# 各prefixごとにCSVファイルとして保存
for yyyymm in yyyymms:
    # prefix列を除外して保存
    df_yyyymm = df.filter(col("YYYYMM") == yyyymm).drop("YYYYMM")

    if yyyymm in ["202411"]:
        df_yyyymm = df_yyyymm.withColumn("YYYYMMDD", translate(col("work_dt"), "/", ""))
        yyyymmdds = [row["YYYYMMDD"] for row in df_yyyymm.select("YYYYMMDD").distinct().collect()]

        for yyyymmdd in yyyymmdds:
            # prefix列を除外して保存
            df_yyyymmss = df_yyyymm.filter(col("YYYYMMDD") == yyyymmdd).drop("YYYYMMDD")
            
            output_file_name = f"{output_data_name}_{yyyymmdd}090000.csv"
            tmp_dir_prefix = f"{output_dir_prefix}/tmp_{yyyymmdd}/"
            tmp_dir_s3_url = f"s3://{output_bucket_name}/{tmp_dir_prefix}"
            output_file_key = f"{output_dir_prefix}/{output_file_name}"

            print(f"{now_to_str()} DEBUG (main) yyyymmdd:{yyyymmdd}")
            print(f"{now_to_str()} DEBUG (main) tmp_dir_s3_url:{tmp_dir_s3_url}")

            output_csv(df_yyyymmss, output_bucket_name, tmp_dir_s3_url, tmp_dir_prefix, output_file_key)
    else:
        output_file_name = f"{output_data_name}_{yyyymm}01090000.csv"
        tmp_dir_prefix = f"{output_dir_prefix}/tmp_{yyyymm}/"
        tmp_dir_s3_url = f"s3://{output_bucket_name}/{tmp_dir_prefix}"
        output_file_key = f"{output_dir_prefix}/{output_file_name}"

        print(f"{now_to_str()} DEBUG (main) yyyymm:{yyyymm}")
        print(f"{now_to_str()} DEBUG (main) tmp_dir_s3_url:{tmp_dir_s3_url}")

        output_csv(df_yyyymm, output_bucket_name, tmp_dir_s3_url, tmp_dir_prefix, output_file_key)

print(f"{now_to_str()} INFO (main) end output")

# Sparkセッションの終了
spark.stop()

job.commit()

print(f"{now_to_str()} INFO (main) end")
