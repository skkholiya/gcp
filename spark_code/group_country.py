from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,ArrayType
rdd = SparkSession.sparkContext
spark = SparkSession.builder.appName("job details").getOrCreate()
schema = StructType([StructField("DEST_COUNTRY_NAME",StringType()),
                     StructField("ORIGIN_COUNTRY_NAME",StringType()),
                     StructField("count",IntegerType())
                     ])
# Disable AQE
#spark.conf.set("spark.sql.adaptive.enabled", "false")
ddl_schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
# read_csv = spark.read.format("csv").option("header","true")\
#     .load("/home/kholiya/Downloads/2015-summary.csv",header="true")
read_csv = spark.read.csv("gs://skkholiya_upload_data/csv/incoming_data/2015-summary.csv",header="true",schema = ddl_schema)
#read_csv.repartition(8)
#sprint(read_csv.rdd.getNumPartitions())
group_des_country = read_csv.groupBy("DEST_COUNTRY_NAME").agg(count(col("ORIGIN_COUNTRY_NAME")).alias("country_cnt"))
group_des_country.write.mode("append").csv("gs://skkholiya_upload_data/dump/group_country.csv")