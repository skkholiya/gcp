from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("emission data")\
    #.config("spark.jars", "/home/ashakholiya87/gcs-connector-hadoop3-latest.jar,/home/ashakholiya87/spark-bigquery-latest.jar")\
    .getOrCreate()
# Set Hadoop configurations for GCS
#hadoop_conf = spark._jsc.hadoopConfiguration()
#hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

read_data = spark.read.format("csv")\
            .option("header",True)\
            .load("gs://skkholiya_upload_data/dataproc_job/ephemeral_cluster/emmission.csv")

#Sanitizing col names
sanitize_col_names = read_data.withColumnRenamed("Emissions (metric tons CO2e)","emission_in_ton")

#Year wise counties emission sum and gas count
group_year_emission = sanitize_col_names.groupBy("Year","Country")\
    .agg(sum(col("emission_in_ton")).cast('double').alias("total_emission"),\
        count_distinct(col("Gas")).alias("gas_count")
        )

#Load data to the BQ
group_year_emission.write.format("bigquery")\
    .option("table","chrome-horizon-448017-g5:dataproc_dump.emmision_dump")\
    .option("createDisposition","CREATE_IF_NEEDED").save()     

spark.stop()
