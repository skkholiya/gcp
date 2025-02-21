from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
spark = SparkSession.builder.appName("spark questions").getOrCreate()

'''
Q1:

You are given a dataset containing customer information. Some fields have NULL values.
Write code to replace NULL values with default values.
For the age column, replace NULL with the average age, and for the city column, replace NULL with 'Unknown'.
'''
data = [
    (1, "John", 25, "New York"),
    (2, "Sarah", None, "San Francisco"),
    (3, "Michael", 40, None),
    (4, "Jessica", None, "Los Angeles"),
    (5, "David", 35, "Seattle")
]
 
columns = ["customer_id", "name", "age", "city"]
obj = spark.createDataFrame(data,columns)

avg_age_window = Window.orderBy(monotonically_increasing_id())\
    .rangeBetween(Window.unboundedPreceding,Window.unboundedFollowing)

obj = obj.withColumn("age",when(col("age").isNull(),0).otherwise(col("age")))\
    .withColumn("age",when(col("age")==0,avg(col("age"))\
    .over(avg_age_window).cast("int")).otherwise(col("age").cast("int")))\
    .withColumn("city",when(col("city").isNull(),"Unknown").otherwise(col("city")))

'''
Find total amount received by each merchant cash and online?
merchant  cashamount onlineamount
'''
# Define the schema
schema = StructType([
    StructField("trns_date", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("paymentmode", StringType(), True)
])

# Create the data
data = [
    ("2023-05-01", "M1", 150, "CASH"),
    ("2023-05-01", "M1", 500, "ONLINE"),
    ("2023-05-02", "M2", 450, "ONLINE"),
    ("2023-05-02", "M1", 100, "CASH"),
    ("2023-05-02", "M3", 600, "CASH"),
    ("2023-04-30", "M4", 200, "ONLINE"),
    ("2023-04-30", "M2", 100, "ONLINE")
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

df = df.withColumn("online",when(col("paymentmode")=="ONLINE",col("amount")).otherwise(0))\
.withColumn("cash",when(col("paymentmode")=="CASH",col("amount")).otherwise(0))\
.groupBy("merchant").agg(sum("online").alias("online_amount"),sum("cash").alias("cash_amount"))

df.show()


