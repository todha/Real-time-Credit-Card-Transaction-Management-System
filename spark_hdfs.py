from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import requests
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
# Create a Spark session
spark = SparkSession.builder.appName("CreditCardProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema for the streaming data
schema = StructType().add("User", StringType())\
                     .add("Card", StringType())\
                     .add("Year", StringType())\
                     .add("Month", StringType())\
                     .add("Day", StringType())\
                     .add("Time", StringType())\
                     .add("Amount", StringType())\
                     .add("Use Chip", StringType())\
                     .add("Merchant Name", StringType())\
                     .add("Merchant City", StringType())\
                     .add("Merchant State", StringType())\
                     .add("Zip", StringType())\
                     .add("MCC", StringType())\
                     .add("Errors?", StringType())\
                     .add("Is Fraud?", StringType())\
                     .add("key", IntegerType())

# Read data from Kafka using the structured streaming API
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test3-topic") \
    .load()
# Convert the value from Kafka into a string
value_str = kafka_stream_df.selectExpr("CAST(value AS STRING)").alias("value")

# Parse the JSON string into a DataFrame
parsed_df = value_str.select(from_json("value", schema).alias("data")).select("data.*")
# Filter for records where 'Is Fraud?' is 'No'
non_fraud_df = parsed_df.filter("`Is Fraud?` = 'No'")

# Chuyển đổi kiểu dữ liệu cột "Amount"
non_fraud_df = non_fraud_df.withColumn("Amount", regexp_replace("Amount", "\\$", "").cast(FloatType()))
def get_exchange_rate():
    api_url = "https://api.exchangerate-api.com/v4/latest/USD"

    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Nếu response status code là 4xx hoặc 5xx, sẽ ném ra exception
        exchange_rate_data = response.json()
        return exchange_rate_data["rates"]["VND"]  # Lấy tỉ giá từ USD sang VND
    except requests.exceptions.RequestException as e:
        print(f"Error while fetching exchange rate: {e}")
        return None

# Lấy tỉ giá từ API
exchange_rate_usd_to_vnd = get_exchange_rate()
#đổi từ USD sang VND làm tròn 2 chữ sô thập phân
non_fraud_df = non_fraud_df.withColumn("Amount", round(col("Amount") * exchange_rate_usd_to_vnd, 2))

# Tạo cột "date"
df3 = non_fraud_df.withColumn("date", concat(col("Day"), lit("/"), col("Month"), lit("/"), col("Year")))

# Định dạng kiểu ngày thành "dd/MM/yyyy"
df3 = df3.withColumn("formatted_date", date_format(col("date"), "dd/MM/yyyy"))

#Tạo cột time
df3 = df3.withColumn("time", concat(col("Time"), lit(":00")))
# select columns
df3 = df3.select("key", "User", "Card", "Date", "Time", "Amount", "Merchant Name", "Merchant City")

#đường dẫn file hdfs
saving_location = "hdfs://127.0.0.1:9000/transaction_temp_data"
checkpoint_location = "hdfs://127.0.0.1:9000/checkpoint_data"

#mỗi 60 giây push file lên hdfs
query = df3 \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime='60 seconds') \
    .format("csv") \
    .option("path", saving_location) \
    .option("checkpointLocation", checkpoint_location) \
    .start()
# Output the result to the console for testing purposes
query = df3 \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

#result_df.show()
