from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def main():

    topic_name = "fifa_players"
    checkpoint_location =  "hdfs://localhost:9000/user/hadoop/FIFA_SEER/checkpoint"
    tmp_path = "hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/bronze"

    spark = SparkSession.builder \
            .appName('bronze') \
            .getOrCreate()

    df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', topic_name) \
                .option('startingOffsets', 'earliest') \
                .load()

    df = df.select(col('key').cast('string'), col('value').cast('string'))
    
    query = df.writeStream \
                .format('parquet') \
                .option('checkpointLocation', checkpoint_location) \
                .option('path', tmp_path) \
                .outputMode('append') \
                .trigger(once=True) \
                .start() \
                .awaitTermination()

if __name__ == "__main__":
    main()