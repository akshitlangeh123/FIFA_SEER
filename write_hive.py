from pyspark.sql import SparkSession

def main():
    
    hive_db = 'fifa_db'
    hive_table = 'players'
    tmp_silver_path = "hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/silver"

    spark = SparkSession.builder \
                .appName('write_to_hive') \
                .enableHiveSupport() \
                .getOrCreate()
    
    df_final = spark.read.parquet(tmp_silver_path)

    df_final.write \
        .mode('append') \
        .partitionBy('ingest_year', 'ingest_month', 'ingest_day') \
        .parquet(f"hdfs://localhost:9000/user/hive/warehouse/{hive_db}.db/{hive_table}")

if __name__ == '__main__':
    main()