from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, year, month, dayofmonth, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

def add_timestamp(df):
    df_with_timestamp = df.withColumn('ingest_date', current_date()) \
                            .withColumn('ingest_year', year('ingest_date')) \
                            .withColumn('ingest_month', month('ingest_date')) \
                            .withColumn('ingest_day', dayofmonth('ingest_date'))\
                            .withColumn('updated_at', current_timestamp())
    return df_with_timestamp

def get_json_data(df, json_schema):
    df_parsed = df.withColumn('json_data',
                    from_json(col('value'), json_schema))
    for field in json_schema.fieldNames():
        df_parsed = df_parsed.withColumn(field, col('json_data.'+field))
    df_parsed = df_parsed.drop('json_data', 'value', 'key')
    return df_parsed

def drop_col(df, *column_list):
    df_final = df.drop(*column_list)
    return df_final


def main():

    tmp_bronze_path = "hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/bronze"
    tmp_silver_path = "hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/silver"

    json_schema = StructType([
                    StructField("sofia_id", IntegerType(), True),
                    StructField("player_url", StringType(), True),
                    StructField("short_name", StringType(), True),
                    StructField("long_name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("dob", DateType(), True),
                    StructField("height_cm", IntegerType(), True),
                    StructField("weight_kg", IntegerType(), True),
                    StructField("nationality", StringType(), True),
                    StructField("club_name", StringType(), True),
                    StructField("league_name", StringType(), True),
                    StructField("overall", IntegerType(), True),
                    StructField("potential", IntegerType(), True),
                    StructField("value_eur", IntegerType(), True),
                    StructField("wage_eur", IntegerType(), True),
                    StructField("player_positions", StringType(), True),
                    StructField("preferred_foot", StringType(), True),
                    StructField("work_rate", StringType(), True),
                ])

    cols_to_drop = ['sofia_id','player_url','short_name']

    spark = SparkSession.builder \
                .appName('silver') \
                .getOrCreate()

    df_bronze = spark.read.parquet(tmp_bronze_path)

    df_phase_one = add_timestamp(df_bronze)
    df_phase_two = get_json_data(df_phase_one, json_schema)
    df_silver = drop_col(df_phase_two, *cols_to_drop)

    df_silver.write.mode('overwrite').parquet(tmp_silver_path)

if __name__ == '__main__':
    main()