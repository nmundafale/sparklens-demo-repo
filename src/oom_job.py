from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Fix: Reduce the size of datasets to make the cross join manageable.
    # Original: spark.range(0, 50000000) leading to 2.5 * 10^15 rows after cross join.
    # New: spark.range(0, 5000) leading to 25,000,000 rows after cross join, which is manageable.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2")
    
    # Force a cross join (now on a manageable scale)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()