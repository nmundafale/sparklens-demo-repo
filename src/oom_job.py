from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate smaller datasets to avoid OutOfMemory with cross join
    # Original: 50,000,000 rows -> Cross Join: 2.5 * 10^15 rows (unmanageable)
    # Fixed: 1,000 rows -> Cross Join: 1,000 * 1,000 = 1,000,000 rows (manageable)
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2")
    
    # Perform a cross join with smaller datasets
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    # With smaller data, this should now succeed.
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()