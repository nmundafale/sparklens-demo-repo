from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job (fixed)...")
    
    # Generate smaller datasets to avoid OutOfMemory
    # Original: df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    # Original: df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    # Reduced to 1,000 rows each, resulting in 1,000 * 1,000 = 1,000,000 rows after cross join,
    # which is a manageable size for demonstration without OOM.
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1") 
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2") 
    
    # Force a cross join. With smaller datasets, this will no longer blow up Executors.
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    # With 1M rows, caching might still be memory intensive, but manageable or spillable without OOM.
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()