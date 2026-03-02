from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate large datasets to trigger OutOfMemory
    # FIX: Reduce the range significantly to prevent an astronomically large cross join result.
    # A 5000x5000 cross join results in 25 million rows, which is still large but potentially manageable.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1") # Reduced from 50,000,000
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2") # Reduced from 50,000,000
    
    # Force a cross join to maximize memory and shuffle usage, which will blow up Executors
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    # With reduced input sizes, caching might now be feasible, or at least the subsequent
    # groupBy operation won't immediately OOM.
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully (this shouldn't print!)")
    spark.stop()

if __name__ == "__main__":
    main()