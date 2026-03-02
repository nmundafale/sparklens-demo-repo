from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate smaller datasets to avoid OutOfMemory for a cross join
    # Reduced from 50,000,000 to 5,000 rows each.
    # This makes the cross join result (5,000 * 5,000 = 25,000,000 rows) manageable.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2")
    
    # Force a cross join to maximize memory and shuffle usage, which will blow up Executors
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully (this shouldn't print!)")
    spark.stop()

if __name__ == "__main__":
    main()