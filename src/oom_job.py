from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate smaller datasets to avoid OutOfMemory
    # Original: df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    # Original: df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    # Reduced size to make the cross join manageable (1000 * 1000 = 1 million rows)
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2")
    
    # Force a cross join (now with manageable input sizes)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    # Caching might still spill to disk if memory is tight, but the overall task size is now reduced.
    df_cross.cache() 
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully (this should print now!)")
    spark.stop()

if __name__ == "__main__":
    main()