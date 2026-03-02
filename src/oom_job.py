from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate datasets.
    # FIX: Reduced the range significantly to make the cross join manageable.
    # A 50M x 50M cross join (2.5 * 10^15 rows) is practically impossible.
    # Reducing to 1k x 1k results in 1 million rows, which is a large but
    # perfectly processable dataset for Spark with default or slightly tuned configurations.
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2")
    
    # Force a cross join.
    # spark.conf.set("spark.sql.crossJoin.enabled", "true") is not strictly needed
    # if using df1.crossJoin(df2) as it explicitly allows it, but keeping for context.
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Cache and evaluate
    # Caching is still performed, but on a manageable dataset.
    df_cross.cache()
    
    # The action that triggers computation.
    # For a 1M row DataFrame, show() is acceptable for a small sample.
    print(f"Cross-joined DataFrame schema:")
    df_cross.printSchema()
    print(f"First 5 rows of cross-joined DataFrame:")
    df_cross.limit(5).show()
    
    # Perform the original aggregation
    result_df = df_cross.groupBy("id1").count()
    print(f"Result of groupBy('id1').count():")
    result_df.show(5)
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()