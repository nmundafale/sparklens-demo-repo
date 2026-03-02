from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate large datasets to trigger OutOfMemory
    # FIX: Reduced the size of the input DataFrames to make the cross join manageable.
    # A cross join of 50,000,000 x 50,000,000 rows results in 2.5 quadrillion rows,
    # which is infeasible for any practical Spark cluster. 
    # Reducing to 5,000 x 5,000 results in 25 million rows, which is still large
    # but can be processed without immediate OOM.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2")
    
    # Force a cross join to maximize memory and shuffle usage, which will blow up Executors
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    # With reduced input sizes, caching might still spill to disk but the subsequent
    # aggregation should be able to complete without OOM.
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully!") # Adjusted message as the job should now succeed
    spark.stop()

if __name__ == "__main__":
    main()