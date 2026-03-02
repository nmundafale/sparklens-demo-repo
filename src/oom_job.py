from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate large datasets to trigger OutOfMemory
    # FIX: Reduce the size of the input DataFrames significantly.
    # A cross-join of 50M x 50M rows results in 2.5 quadrillion rows, which is impossible to process.
    # Reducing to 5,000 x 5,000 results in 25 million rows, which is much more manageable for a typical cluster.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2")
    
    # Force a cross join to maximize memory and shuffle usage, which will blow up Executors
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully (this should now print!)")
    spark.stop()

if __name__ == "__main__":
    main()