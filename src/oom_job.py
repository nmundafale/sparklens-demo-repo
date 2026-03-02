from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job (fixed version)...")
    
    # Generate large datasets
    df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    
    # FIX: Replace the problematic cross join with an inner join on a common key.
    # A cross join of two large dataframes creates an astronomically large dataset
    # leading to OutOfMemory errors. If the intent was to join on matching IDs,
    # an inner join is appropriate and will result in a manageable dataset size.
    # The spark.sql.crossJoin.enabled setting is no longer needed.
    df_joined = df1.join(df2, df1.id1 == df2.id2, "inner")
    
    # Try to cache and evaluate the much smaller, joined DataFrame
    # The resulting DataFrame from an inner join on matching IDs will have 50M rows,
    # which is manageable for caching and aggregation.
    df_joined.cache()
    df_joined.groupBy("id1").count().show()
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()