from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate large datasets
    df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    
    # FIX: Replace the cross join with an inner join.
    # A cross join of two 50M row DataFrames creates an unmanageably large dataset (2.5 * 10^15 rows),
    # which inevitably leads to OutOfMemory errors. 
    # Assuming the intent was to join on matching IDs, an inner join is appropriate.
    # If a cross join was truly intended, the input DataFrames must be significantly smaller.
    df_joined = df1.join(df2, df1.id1 == df2.id2, "inner")
    
    # Remove the crossJoin configuration as it's no longer needed
    # spark.conf.set("spark.sql.crossJoin.enabled", "true") 
    
    # Cache and evaluate the now manageable joined DataFrame
    df_joined.cache()
    df_joined.groupBy("id1").count().show()
    
    print("Job completed successfully (this shouldn't print!)")
    spark.stop()

if __name__ == "__main__":
    main()