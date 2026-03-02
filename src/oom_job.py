from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Generate smaller datasets to avoid OutOfMemory caused by an unmanageably large cross join.
    # The original 50,000,000 rows for each DataFrame resulted in 2.5 * 10^15 rows after crossJoin,
    # which is too large for any practical cluster.
    # Reducing the range to 5,000 for each DataFrame will result in 25,000,000 rows after crossJoin,
    # which is a large but manageable dataset size for Spark.
    df1 = spark.range(0, 5000).withColumnRenamed("id", "id1") 
    df2 = spark.range(0, 5000).withColumnRenamed("id", "id2") 
    
    # Force a cross join. With the reduced input sizes, this operation becomes feasible.
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate. With manageable data size, this should now succeed.
    df_cross.cache()
    df_cross.groupBy("id1").count().show()
    
    print("Job completed successfully (this shouldn't print in the original OOM scenario, but will now)")
    spark.stop()

if __name__ == "__main__":
    main()