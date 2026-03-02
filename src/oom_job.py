from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Fix: Reduce the size of the input DataFrames significantly.
    # A cross join of 50M x 50M rows (2.5 quadrillion) is unmanageable.
    # Reducing to 1000 x 1000 rows (1 million) makes the operation feasible
    # and demonstrates the cross join functionality without OOM.
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2")
    
    # Force a cross join (now on manageable datasets)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Try to cache and evaluate
    df_cross.cache()
    
    # Perform an action to trigger computation and display a few results.
    # .count() forces the full computation of the cross-joined DataFrame.
    print(f"Cross join result will have approximately {df_cross.count()} rows.")
    df_cross.groupBy("id1").count().show(5)
    
    # It's good practice to unpersist cached DataFrames when no longer needed
    df_cross.unpersist()
    
    print("Job completed successfully (this should print now!)")
    spark.stop()

if __name__ == "__main__":
    main()