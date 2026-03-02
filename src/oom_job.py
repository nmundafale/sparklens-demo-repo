from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("OOM_Job_Fixed") \
        .getOrCreate()
        
    print("Starting OOM dummy job...")
    
    # Fix: Generate significantly smaller datasets to make the cross join feasible.
    # A cross join of 50M x 50M rows results in 2.5 quadrillion rows, which is unmanageable.
    # Reducing to 1,000 x 1,000 rows results in 1 million rows, which is manageable.
    df1 = spark.range(0, 1000).withColumnRenamed("id", "id1") 
    df2 = spark.range(0, 1000).withColumnRenamed("id", "id2") 
    
    # Cross join is still performed, but on smaller datasets, preventing OOM.
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df_cross = df1.crossJoin(df2)
    
    # Cache and evaluate on the manageable dataset
    df_cross.cache()
    # Use show(5) to limit output for the now manageable dataset
    df_cross.groupBy("id1").count().show(5)
    
    print("Job completed successfully!") # This should now print
    spark.stop()

if __name__ == "__main__":
    main()