from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \n        .appName("OOM_Job") \n        .getOrCreate()
    
    print("Starting OOM dummy job...")
    
    # Generate large datasets (but avoid cross join)
    df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    
    # Use a proper join instead of cross join (example: broadcast join)
    # df_cross = df1.join(df2, df1.id1 == df2.id2, "inner")  # Replace with actual join logic
    
    # Example of safe operation: filter data to reduce size
    df_filtered = df1.filter(col("id1") % 10 == 0).join(df2.filter(col("id2") % 10 == 0), df1.id1 == df2.id2, "inner")
    
    # Process data
    df_filtered.groupBy("id1").count().show()
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()