from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \n        .appName("OOM_Job") \n        .getOrCreate()
    
    print("Starting OOM dummy job...")
    
    # Generate large datasets (without cross join)
    df1 = spark.range(0, 50000000).withColumnRenamed("id", "id1")
    df2 = spark.range(0, 50000000).withColumnRenamed("id", "id2")
    
    # Process data without cross join (example: just show sample data)
    df1.show(5)
    df2.show(5)
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()