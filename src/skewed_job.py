from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def main():
    spark = SparkSession.builder \
        .appName("Skewed_Data_Job") \
        .getOrCreate()
        
    print("Starting Skewed Data dummy job...")
    
    # Create a highly skewed dataframe
    # 99.9% of data has key 'SKEWED_KEY', the rest have other keys
    df1 = spark.range(0, 10000000) \
        .withColumn("key", when(col("id") < 9990000, lit("SKEWED_KEY")).otherwise(lit("NORMAL_KEY"))) \
        .withColumn("value1", col("id") * 2)
        
    df2 = spark.range(0, 5000000) \
        .withColumn("key", when(col("id") < 4995000, lit("SKEWED_KEY")).otherwise(col("id").cast("string"))) \
        .withColumn("value2", col("id") * 3)
        
    # Join on the skewed key without salting or broadcast
    # One executor will receive almost all data for the join, causing stuck tasks and ExecutorLost
    df_joined = df1.join(df2, on="key", how="inner")
    
    # Force evaluation which triggers the skewed shuffle
    df_joined.groupBy("key").sum("value1", "value2").show()
    
    print("Job completed successfully (this shouldn't print!)")
    spark.stop()

if __name__ == "__main__":
    main()
