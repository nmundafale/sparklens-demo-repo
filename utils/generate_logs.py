import random
import datetime

def generate_oom_log(filename):
    lines = []
    base_time = datetime.datetime(2025, 3, 1, 10, 0, 0)
    
    def add_log(level, component, msg, time_offset_sec):
        nonlocal base_time
        base_time += datetime.timedelta(seconds=time_offset_sec)
        time_str = base_time.strftime("%y/%m/%d %H:%M:%S")
        lines.append(f"{time_str} {level} {component}: {msg}")

    # Startup logs
    add_log("INFO", "SparkContext", "Running Spark version 3.4.1", 0)
    add_log("INFO", "SparkContext", "Submitted application: OOM_Job", 1)
    for i in range(100):
        add_log("INFO", "SparkEnv", f"Registering MapOutputTracker, BlockManager", 0.1)

    # Some successful tasks
    for i in range(500):
        exec_id = i % 10
        add_log("INFO", "TaskSetManager", f"Starting task {i}.0 in stage 0.0 (TID {i}) (10.0.0.{exec_id}, executor {exec_id}, partition {i}, PROCESS_LOCAL, 7864 bytes)", random.uniform(0.01, 0.1))
        if i > 20:
            add_log("INFO", "TaskSetManager", f"Finished task {i-20}.0 in stage 0.0 (TID {i-20}) in 24 ms on 10.0.0.{exec_id} (executor {exec_id}) (1/500)", random.uniform(0.01, 0.05))

    # Trigger OOM symptoms
    for i in range(200):
        exec_id = 4
        add_log("WARN", "MemoryStore", f"Not enough space to cache rdd_3_{i} in memory!", random.uniform(0.1, 0.5))
        add_log("WARN", "BlockManager", f"Persisting block rdd_3_{i} to disk instead.", 0.1)
        add_log("INFO", "TaskSetManager", f"Starting task {i}.0 in stage 1.0", 0.1)

    add_log("ERROR", "Executor", "Exception in task 45.0 in stage 1.0 (TID 545)", 1)
    lines.append("java.lang.OutOfMemoryError: Java heap space")
    lines.append("\tat java.util.Arrays.copyOf(Arrays.java:3332)")
    lines.append("\tat java.lang.AbstractStringBuilder.ensureCapacityInternal(AbstractStringBuilder.java:124)")
    lines.append("\tat java.lang.AbstractStringBuilder.append(AbstractStringBuilder.java:448)")
    lines.append("\tat java.lang.StringBuilder.append(StringBuilder.java:136)")
    # Add a big stack trace
    for i in range(50):
        lines.append(f"\tat org.apache.spark.sql.execution.UnsafeExternalRowSorter.insertRow(UnsafeExternalRowSorter.java:{100+i})")
        lines.append(f"\tat org.apache.spark.sql.execution.WindowExec$$anonfun$14.apply(WindowExec.scala:{200+i})")

    add_log("ERROR", "TaskSetManager", "Task 45 in stage 1.0 failed 4 times; aborting job", 1)
    add_log("INFO", "TaskSchedulerImpl", "Removed TaskSet 1.0, whose tasks have all completed, from pool", 0.1)
    add_log("INFO", "TaskSchedulerImpl", "Cancelling stage 1", 0.1)
    for i in range(50):
        exec_id = i % 10
        add_log("INFO", "Executor", f"Executor {exec_id} killed task {i}.0 in stage 1.0 (TID {500+i})", random.uniform(0.01, 0.1))

    add_log("ERROR", "SparkContext", "Error initializing SparkContext.", 0)
    add_log("ERROR", "SparkEnv", "Exception in SparkContext shutdown", 0)

    # Padding with junk
    for i in range(3000):
        add_log("INFO", "BlockManagerInfo", f"Removed broadcast_{i}_piece0 on 10.0.0.{i%10} in memory (size: 4.2 KB, free: 2.1 GB)", random.uniform(0.01, 0.05))

    # Add Py4J Python traceback to simulate PySpark calling show()
    lines.append("Traceback (most recent call last):")
    lines.append("  File \"/workspace/sparklens-demo-repo/oom_job.py\", line 22, in <module>")
    lines.append("    main()")
    lines.append("  File \"/workspace/sparklens-demo-repo/oom_job.py\", line 18, in main")
    lines.append("    df_cross.groupBy(\"id1\").count().show()")
    lines.append("  File \"/opt/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py\", line 606, in show")
    lines.append("  File \"/opt/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py\", line 1322, in __call__")
    lines.append("  File \"/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py\", line 196, in deco")
    lines.append("py4j.protocol.Py4JJavaError: An error occurred while calling o88.show.")
    lines.append(": org.apache.spark.SparkException: Job aborted due to stage failure: Task 45 in stage 1.0 failed 4 times, most recent failure: Lost task 45.3 in stage 1.0 (TID 548) (10.0.0.4 executor 4): java.lang.OutOfMemoryError: Java heap space")

    with open(filename, "w") as f:
        f.write("\n".join(lines) + "\n")

def generate_skew_log(filename):
    lines = []
    base_time = datetime.datetime(2025, 3, 1, 12, 0, 0)
    
    def add_log(level, component, msg, time_offset_sec):
        nonlocal base_time
        base_time += datetime.timedelta(seconds=time_offset_sec)
        time_str = base_time.strftime("%y/%m/%d %H:%M:%S")
        lines.append(f"{time_str} {level} {component}: {msg}")

    # Startup logs
    add_log("INFO", "SparkContext", "Running Spark version 3.4.1", 0)
    add_log("INFO", "SparkContext", "Submitted application: Skewed_Data_Job", 1)
    
    # Run Map Stage fast
    add_log("INFO", "DAGScheduler", "Submitting 200 missing tasks from Stage 0 (Map)", 1)
    for i in range(200):
        exec_id = i % 5
        add_log("INFO", "TaskSetManager", f"Starting task {i}.0 in stage 0.0 (TID {i}) on 10.0.0.{exec_id}", random.uniform(0.01, 0.05))
        add_log("INFO", "TaskSetManager", f"Finished task {i}.0 in stage 0.0 (TID {i}) in {random.randint(10, 50)} ms", random.uniform(0.01, 0.05))

    add_log("INFO", "DAGScheduler", "Stage 0.0 ended successfully.", 1)
    
    # Skewed Join Stage
    add_log("INFO", "DAGScheduler", "Submitting 200 missing tasks from Stage 1 (ShuffleMap)", 1)
    for i in range(200):
        exec_id = i % 5
        add_log("INFO", "TaskSetManager", f"Starting task {i}.0 in stage 1.0 (TID {200+i}) on 10.0.0.{exec_id}", random.uniform(0.01, 0.05))

    # All tasks finish quickly except one
    for i in range(1, 200):
        exec_id = i % 5
        add_log("INFO", "TaskSetManager", f"Finished task {i}.0 in stage 1.0 (TID {200+i}) in {random.randint(50, 200)} ms", random.uniform(0.01, 0.05))

    # The stuck task is task 0 on exec 0
    add_log("WARN", "TaskSetManager", "Lost task 0.0 in stage 1.0 (TID 200) on 10.0.0.0, executor 0: FetchFailed(10.0.0.1, 1, 0)", 10)
    
    # Repeated attempts on the skewed partition
    for attempt in range(1, 6):
        add_log("INFO", "TaskSetManager", f"Starting task 0.{attempt} in stage 1.0 (TID {200+200+attempt}) on 10.0.0.0", 5)
        
        # Simulating GC and spilling
        for j in range(500):
            add_log("INFO", "ExternalSorter", f"Spilling in-memory map of 256.0 MB to disk (7 times so far)", random.uniform(5, 10))
            if j % 50 == 0:
                add_log("WARN", "MemoryStore", "Failed to reserve initial memory threshold for TwoWayMerge", 0)

        if attempt < 5:
            add_log("WARN", "TaskSetManager", f"Lost task 0.{attempt} in stage 1.0 (TID {200+200+attempt}) on 10.0.0.0, executor 0: ExecutorLostFailure (executor 0 exited non-zero status)", 30)

    # Job Abort due to too many failures
    add_log("ERROR", "TaskSetManager", "Task 0 in stage 1.0 failed 4 times; aborting job", 1)
    lines.append("org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 4 times, most recent failure: Failure while fetching shuffle block...")
    for i in range(30):
         lines.append(f"\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:{2000+i})")

    add_log("INFO", "TaskSchedulerImpl", "Cancelling stage 1", 0.1)

    # Padding with junk to make it huge
    for i in range(4000):
        add_log("INFO", "BlockManagerInfo", f"Removed broadcast_{i}_piece0 on 10.0.0.{i%5} in memory (size: 6.2 KB, free: 3.1 GB)", random.uniform(0.01, 0.05))

    # Add Py4J Python traceback to simulate PySpark calling show()
    lines.append("Traceback (most recent call last):")
    lines.append("  File \"/workspace/sparklens-demo-repo/skewed_job.py\", line 26, in <module>")
    lines.append("    main()")
    lines.append("  File \"/workspace/sparklens-demo-repo/skewed_job.py\", line 22, in main")
    lines.append("    df_joined.groupBy(\"key\").sum(\"value1\", \"value2\").show()")
    lines.append("  File \"/opt/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py\", line 606, in show")
    lines.append("  File \"/opt/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py\", line 1322, in __call__")
    lines.append("  File \"/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py\", line 196, in deco")
    lines.append("py4j.protocol.Py4JJavaError: An error occurred while calling o102.show.")
    lines.append(": org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 4 times, most recent failure: Failure while fetching shuffle block...")

    with open(filename, "w") as f:
        f.write("\n".join(lines) + "\n")

generate_oom_log("oom_job.stderr")
generate_skew_log("skewed_job.stderr")
