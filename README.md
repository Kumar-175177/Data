# Data
**Data Engineering Interview Q&A – PySpark, Azure, SQL, ADF, Delta Lake, and More**

---

**1. How does lazy evaluation work in PySpark?**  
• **Answer:** Lazy evaluation in PySpark means Spark doesn’t execute operations immediately. Instead, it builds a logical plan and waits for an action (like `count()`, `collect()`) to trigger execution. This lets Spark optimize the entire execution plan for better performance.

---

**2. What is the difference between groupByKey and reduceByKey in PySpark?**  
• **Answer:** `groupByKey()` groups values with the same key but can be memory-intensive and cause large shuffles. `reduceByKey()` performs aggregation before shuffling, making it more efficient. Use `reduceByKey()` when aggregation is needed.

---

**3. What is the difference between wide and narrow transformations in PySpark?**  
• **Answer:** Narrow transformations (like `map`, `filter`) don’t require data to move across partitions. Wide transformations (like `groupByKey`, `join`) involve shuffling data between nodes, which can be expensive.

---

**4. What are broadcast joins in PySpark?**  
• **Answer:** Broadcast joins improve join performance by sending the smaller DataFrame to all executors, avoiding shuffles. This is efficient when one dataset is small enough to fit in memory.

---

**5. What is the role of SparkSession in PySpark?**  
• **Answer:** `SparkSession` is the entry point to PySpark functionality. It allows you to create DataFrames, read/write data, and configure Spark settings. It replaces older contexts like `SQLContext` and `HiveContext`.

---

**6. What are accumulators and broadcast variables in Spark?**  
• **Answer:** Accumulators are write-only shared variables used for aggregation (e.g., counters). Broadcast variables are read-only shared variables used to efficiently distribute large data (e.g., lookup tables) across executors.

---

**7. What is Delta Lake and its advantages?**  
• **Answer:** Delta Lake adds ACID transactions, schema enforcement, and versioning to data lakes. It enables time travel, upserts (MERGE), and handles schema evolution for reliable big data pipelines.

---

**8. What is the significance of Z-ordering in Delta tables?**  
• **Answer:** Z-ordering optimizes data layout by colocating related records in the same file. This improves query performance by minimizing data scanned during filtering operations (especially on large datasets).

---

**9. How do you handle schema evolution in Delta Lake?**  
• **Answer:** Delta Lake supports schema evolution using options like `mergeSchema` during writes. This allows adding new columns while ensuring backward compatibility. Schema enforcement avoids bad data writes.

---

**10. Explain Adaptive Query Execution (AQE) in Spark.**  
• **Answer:** AQE optimizes queries at runtime based on statistics like partition sizes. It can dynamically switch join strategies, optimize skewed joins, and reduce number of partitions, improving performance.

---

**11. How do you handle job failures in an ETL pipeline?**  
• **Answer:** Use retry policies, logging, and failure paths. In ADF, use failure dependencies and alerts. Analyze logs for root cause and apply fixes. For critical jobs, use monitoring and alerts.

---

**12. What steps do you take when a data pipeline is running slower than expected?**  
• **Answer:** Check data volume, shuffle operations, skewed partitions, and transformations. Enable Spark UI for analysis. Optimize joins, repartition data, and use caching or broadcast joins where needed.

---

**13. How do you address data quality issues in a large dataset?**  
• **Answer:** Implement validation rules, null checks, deduplication, and schema checks. Use tools like Great Expectations or custom PySpark validation logic. Log and quarantine bad records.

---

**14. How do you manage and monitor ADF pipeline performance?**  
• **Answer:** Use ADF monitoring tab, integration with Azure Monitor, and log analytics. Enable activity runs logging and check metrics like duration, throughput, and data volume.

---

**15. Explain the concept of Managed Identity in Azure.**  
• **Answer:** Managed Identity allows ADF, Synapse, or Databricks to access Azure services securely without managing secrets. It uses Azure AD authentication, simplifying access control.

---

**16. How do you implement CI/CD pipelines for ADF and Databricks?**  
• **Answer:** Use Azure DevOps or GitHub Actions. Store ADF JSON and notebooks in Git repo. Create deployment pipelines with ARM templates for ADF and workspace APIs for Databricks.

---

**17. How do you handle schema drift in ADF?**  
• **Answer:** Enable schema drift in source/sink datasets. Use mapping data flows to define dynamic mappings. This helps pipelines adjust automatically to minor schema changes.

---

**18. What is the difference between coalesce and repartition in Spark?**  
• **Answer:** `repartition()` increases or decreases partitions with a full shuffle. `coalesce()` only reduces partitions without full shuffle, making it more efficient for narrowing partitions.

---

**19. What is a vacuum command in Delta Lake?**  
• **Answer:** `VACUUM` deletes old, unused files from Delta tables to free up space. You can set a retention threshold (e.g., 168 hours by default) to safely remove files.

---

**20. What are common transformations used in PySpark?**  
• **Answer:** `map()`, `flatMap()`, `filter()`, `join()`, `groupBy()`, `select()`, `withColumn()`, and `agg()` are commonly used to shape and transform data in Spark jobs.


### PySpark and Big Data Interview Q&A (Set 2)

---

**3. How do you optimize a data pipeline for better performance and scalability?**

To optimize a data pipeline, I focus on minimizing I/O operations and data shuffling. This includes filtering early, avoiding unnecessary data writes, and using partitioning to parallelize workloads. I also leverage caching for intermediate results, use optimized file formats like Parquet, and ensure schema enforcement to reduce overhead. In distributed systems like Spark, tuning the number of partitions and executor memory can significantly improve performance and scalability.

---

**4. What is the difference between transformations and actions in Spark? Can you give an example?**

In Spark, transformations are lazy operations that define a logical plan (like `map`, `filter`, `groupBy`). They don't execute until an action is triggered. Actions, like `collect()` or `count()`, execute the transformations and return results.

**Example:**
```python
data = spark.range(1, 5)  # Creates a DataFrame
transformed = data.filter("id > 2")  # Transformation (lazy)
transformed.show()  # Action (triggers execution)
```

---

**5. How does Spark handle data shuffling, and what are some ways to reduce it?**

Data shuffling in Spark occurs when data needs to be redistributed across partitions, usually during operations like `groupBy`, `join`, or `distinct`. Shuffling can slow down jobs and increase memory pressure. To reduce it, I use techniques like broadcasting small datasets, using `reduceByKey` instead of `groupByKey`, and pre-partitioning data using `repartition()` or `partitionBy()`.

---

**6. What are the different persistence levels in Spark, and when would you use them?**

Spark provides several persistence levels:
- **MEMORY_ONLY**: Stores RDD in memory; recomputes if not enough space.
- **MEMORY_AND_DISK**: Caches in memory, spills to disk if full.
- **DISK_ONLY**: Stores RDD only on disk.
- **MEMORY_ONLY_SER / MEMORY_AND_DISK_SER**: Stores serialized objects to save space.

I use **MEMORY_AND_DISK** when the data doesn’t fit into memory and recomputing is expensive.

---

**8. How does HDFS store large datasets, and what are the advantages of using it?**

HDFS splits large files into blocks (default 128MB) and stores them across multiple nodes in a cluster. Each block is replicated (usually 3 times) for fault tolerance.

**Advantages:**
- High fault tolerance
- Scalability for petabyte-scale data
- Data locality optimizations
- Integration with Hadoop ecosystem tools

---

**9. What is the difference between internal and external tables in Hive?**

- **Internal Table**: Managed by Hive. Dropping the table deletes both metadata and data from HDFS.
- **External Table**: Hive manages only metadata. Dropping the table doesn’t delete data from HDFS.

Use **external tables** when you want Hive to reference data without taking control of its lifecycle.

---

**10. Can you explain partitioning and bucketing in Hive with an example?**

- **Partitioning** divides data based on column values (e.g., by `year`, `month`). It reduces the amount of data scanned.
- **Bucketing** hashes the data into fixed buckets using a column (e.g., `user_id`). It helps with efficient joins and sampling.

**Example:**
```sql
CREATE TABLE sales_partitioned (
  id INT, amount DOUBLE
)
PARTITIONED BY (year INT);

CREATE TABLE user_bucketed (
  id INT, name STRING
)
CLUSTERED BY (id) INTO 4 BUCKETS;
```

---

**11. Write an SQL query to find the second highest salary from an employee table.**
```sql
SELECT MAX(salary) AS SecondHighest
FROM employee
WHERE salary < (SELECT MAX(salary) FROM employee);
```

---

**13. How do you remove duplicate records from a PySpark DataFrame?**

Use the `dropDuplicates()` function, optionally with specific columns:
```python
df.dropDuplicates()  # Removes full row duplicates
df.dropDuplicates(["name", "salary"])  # Based on selected columns
```

---

**14. Explain how to use window functions in PySpark with an example.**

Window functions perform operations across rows that are related to the current row. They require a partition and ordering logic.

**Example:** Rank employees by salary within departments:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", rank().over(window_spec)).show()
```

---

**15. How do you handle skewed data in PySpark?**

To handle skewed data (where some keys have significantly more records), I use these strategies:
- **Salting the keys**: Add random prefixes to keys before joins to distribute the load.
- **Broadcast Join**: Broadcast the smaller dataset to avoid shuffle.
- **Skew Join Hints**: Use `.hint("skew")` in Spark 3+ to let Spark optimize skewed joins.
- **Increase parallelism**: Use `repartition()` to increase the number of tasks.

---



---

