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
**1. What is the difference between groupByKey and reduceByKey in PySpark?**
- `groupByKey` groups all values with the same key into a single sequence, which can lead to large data shuffling and potential memory issues.
- `reduceByKey` performs a local aggregation before the shuffle, reducing the amount of data transferred across the cluster. It is more efficient and preferred for large datasets.

**2. How to register a User Defined Function (UDF) in PySpark?**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def square(x):
    return x * x

square_udf = udf(square, IntegerType())
df.withColumn("squared", square_udf(df["number"]))
```

**3. What are Delta logs, and how to track data versioning in Delta tables?**
- Delta Lake maintains `_delta_log` directory to store transaction logs in JSON format. These logs track changes and allow versioning.
- You can use `DESCRIBE HISTORY delta_table_path` to view the version history.

**4. How do you monitor and troubleshoot ADF pipeline failures?**
- Use Azure Monitor or ADF Monitor tab to check pipeline run status, errors, and activity details.
- Enable diagnostic logs to send data to Log Analytics.
- Review error messages and failed activity inputs/outputs to pinpoint issues.

**5. What is the use of Delta Lake, and how does it support ACID transactions?**
- Delta Lake enables ACID transactions using the transaction log (_delta_log) that serializes operations like inserts, updates, and deletes.
- It ensures atomicity, consistency, isolation, and durability, making data lakes more reliable.

**6. Explain the concept of Managed Identity in Azure and its use in data engineering.**
- Managed Identity allows Azure services (like ADF, Databricks) to authenticate to other Azure resources (like Key Vault, Storage) without credentials.
- Helps implement secure, password-less authentication.

**7. Write a SQL query to find employees earning more than their manager.**
```sql
SELECT e.name
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

**8. Describe the process of migrating on-premises databases to Azure SQL Database.**
- Assess using Data Migration Assistant (DMA).
- Use Azure Database Migration Service (DMS) for schema and data transfer.
- Perform testing and validation before cutover.

**9. Write PySpark code to filter records based on a condition.**
```python
df.filter(df.age > 30).show()
```

**10. How do you implement error handling in ADF pipelines?**
- Use `If Condition`, `Until`, and `Switch` activities.
- Configure retry policies, and use `On Failure` paths in activities.
- Log failures using Web, Azure Function, or stored procedures.

**11. Explain the role of SparkSession in PySpark.**
- `SparkSession` is the entry point to work with DataFrames and SQL in PySpark.
- It encapsulates SparkContext and provides methods to create DataFrames, run SQL queries, and configure the Spark application.

**12. How do you optimize storage costs in ADLS?**
- Enable lifecycle management to move cold data to archive tiers.
- Compress data formats (Parquet, Avro).
- Partition data properly and avoid small files.

**13. Write a SQL query to find duplicate records in a table.**
```sql
SELECT name, COUNT(*)
FROM employees
GROUP BY name
HAVING COUNT(*) > 1;
```

**14. What is the purpose of caching in PySpark, and how is it implemented?**
- Caching stores intermediate results in memory to avoid recomputation.
- Use `df.cache()` or `df.persist()`.
- Helps improve performance when reusing DataFrames multiple times.

**15. Describe the process of integrating ADF with Databricks for ETL workflows.**
- Create a Linked Service for Databricks.
- Use "Notebook" activity in ADF pipeline to trigger Databricks notebooks.
- Pass parameters between ADF and notebook for dynamic execution.

**16. Write Python code to count the frequency of words in a string.**
```python
from collections import Counter
text = "hello world hello"
word_freq = Counter(text.split())
print(word_freq)
```

**17. How do you handle schema evolution in Delta Lake?**
- Use `mergeSchema` option when writing data:
```python
df.write.option("mergeSchema", "true").format("delta").mode("append").save(path)
```
- Delta Lake automatically merges schema changes.

**18. Explain the difference between streaming and batch processing in Spark.**
- **Batch:** Processes a finite volume of data at once. Eg: daily reports.
- **Streaming:** Processes data in real-time/micro-batches as it arrives. Eg: IoT data streams.

**19. How do you secure data pipelines in Azure?**
- Use Managed Identity and Key Vault for secrets.
- Enable RBAC and network-level restrictions (Private Endpoints).
- Encrypt data at rest and in transit.

**20. What are the best practices for managing large datasets in Databricks?**
- Use Delta Lake with Z-ordering and compaction.
- Avoid small files, repartition data.
- Leverage caching and broadcast joins.
- Monitor performance via Spark UI and Ganglia.

**Data Engineering and ADF - Q&A (Set 4)**

1. **What is deep clone and shallow clone?**
   - **Deep Clone:** Creates a full copy of the Delta table including the metadata and data at the time of clone. It is independent of the source.
   - **Shallow Clone:** Copies only metadata and references to data files. The actual data is not copied.

2. **What happens in shallow clone if table metadata is changed where you have cloned the table?**
   - Shallow clone maintains a snapshot of the table at the time of cloning. Any metadata changes in the source after cloning do not reflect in the cloned table.

3. **What is Vacuum command in Databricks?**
   - `VACUUM` removes files no longer referenced by Delta tables to free up storage. It is used for housekeeping.

4. **I have 100 rows, what happens if I run Vacuum?**
   - If no time threshold is crossed (default 7 days), `VACUUM` won’t delete any data. It removes stale data files that are no longer referenced.

5. **Various activities used in ADF:**
   - Copy activity, Lookup, Web, ForEach, If Condition, Execute Pipeline, Wait, Set Variable, Get Metadata, Stored Procedure, Script activity, Data Flow.

6. **Metadata driven architecture:**
   - Architecture that uses configuration metadata (e.g., tables, columns, transformations) stored in control tables to drive the ETL logic dynamically.

7. **What is Lookup activity?**
   - It fetches a single or multiple rows from a data source. Often used to retrieve config data.

8. **Types of linked services used in my project:**
   - Azure SQL Database, Azure Blob Storage, ADLS Gen2, Databricks, REST API.

9. **What are the triggers available?**
   - Schedule, Tumbling Window, Event-based, Manual.

10. **Use case for event-based trigger:**
   - When a file arrives in ADLS, trigger a pipeline to process it.

11. **What is tumbling trigger and difference between schedule and tumbling trigger?**
   - Tumbling Trigger ensures non-overlapping fixed-size intervals, with dependency tracking. Schedule trigger just runs based on time, without dependency or interval guarantees.

12. **SQL query to remove duplicates:**
   ```sql
   DELETE FROM employees 
   WHERE id NOT IN (
     SELECT MIN(id) 
     FROM employees 
     GROUP BY name, salary
   );
   ```

13. **How switch case works in ADF?**
   - Evaluates an expression and routes execution to the matching case path, similar to if-else ladder.

14. **What is Web activity?**
   - Makes HTTP calls to external APIs from ADF.

15. **What is the output of Web activity?**
   - JSON response. You can use `@activity('Web1').output` to access and parse it.

16. **How many activities are supported within a pipeline?**
   - Up to 40 activities per pipeline.

17. **Question on Execute Pipeline activity in ADF:**
   - Used to call/trigger another pipeline (child) from a parent pipeline.

18. **How to pass parameters between parent and child pipeline:**
   - Define parameters in child pipeline. Pass values from parent using the settings in Execute Pipeline activity.

19. **Transformations used in ADF and Databricks:**
   - ADF: Derived Column, Filter, Conditional Split, Aggregate, Join.
   - Databricks: withColumn, selectExpr, filter, groupBy, join.

20. **If SQL query is running for long, how do you identify the issue?**
   - Check execution plan, look for missing indexes, table scans, large joins, and analyze statistics.

21. **What is Index?**
   - A database object that improves query speed by allowing faster lookups on columns.

22. **Difference between partitioning and index:**
   - Partitioning splits data into segments to reduce scanned data.
   - Index allows faster lookups but doesn’t reduce data scan size.

23. **How index works?**
   - Uses B-tree or hash structures to enable quick data retrieval without scanning full table.

24. **What is Hive metastore and backend mechanism?**
   - It stores metadata of tables/databases. Metadata is stored in RDBMS (e.g., MySQL). Data remains in HDFS or cloud storage.

25. **Difference between managed and external table:**
   - Managed: Spark/Hive manages both data and metadata.
   - External: Only metadata is managed; data remains in user-provided path.

26. **Syntax to specify managed/external table in PySpark/SQL:**
   ```sql
   -- Managed
   CREATE TABLE table_name (id INT, name STRING);

   -- External
   CREATE TABLE table_name (id INT, name STRING)
   USING DELTA LOCATION '/mnt/path/to/data';
   ```

27. **What will be created along with Parquet in Delta tables?**
   - `_delta_log` folder containing transaction logs.

28. **Types of files in _delta_log:**
   - JSON (transaction actions), CRC (checksums).

29. **Question on repartition and coalesce:**
   - `repartition(n)`: Increases/reshuffles partitions (full shuffle).
   - `coalesce(n)`: Reduces partitions (less shuffle).

30. **Update null values in SQL:**
   ```sql
   SELECT COALESCE(column_name, 'default_value') FROM table;
   ```

31. **What is constraint and types:**
   - Rules to restrict data in a table.
   - Types: Primary Key, Foreign Key, Unique, Check, Not Null, Default.

32. **What is unique constraint and check constraint:**
   - **Unique:** Ensures column values are distinct.
   - **Check:** Validates column values based on a condition.

33. **Have I used constraints in project?**
   - Yes, used **Primary Key** and **Check Constraint** in Azure SQL to ensure data integrity.

34. **Explain Medallion Architecture:**
   - Bronze (raw data), Silver (cleaned data), Gold (aggregated data) layers for organizing Lakehouse data.

35. **Question on Node and Partitions:**
   - Node: A physical/virtual machine in the cluster.
   - Partition: Logical division of data across nodes for parallelism.

36. **SCD Type 1 and Type 2:**
   - **SCD Type 1:** Overwrites old data.
   - **SCD Type 2:** Maintains history using new rows with start and end date columns.

37. **Ways to implement SCD Type 2:**
   - Merge in SQL or Delta Lake using surrogate keys and date fields.
   - Data Flows in ADF using Alter Row and Conditional Split.
   - PySpark Merge with `whenMatchedUpdate`, `whenNotMatchedInsert`.

Behavoural questions :
Here are answers to all the technical, scenario-based, and behavioral interview questions based on your two Azure Data Engineering projects:

---

## ✅ Technical Questions & Detailed Answers

### 1. **Walk me through the architecture of your pipeline.**

**Answer:**
In the scalable log processing project, I built a hybrid architecture combining real-time and batch processing.

* **Real-time**: Kafka ingested JSON logs from website events. Spark Structured Streaming parsed and validated logs with a strict schema, then enriched data (adding geolocation, user agent info). Output was stored in Azure Data Lake Storage (ADLS) as partitioned Parquet files (e.g., partitioned by date, region).
* **Batch**: Azure Data Factory picked up the same files, re-applied transformations for consistency, then performed aggregations like average TTI and TTAR per page URL. Output was stored in Azure Synapse Analytics.
* **Orchestration**: Azure Functions responded to ADLS events, determined dynamic paths, retrieved last checkpoints from Cosmos DB, and triggered the ADF pipeline.

Cluster: Spark autoscaling with 8-16 nodes (8 vCPU, 64 GB RAM each).
Data Size: \~3 TB per day, \~90 TB/month.

---

### 2. **Why did you use both real-time and batch processing?**

**Answer:**
Real-time streaming (Kafka + Spark Structured Streaming) provided near-instant insights like click behavior and UI responsiveness. But for heavy joins and aggregations, we used batch processing (ADF) to optimize cost and accuracy. This also helped reconcile with historical data and regenerate if needed.

---

### 3. **How did you handle schema drift in JSON logs?**

**Answer:**
We defined a strict schema using Spark `StructType` and rejected records that didn't match. Bad records were sent to a quarantine path in ADLS. This allowed uninterrupted processing and easier debugging. We monitored drift patterns to update schema definitions quarterly.

---

### 4. **Why use Parquet format in ADLS?**

**Answer:**
Parquet is columnar and supports compression, making it ideal for analytical queries. For instance, when calculating TTAR, we read only specific columns, speeding up processing. We also leveraged `snappy` compression and partitioned by `event_date` and `region` for better query pushdown.

---

### 5. **How did Cosmos DB help with checkpointing?**

**Answer:**
Cosmos DB stored metadata like the last file processed or timestamp for each input path. Azure Functions used this to determine incremental files. This helped us resume gracefully during failures and avoided duplicate processing.

---

## ✅ Scenario-Based Questions & Answers

### 6. **Your Spark job is slow. How do you debug?**

**Answer:**
I check Spark UI for skewed stages or long tasks. In one case, page URL caused skew because 90% logs were from one popular page. I applied salting to the key during aggregation and later de-salted. I also tuned `spark.sql.shuffle.partitions` and used broadcast joins when feasible.

---

### 7. **Kafka sends duplicate logs. How do you avoid duplicates?**

**Answer:**
We added a deduplication step in Spark using `dropDuplicates()` based on composite key (`event_id`, `timestamp`, `user_id`). In batch, we used Delta Lake's `MERGE` to avoid insert-only logic and ensured idempotency.

---

### 8. **Azure Function fails. What’s your recovery plan?**

**Answer:**
We implemented a backup timer-based Azure Function to scan ADLS for missed files. It cross-referenced Cosmos DB's checkpoint and triggered ADF for any gaps. Logs were pushed to Log Analytics, and alerts sent to DevOps via Teams.

---

## ✅ Behavioral Questions & STAR-Format Answers

### 9. **Describe a time you worked with other teams.**

**Answer:**
**Situation:** At Sony, product managers wanted TTI and TTAR metrics per campaign.
**Task:** I had to ensure the data pipeline provided accurate, timely metrics.
**Action:** Collaborated with web developers for event design, and analysts for KPI definitions. Incorporated logic into both streaming and batch flows.
**Result:** Improved dashboard accuracy by 90% and helped reduce bounce rate by 12% through actionable insights.

---

### 10. **Biggest challenge or bug you solved?**

**Answer:**
**Situation:** Spark job failed randomly during nightly runs.
**Task:** Identify root cause of instability.
**Action:** Analyzed logs and noticed large JSON payloads caused memory issues. Refactored schema parsing to be lazy-loaded and added adaptive query execution.
**Result:** Pipeline stability improved, and job runtime reduced by 30%.

---

### 11. **What was the business impact of your solution?**

**Answer:**
Our log analytics pipeline helped identify that pages with high TTAR had lower conversions. The website team optimized loading, which improved page load times by 35% and raised sales conversion by 8% in key regions.

---

Let me know if you'd like this in a formatted PDF or presentation slide deck for interviews.



---

