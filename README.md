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

---

