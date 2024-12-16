-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC data_df=spark.read.csv.option("header","true").option("inferSchema",True)\
-- MAGIC .load("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.csv")
-- MAGIC display(data_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_date
-- MAGIC data_df = data_df.withColumn("date", current_date())
-- MAGIC print("Updated DataFrame with 'date' column:")
-- MAGIC display(data_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC data_df = data_df. withColumn("URBAN FEMALE %", (col("Literate and Educated persons - Urban - Females") / col("Literate and Educated persons - Urban - Total")) * 100) \
-- MAGIC         .withColumn("URBAN MALE %", (col("Literate and Educated persons - Urban - Males") / col("Literate and Educated persons - Urban - Total")) * 100)
-- MAGIC
-- MAGIC display(data_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit, to_date, year, month, dayofmonth, col, when
-- MAGIC
-- MAGIC start_date = lit('2011-01-01')
-- MAGIC end_date = lit('2024-12-16')
-- MAGIC
-- MAGIC data_df = data_df.withColumn("YEARS", year(to_date(end_date)) - year(to_date(start_date))) \
-- MAGIC                  .withColumn("MONTHS", month(to_date(end_date)) - month(to_date(start_date))) \
-- MAGIC                  .withColumn("DAYS", dayofmonth(to_date(end_date)) - dayofmonth(to_date(start_date))) \
-- MAGIC                  .withColumn("YEARS", col("YEARS") - (col("MONTHS") < 0).cast("int")) \
-- MAGIC                  .withColumn("MONTHS", (col("MONTHS") + 12) % 12) \
-- MAGIC                  .withColumn("DAYS", when(col("DAYS") < 0, col("DAYS") + 30).otherwise(col("DAYS"))) \
-- MAGIC                  .withColumn("MONTHS", when(col("DAYS") < 0, col("MONTHS") - 1).otherwise(col("MONTHS")))
-- MAGIC
-- MAGIC display(data_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_df.write.mode("overwrite").parquet("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.parquet")
-- MAGIC df = spark.read.parquet("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.parquet")
-- MAGIC df.show()
