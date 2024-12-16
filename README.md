# Import necessary libraries
from pyspark.sql.functions import current_date, col, lit, to_date, year, month, dayofmonth, when

# Read the CSV file
data_df = spark.read.csv.option("header", "true").option("inferSchema", True) \
    .load("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.csv")

# Display the original DataFrame
display(data_df)

# Add current date column to the DataFrame
data_df = data_df.withColumn("date", current_date())
print("Updated DataFrame with 'date' column:")
display(data_df)

# Calculate Urban Female and Male literacy percentages
data_df = data_df.withColumn("URBAN FEMALE %", (col("Literate and Educated persons - Urban - Females") / col("Literate and Educated persons - Urban - Total")) * 100) \
    .withColumn("URBAN MALE %", (col("Literate and Educated persons - Urban - Males") / col("Literate and Educated persons - Urban - Total")) * 100)

display(data_df)

# Calculate years, months, and days between the start and end date
start_date = lit('2011-01-01')
end_date = lit('2024-12-16')

data_df = data_df.withColumn("YEARS", year(to_date(end_date)) - year(to_date(start_date))) \
    .withColumn("MONTHS", month(to_date(end_date)) - month(to_date(start_date))) \
    .withColumn("DAYS", dayofmonth(to_date(end_date)) - dayofmonth(to_date(start_date))) \
    .withColumn("YEARS", col("YEARS") - (col("MONTHS") < 0).cast("int")) \
    .withColumn("MONTHS", (col("MONTHS") + 12) % 12) \
    .withColumn("DAYS", when(col("DAYS") < 0, col("DAYS") + 30).otherwise(col("DAYS"))) \
    .withColumn("MONTHS", when(col("DAYS") < 0, col("MONTHS") - 1).otherwise(col("MONTHS")))

display(data_df)

# Write the updated DataFrame to a Parquet file
data_df.write.mode("overwrite").parquet("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.parquet")

# Read the Parquet file and display its contents
df = spark.read.parquet("dbfs:/FileStore/sample data/Literacy_in_Haryana_by_Districts_2011.parquet")
df.show()
