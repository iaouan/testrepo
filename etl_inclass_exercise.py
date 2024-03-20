# Databricks notebook source
# MAGIC %md ###Workshop for ETL
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv',header=True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md ###Transform Data
# MAGIC

# COMMAND ----------

df_driver = df_driver.withColumn('age', datediff(current_date(),df_driver.dob)/365)

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_driver = df_driver.join(df_laptimes, on=['driverID'])

# COMMAND ----------

display(df_lap_driver)

# COMMAND ----------

df_lap_driver = df_driver.select('driverID','nationality','age','forename','surname','url').join(df_laptimes, on=['driverID'])

# COMMAND ----------

display(df_lap_driver)

# COMMAND ----------

# MAGIC %md ###Aggregate by Age
# MAGIC

# COMMAND ----------

df_lap_driver = df_lap_driver.groupBy('nationality', 'driverID', 'age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_driver)

# COMMAND ----------

# MAGIC %md ###Storing Data in S3
# MAGIC

# COMMAND ----------

df_lap_driver.write.csv('s3://xy25980-gr5069/processed/in_class_workshop/laptimes_by_drive.csv')
