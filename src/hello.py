# Databricks notebook source

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

display(spark.range(5))
# COMMAND ----------
