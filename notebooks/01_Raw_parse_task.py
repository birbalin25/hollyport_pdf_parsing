# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

# COMMAND ----------

element_schema = ArrayType(StructType([
    StructField("id", IntegerType()),
    StructField("type", StringType()),
    StructField("content", StringType()),
    StructField("bbox", ArrayType(StructType([
        StructField("page_id", IntegerType())
    ])))
]))

# Parse PDFs using ai_parse_document (runs distributed across executors)
parsed_df = spark.sql("""
SELECT
  ai_parse_document(content, MAP('version', '2.0')) AS parsed,
  path AS file_location,
  SUBSTRING_INDEX(path, '/', -1) AS file_name
FROM READ_FILES(
--   '/Volumes/serverless_stable_14ey07_catalog/hollyport/vol1/unit_test/260331-FS-Bain X.pdf',
 '/Volumes/serverless_stable_14ey07_catalog/hollyport/vol1/sources/hollyport_test/*/*.pdf',
  format => 'binaryFile'
)
""")

# Explode elements array and keep only table elements
tables_df = (
    parsed_df
    .withColumn("elements", F.from_json(
        F.get_json_object(F.col("parsed").cast("string"), "$.document.elements"),
        element_schema
    ))
    .select("file_name", "file_location", F.explode("elements").alias("elem")))

tables_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
  "serverless_stable_14ey07_catalog.hollyport.raw_parsed")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from serverless_stable_14ey07_catalog.hollyport.raw_parsed --where file_name="Permira V - FS - 2024-12-31_Redacted.pdf"