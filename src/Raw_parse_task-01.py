# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# DBTITLE 1,Parameters
# Config comes from the DAB (variables.yml -> job parameters). Defaults keep the
# notebook runnable standalone.
dbutils.widgets.text("catalog", "serverless_stable_14ey07_catalog")
dbutils.widgets.text("schema", "hollyport")
dbutils.widgets.text("volume", "vol1")
dbutils.widgets.text("source_subpath", "sources/hollyport_test/*/*.pdf")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
source_subpath = dbutils.widgets.get("source_subpath")

def t(name):
    """Fully-qualified table name in the target catalog.schema."""
    return f"{catalog}.{schema}.{name}"

# COMMAND ----------

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

# Source PDFs live under the UC volume; the glob is built from the parameters above.
source_path = f"/Volumes/{catalog}/{schema}/{volume}/{source_subpath}"

# Parse PDFs using ai_parse_document (runs distributed across executors)
parsed_df = spark.sql(f"""
SELECT
  ai_parse_document(content, MAP('version', '2.0')) AS parsed,
  path AS file_location,
  SUBSTRING_INDEX(path, '/', -1) AS file_name
FROM READ_FILES(
  '{source_path}',
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
  t("raw_parsed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Interactive check (disabled for job runs; table is in the parameterized catalog/schema):
# MAGIC -- select * from serverless_stable_14ey07_catalog.hollyport.raw_parsed

# COMMAND ----------

