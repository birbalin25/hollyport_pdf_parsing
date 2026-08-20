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

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

def t(name):
    """Fully-qualified table name in the target catalog.schema."""
    return f"{catalog}.{schema}.{name}"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read tables
A = spark.read.table(t("grouped_table_metadata"))
B = spark.read.table(t("extracted_table_data_variant"))

# Step 1: Filter A where count_of_elements > 1, explode list_of_element_ids to join with B
A_exploded = (
    A.filter(F.col("count_of_elements") > 1)
    .withColumn("group_key", F.col("nearest_elements").cast("string"))
    .select("file_name", "group_key", F.explode("list_of_element_ids").alias("element_id"))
)

# Step 2: Left join B with A_exploded on element_id to preserve all rows in B
B_joined = B.join(A_exploded, on=["file_name", "element_id"], how="left")

# Step 3: Within each group, check if formatted_table_columns are the same
# and compute min(table_id)
group_window = Window.partitionBy("group_key")

B_with_group = B_joined.withColumn(
    "min_table_id",
    F.when(
        F.col("group_key").isNotNull(),
        F.min("table_id").over(group_window)
    )
).withColumn(
    "distinct_cols_count",
    F.when(
        F.col("group_key").isNotNull(),
        F.size(F.collect_set(F.col("formatted_table_columns").cast("string")).over(group_window))
    )
)

# display(B_with_group)

# COMMAND ----------

# Step 4: Replace table_id with min_table_id only when:
#   - the element is part of a matched group (group_key is not null)
#   - AND all formatted_table_columns in the group are identical (distinct count = 1)

final_df = B_with_group.withColumn(
    "table_id",
    F.when(
        (F.col("group_key").isNotNull()) & (F.col("distinct_cols_count") == 1),
        F.col("min_table_id")
    ).otherwise(F.col("table_id"))
).select("table_id", "page_id", "element_id", "file_name", "file_location", "formatted_table_columns", "data")

# Save to new table
final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    t("extracted_table_data_variant_merged")
)
# display(final_df)

# COMMAND ----------

from pyspark.sql import functions as F

file_name = "Permira V - FS - 2024-12-31_Redacted.pdf"
# file_name = "260331-FS-Bain X.pdf"
table_id = 19

df = spark.read.table(t("extracted_table_data_variant_merged"))

columns_row = (
    df.filter((F.col("file_name") == file_name) & (F.col("table_id") == table_id))
    .select("formatted_table_columns")
    .first()
)

columns_list = columns_row["formatted_table_columns"] if columns_row else []

# Guarded so it is a harmless no-op during job runs (the hardcoded row may be absent).
if columns_list:
    # Cast variant to JSON string, parse as MAP to handle keys with special characters (e.g. double quotes)
    data_map = F.from_json(F.col("data").cast("string"), "MAP<STRING,STRING>")
    selected_cols = [F.element_at(data_map, col).alias(col) for col in columns_list]

    result_df = (
        df.filter((F.col("file_name") == file_name) & (F.col("table_id") == table_id))
        .select(*selected_cols)
    )
    display(result_df)
else:
    print(f"No rows for file_name={file_name!r}, table_id={table_id}; skipping interactive preview.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Interactive check (disabled for job runs; table lives in the parameterized catalog/schema):
# MAGIC -- select * from serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant_merged