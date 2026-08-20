# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,PySpark implementation
from pyspark.sql import functions as F
from pyspark.sql.window import Window

page_elements_df = (spark.read.table("serverless_stable_14ey07_catalog.hollyport.raw_parsed")
            .select(
                F.col("file_name"),
                F.col("elem.id").cast("int").alias("element_id"),
                F.col("elem.bbox")[0]["page_id"].cast("int").alias("page_id"),
                F.col("elem.content").alias("content"),
                F.col("elem.type").alias("type")
            )
)

# Step 3: Window spec - all preceding rows ordered by (Page_id, element id)
window_spec = (
    Window.orderBy(F.col("file_name"),F.col("Page_id"), F.col("element_id"))
    .rowsBetween(Window.unboundedPreceding, -1)
)

# Step 4: Enrich with nearest section_header, page_header, and title
enriched_df = page_elements_df.select(
    "file_name",
    "page_id",
    F.col("type"),
    F.col("element_id"),
    F.last(
        F.expr("CASE WHEN type = 'section_header' THEN named_struct('section_header', content, 'element_id', element_id,'page_id',page_id) END"),
        ignorenulls=True
    ).over(window_spec).alias("nearest_section_header"),
    F.last(
        F.expr("CASE WHEN type = 'page_header' THEN named_struct('page_header', content, 'element_id', element_id,'page_id',page_id) END"),
        ignorenulls=True
    ).over(window_spec).alias("nearest_page_header"),
    F.last(
        F.expr("CASE WHEN type = 'title' THEN named_struct('title', content, 'element_id', element_id,'page_id',page_id) END"),
        ignorenulls=True
    ).over(window_spec).alias("nearest_title")
)

# Step 5: Filter for table elements, package into VARIANT column
result_df = (
    enriched_df
    .filter(F.col("type") == "table")
    .select(
        "file_name",
        "type",
        "page_id",
        "element_id",
        F.expr("""
            parse_json(to_json(named_struct(
                'nearest_section_header', nearest_section_header,
                'nearest_page_header', nearest_page_header,
                'nearest_title', nearest_title
            )))
        """).alias("nearest_elements")
    )
    .orderBy("file_name","page_id", F.col("element_id").cast("int"))
)
result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("serverless_stable_14ey07_catalog.hollyport.table_metadata")
# display(result_df)

# COMMAND ----------

# DBTITLE 1,Group elements by nearest_elements
from pyspark.sql import functions as F

df = spark.read.table("serverless_stable_14ey07_catalog.hollyport.table_metadata")

# Regex to strip trailing "continued" variations:
# e.g. (continued), ( continued ), - continued, with any spacing
continued_pattern = r"\s*[-_#\.\{\[\(]?\.{0,}[\(\{\[]?\.{0,}\s*continued\.{0,}\s*[\)\]\}\]]?\.{0,}\s*$"

# Extract all fields from VARIANT and normalize page_header & section_header text
normalized_df = (
    df
    # page_header fields
    .withColumn("norm_page_header", F.trim(F.regexp_replace(
        F.expr("nearest_elements:nearest_page_header:page_header").cast("string"),
        continued_pattern, ""
    )))
    .withColumn("ph_element_id", F.expr("nearest_elements:nearest_page_header:element_id").cast("int"))
    .withColumn("ph_page_id", F.expr("nearest_elements:nearest_page_header:page_id").cast("int"))
    # section_header fields
    .withColumn("norm_section_header", F.trim(F.regexp_replace(
        F.expr("nearest_elements:nearest_section_header:section_header").cast("string"),
        continued_pattern, ""
    )))
    .withColumn("sh_element_id", F.expr("nearest_elements:nearest_section_header:element_id").cast("int"))
    .withColumn("sh_page_id", F.expr("nearest_elements:nearest_section_header:page_id").cast("int"))
    # title fields
    .withColumn("norm_title", F.expr("nearest_elements:nearest_title:title").cast("string"))
    .withColumn("t_element_id", F.expr("nearest_elements:nearest_title:element_id").cast("int"))
    .withColumn("t_page_id", F.expr("nearest_elements:nearest_title:page_id").cast("int"))
)

# Group by all nearest_elements fields (normalized text + element_id + page_id)
grouped_df = (
    normalized_df
    .groupBy(
        "file_name","norm_page_header", "norm_section_header", "norm_title"
    )
    .agg(
        F.count("*").alias("count_of_elements"),
        F.collect_list("page_id").alias("list_of_page_ids"),
        F.collect_list("element_id").alias("list_of_element_ids")
    )
    .select(
        "file_name",
        "count_of_elements",
        "list_of_page_ids",
        "list_of_element_ids",
        F.expr("""
            parse_json(to_json(named_struct(
                'file_name',file_name,
                'nearest_page_header', norm_page_header,
                'nearest_section_header', norm_section_header,
                'nearest_title', norm_title
            )))
        """).alias("nearest_elements")
    )

    .orderBy(F.col("count_of_elements").desc())
)
grouped_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("serverless_stable_14ey07_catalog.hollyport.grouped_table_metadata")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from serverless_stable_14ey07_catalog.hollyport.grouped_table_metadata where file_name="Permira V - FS - 2024-12-31_Redacted.pdf" 

# COMMAND ----------

