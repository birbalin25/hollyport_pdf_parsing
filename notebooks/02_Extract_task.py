# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Parse PDF and extract table elements
import json
import re
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType, IntegerType, MapType, StringType, StructField, StructType
)
from pdf_table_utils import fix_misaligned_headers, merge_partial_rows

for_test = True

# ---------------------------------------------------------------------------
# Prompt template (same as get_extraction_prompt, built as SQL concat column)
# ---------------------------------------------------------------------------
PROMPT_TEMPLATE = r"""You are a Financial data extraction expert. Parse the following HTML table into a JSON array of records.

            IMPORTANT RULES:

            1. When one column contains descriptive values across multiple consecutive rows while all other columns in those rows are empty/null, this indicates a multi-line description split across rows. Merge those rows (may be two or more rows as required) by concatenating the descriptive values (joined by "\n") into the corresponding column of the next row that has values in multiple columns, and remove the partial rows. Example: ["A","",""], ["B","",""], ["C","$100","$200"] -> ["A\nB\nC","$100","$200"].
            2. If the HTML has colspan="2" on any header - treat it as a SINGLE column.
            3. Keep numeric values as strings (preserve commas and $ signs).
            4. Return ONLY a valid JSON array, no markdown, no explanation.
            5. Keep the JSON columns in the same order as they appear in the html table.
            6. The currency symbol like $, etc sometimes is far apart from the number. As such, they may appear as separate columns in html data. In such case, merge them into a single column.
            7. If a table row contains multiple values separated by <br> tags, split it into separate JSON records, with each <br>-separated value becoming its own record. Do not duplicate columns that contain only a single-line value; leave those fields empty in the additional records unless the column itself contains value in multiple lines.
            8. If any header column (<th>) in the HTML is empty or blank, assign it a name like column1_llm, column2_llm, column3_llm and so on. Never use an empty string as a JSON key.
            9. HEADER NAME CLEANUP (MANDATORY for EVERY header — no exceptions):
               a) Replace any <br>, <br/>, or <br /> tags in header text with a single space. Never put "\n" in a JSON key.
               b) Then strip any trailing currency-unit suffix from the key: " €'000", " €m", " €bn", or " €" (and any <sup>…</sup> after it). If the entire header was just a suffix (e.g., "€'000"), it becomes empty — apply Rule 8.
               c) VERIFY: If any JSON key still contains "€'000", "€m", "€bn", trailing "€", or "\n", fix it before outputting.
               Examples:
               - "Cost €'000" → "Cost" | "LP1 €m" → "LP1" | "Level 1 €'000" → "Level 1"
               - "LP1<br>€'000" → "LP1" | "December 2024<br>Total<br>€'000" → "December 2024 Total"
               - "Net Asset Value" → no change | "€'000" alone → apply Rule 8 (column1_llm etc.)
            10. COLUMN SPLITTING FOR MERGED NUMERIC+TEXT CELLS: After applying Rule 1 (multi-row merging), or if a single HTML cell already contains both a numeric quantity and unrelated descriptive text, split them into two JSON columns:
               - The ORIGINAL column (whatever its header name is) must contain ONLY the numeric quantity — nothing else.
               - A NEW column called "Description" must contain ALL remaining text from that cell: any names, labels, or descriptions that appeared BEFORE the number, AND any descriptive text that appeared AFTER the number. Preserve their original order, joined by "\n".
               
               HOW TO DECIDE WHETHER TO SPLIT: Look at the ENTIRE table, not just one cell. If MOST data rows in the same column consistently show the same pattern — either "numeric quantity followed by text" or "text followed by numeric quantity" — then this column has a merged-column problem and ALL such rows should be split. A consistent pattern across multiple rows is strong evidence that two visual columns were merged during PDF parsing. Do NOT split based on a single isolated cell that happens to contain a number.
               
               A numeric quantity is a standalone number that represents a count, amount, or measurement (e.g., number of shares, par value, units held). It is NOT part of a name or description. Use your judgment to distinguish a standalone quantity from numbers that are part of descriptive text (e.g., "7.00%" in "7.00% Preferred Units" is part of the description, not a standalone quantity).
               
               Examples:
               - Cell: "Company ABC:\nSubsidiary XYZ\n$ 23,796,803 5.00% Senior Notes due 2031"
                 -> Original column: "$ 23,796,803"
                 -> Description: "Company ABC:\nSubsidiary XYZ\n5.00% Senior Notes due 2031"
               - Cell: "Entity Name, LLC\n692,353 Class A Units"
                 -> Original column: "692,353"
                 -> Description: "Entity Name, LLC\nClass A Units"
               - Cell with ONLY a number (e.g., "$ 601,258,723") -> do NOT split. No Description needed.
               - Cell with no standalone quantity (e.g., "Total" or a summary label) -> do NOT split, keep as-is in the original column with Description as empty string.
               
               This rule applies to ANY column regardless of its header name. The key signal is: a CONSISTENT pattern across rows where cells mix a standalone numeric quantity alongside unrelated text such as entity names or descriptions.


            HTML Table:
            """

# ---------------------------------------------------------------------------
# Post-processing UDF (runs distributed on executors)
# Applies the same logic: JSON parsing with fallback, fix_misaligned_headers,
# and merge_partial_rows.
# ---------------------------------------------------------------------------
@F.udf(returnType=StructType([
    StructField("raw_table_columns", ArrayType(StringType())),
    StructField("formatted_table_columns", ArrayType(StringType())),
    StructField("formatted_json_table_data", StringType()),  # JSON string of array
]))
def post_process_table(raw_html, llm_output):
    """Parse LLM JSON output and apply fix_misaligned_headers + merge_partial_rows."""
    if not llm_output or not raw_html:
        return None

    # Extract table column headers from raw HTML
    table_columns = re.findall(r'<th[^>]*>(.*?)</th>', raw_html)

    # Parse LLM JSON output (with fallback for malformed/multiple JSON arrays)
    try:
        records = json.loads(llm_output)
    except (json.JSONDecodeError, TypeError):
        records = None
        decoder = json.JSONDecoder()
        for match in re.finditer(r'\[', llm_output):
            try:
                candidate, _ = decoder.raw_decode(llm_output, match.start())
                if isinstance(candidate, list):
                    records = candidate
            except (json.JSONDecodeError, ValueError):
                continue
        if records is None:
            return None

    if not isinstance(records, list) or not records:
        return None

    # Apply post-processing (identical to original logic)
    records = fix_misaligned_headers(records, table_columns)
    records = merge_partial_rows(records)

    if not records:
        return None

    formatted_table_columns = list(records[0].keys())
    return (table_columns, formatted_table_columns, json.dumps(records))

print("Setup complete: prompt template and post-processing UDF defined.")

# COMMAND ----------

# DBTITLE 1,Step 1: Parse PDFs and extract table elements (distributed)
tables_df = (spark.read.table("serverless_stable_14ey07_catalog.hollyport.raw_parsed")
            .filter(F.col("elem.type") == "table")
            .select(
                F.col("file_name"),
                F.col("file_location"),
                F.col("elem.id").cast("int").alias("element_id"),
                F.col("elem.bbox")[0]["page_id"].cast("int").alias("page_id"),
                F.col("elem.content").alias("raw_html")
            )
)

# Generate table_id: rank element_id within each (file_name, file_location) group
# so the lowest element_id gets 1, next gets 2, etc.
window_spec = Window.partitionBy("file_name", "file_location").orderBy("element_id")
tables_df = tables_df.withColumn("table_id", F.dense_rank().over(window_spec))

# Write to staging table (checkpoint: avoids re-running ai_parse_document)


tables_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("serverless_stable_14ey07_catalog.hollyport.parsed_table_elements_staging")
staging_count = spark.table("serverless_stable_14ey07_catalog.hollyport.parsed_table_elements_staging").count()
print(f"Step 1 complete: {staging_count} table elements extracted and staged.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from serverless_stable_14ey07_catalog.hollyport.parsed_table_elements_staging --where file_name="Permira V - FS - 2024-12-31_Redacted.pdf"

# COMMAND ----------

# DBTITLE 1,Step 2: Distributed LLM extraction with ai_query + post-processing
# ---------------------------------------------------------------------------
# Step 2: Read staged table elements, call ai_query() distributed across
# all executors, apply post-processing UDF, and write final results.
# ai_query() handles retries, rate-limiting, and endpoint scaling automatically.
# ---------------------------------------------------------------------------

# Read from staging (no re-computation of ai_parse_document)
tables_df = spark.table("serverless_stable_14ey07_catalog.hollyport.parsed_table_elements_staging")

# Repartition for optimal parallelism during ai_query calls
# More partitions = more concurrent LLM requests across executors
num_elements = tables_df.count()
num_partitions = max(50, min(200, num_elements // 10))
print(f"Processing {num_elements} table elements with {num_partitions} partitions...")

tables_df = tables_df.repartition(num_partitions)

# Escape template for SQL string literal (no single quotes in template, but safe practice)
sql_template = PROMPT_TEMPLATE.replace("'", "''")

# Call ai_query() distributed - concat prompt + raw_html inline, no intermediate column needed
# ai_query handles retries, rate-limiting, and endpoint scaling automatically
# failOnError => false returns struct(response, errorStatus) for graceful error handling
tables_with_llm = tables_df.withColumn(
    "llm_result",
    # F.expr(f"""
    #     ai_query(
    #         'databricks-claude-sonnet-4-6',
    #         concat('{sql_template}', raw_html),
    #         modelParameters => named_struct('max_tokens', 4000),
    #         failOnError => false
    #     )
    # """)

    F.expr(f"""
        ai_query(
            'databricks-gpt-oss-120b',
            concat('{sql_template}', raw_html),
            modelParameters => named_struct('max_tokens', 4000),
            failOnError => false
        )
    """)
    # databricks-gpt-oss-20b   ##  databricks-qwen3-next-80b-a3b-instruct


)

# Extract response and apply post-processing UDF (distributed on executors)
tables_processed = (
    tables_with_llm
    .withColumn("llm_output", F.col("llm_result.result"))
    .withColumn("llm_error", F.col("llm_result.errorMessage"))
    .withColumn("processed", post_process_table(F.col("raw_html"), F.col("llm_output")))
)

# display(tables_processed)
# Build final result DataFrame (include all records - successes and failures)
result_df = (
    tables_processed
    .select(
        F.col("page_id"),
        F.col("element_id"),
        F.lit("table").alias("element_type"),
        F.col("processed.raw_table_columns"),
        F.col("processed.formatted_table_columns"),
        F.col("raw_html").alias("raw_html_table_data"),
        F.from_json(
            F.col("processed.formatted_json_table_data"),
            ArrayType(MapType(StringType(), StringType()))
        ).alias("formatted_json_table_data"),
        F.col("file_name"),
        F.col("file_location"),
        F.col("table_id"),
        F.col("llm_output"),
        F.col("llm_error")        
        # F.col("processed") ##need to remove
    )
)

# Write final results to Delta table
result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("serverless_stable_14ey07_catalog.hollyport.all_table_data_batch")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from serverless_stable_14ey07_catalog.hollyport.all_table_data_batch-- where file_name="Permira V - FS - 2024-12-31_Redacted.pdf"

# COMMAND ----------

# DBTITLE 1,Step 3: Extract table data as VARIANT and save to Delta
from pyspark.sql import functions as F

# Read source table (table_id already computed upstream in Cell 2)
source_df = spark.table("serverless_stable_14ey07_catalog.hollyport.all_table_data_batch")    

# Explode the array so each JSON record becomes its own row, filter nulls
exploded_df = source_df.filter(
    F.col("formatted_json_table_data").isNotNull()
).select(
    F.col("table_id"),
    F.col("page_id"),
    F.col("element_id"),
    F.col("file_name"),
    F.col("file_location"),
    F.col("formatted_table_columns"),    
    F.explode("formatted_json_table_data").alias("row_data")
)

# Convert each map to VARIANT using parse_json(to_json(...))
result_df = exploded_df.select(
    F.col("table_id"),
    F.col("page_id"),
    F.col("element_id"),
    F.col("file_name"),
    F.col("file_location"),
    F.col("formatted_table_columns"),  
    F.expr("parse_json(to_json(row_data))").alias("data")
)

# Save to Delta table

result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
"serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant"
)
print(f"Saved {spark.table('serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant').count()} rows")
# display(spark.table("serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant"))

# COMMAND ----------

display(spark.table("serverless_stable_14ey07_catalog.hollyport.extracted_table_data_variant"))

# COMMAND ----------

